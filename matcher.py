"""
matcher.py — CanadianBids.ai 3-stage matching pipeline.

Stage 1: Keyword/rule-based pre-filter (fast, free) → top 50 candidates
Stage 2: AI semantic scoring via Claude Haiku → relevance + explanation
Stage 3: Historical win data boost → patterns from similar companies

Final score (0-100):
  - Keyword/rule score:  0-40  (pre-filter, uses all profile fields)
  - AI semantic score:   0-40  (Claude Haiku evaluation)
  - History boost:       0-20  (vendor_history patterns)
"""

import re
import anthropic
from datetime import datetime, timezone
from ai_scorer import score_batch, score_single


# ═══════════════════════════════════════════════════════════════════
# STAGE 1: Keyword / Rule-Based Pre-Filter
# ═══════════════════════════════════════════════════════════════════

def normalize(text):
    """Lowercase and strip punctuation for keyword matching."""
    if not text:
        return ""
    return re.sub(r'[^\w\s]', ' ', text.lower())


def keyword_score(tender, profile):
    """
    Rule-based scoring using all available profile fields.
    Returns (score: 0-40, matched_keywords: list)
    """
    score = 0
    matched_keywords = []

    # ── Collect all matchable terms from profile ──
    raw_keywords = [k.lower().strip() for k in (profile.get("keywords") or []) if k]
    
    # Add service_types, goods_types, construction_types as extra keywords
    for field in ["service_types", "goods_types", "construction_types"]:
        extras = profile.get(field) or []
        raw_keywords.extend([k.lower().strip() for k in extras if k])
    
    # Add licensed_trades
    for trade in (profile.get("licensed_trades") or []):
        if trade:
            raw_keywords.append(trade.lower().strip())
    
    # Deduplicate
    keywords = list(dict.fromkeys(raw_keywords))
    
    if not keywords:
        return 0, []

    title = normalize(tender.get("title") or "")
    desc = normalize(tender.get("description") or "")
    dept = normalize(tender.get("department") or "")

    # ── Keyword matching ──
    for kw in keywords:
        kw_norm = normalize(kw)
        if not kw_norm or len(kw_norm) < 2:
            continue

        in_title = kw_norm in title
        in_desc = kw_norm in desc
        in_dept = kw_norm in dept

        if in_title:
            score += 8
            if kw not in matched_keywords:
                matched_keywords.append(kw)
        elif in_desc:
            score += 3
            if kw not in matched_keywords:
                matched_keywords.append(kw)
        elif in_dept:
            score += 2
            if kw not in matched_keywords:
                matched_keywords.append(kw)

    # Cap keyword portion at 24
    score = min(score, 24)

    # ── Category match ──
    tender_cats = [c.strip().upper() for c in (tender.get("category") or "").split(",") if c.strip()]
    profile_cats = [c.strip().upper() for c in (profile.get("categories") or [])]
    # Also check for codes embedded in concatenated strings
    tender_cat_str = (tender.get("category") or "").upper()
    if any(c in tender_cat_str for c in profile_cats):
        score += 4

    # ── Region match ──
    tender_region = normalize(tender.get("region") or "")
    profile_province = normalize(profile.get("province") or "")
    profile_provs = [normalize(p) for p in (profile.get("provinces_operating") or [])]
    delivers_nationally = profile.get("delivers_nationally", False)

    all_profile_regions = ([profile_province] if profile_province else []) + profile_provs
    if any(r and r in tender_region for r in all_profile_regions):
        score += 4
    elif delivers_nationally or "national" in tender_region or "canada" in tender_region:
        score += 3

    # ── Certification / clearance match ──
    tender_text = f"{title} {desc}".lower()
    profile_certs = [c.lower() for c in (profile.get("certifications") or [])]
    profile_clearance = (profile.get("clearance_level") or "").lower()
    
    # Check if tender mentions security clearance
    if any(term in tender_text for term in ["security clearance", "secret", "reliability", "protected"]):
        if profile_clearance and profile_clearance != "none":
            score += 4  # Has clearance and tender needs it
    
    # Check certifications match
    for cert in profile_certs:
        if cert and normalize(cert) in tender_text:
            score += 2
            break  # Only count once

    # ── Supply arrangement match ──
    arrangements = profile.get("supply_arrangements") or []
    if arrangements:
        for arr in arrangements:
            if arr and normalize(arr) in tender_text:
                score += 2
                break

    # Cap at 40
    score = min(score, 40)
    return score, matched_keywords


# ═══════════════════════════════════════════════════════════════════
# STAGE 3: Historical Win Data Boost
# ═══════════════════════════════════════════════════════════════════

def load_history_patterns(db, profile):
    """
    Load vendor_history for the company and extract patterns.
    Returns dict with department/category/keyword patterns.
    """
    company_name = profile.get("company_name", "")
    if not company_name:
        return None

    try:
        resp = db.table("vendor_history") \
            .select("gsin_description_en, tender_description_en, procurement_category") \
            .ilike("supplier_legal_name", f"%{company_name}%") \
            .limit(100) \
            .execute()
        history = resp.data or []
    except Exception as e:
        print(f"  History load error: {e}")
        return None

    if not history:
        return None

    # Extract patterns from past wins
    patterns = {
        "categories": set(),
        "description_terms": set(),
        "gsin_terms": set(),
    }

    for row in history:
        # Categories they've won in
        cat = (row.get("procurement_category") or "").strip().upper()
        if cat:
            patterns["categories"].add(cat)

        # Terms from past tender descriptions
        desc = normalize(row.get("tender_description_en") or "")
        for word in desc.split():
            if len(word) > 4:  # Skip short words
                patterns["description_terms"].add(word)

        # GSIN terms
        gsin = normalize(row.get("gsin_description_en") or "")
        for word in gsin.split():
            if len(word) > 4:
                patterns["gsin_terms"].add(word)

    # Keep only most common terms (top 50) to avoid noise
    patterns["description_terms"] = set(list(patterns["description_terms"])[:50])
    patterns["gsin_terms"] = set(list(patterns["gsin_terms"])[:30])

    return patterns


def history_boost(tender, patterns):
    """
    Score a tender based on historical win patterns.
    Returns 0-20 boost score.
    """
    if not patterns:
        return 0

    boost = 0
    tender_text = normalize(
        f"{tender.get('title', '')} {tender.get('description', '')} "
        f"{tender.get('department', '')}"
    )
    tender_cat = (tender.get("category") or "").upper()

    # Category match with past wins
    if any(cat in tender_cat for cat in patterns["categories"]):
        boost += 6

    # Description term overlap with past wins
    desc_overlap = sum(1 for term in patterns["description_terms"] if term in tender_text)
    if desc_overlap >= 5:
        boost += 8
    elif desc_overlap >= 3:
        boost += 5
    elif desc_overlap >= 1:
        boost += 2

    # GSIN term overlap
    gsin_overlap = sum(1 for term in patterns["gsin_terms"] if term in tender_text)
    if gsin_overlap >= 3:
        boost += 6
    elif gsin_overlap >= 1:
        boost += 3

    return min(boost, 20)


# ═══════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════

def run_matching(db, anthropic_key=None, min_score=15, max_matches=25, prefilter_top=50):
    """
    Full 3-stage matching pipeline.
    
    Stage 1: Keyword pre-filter all live tenders → top N candidates
    Stage 2: AI semantic scoring on candidates (if API key available)
    Stage 3: Historical win data boost
    Final:   Blend scores, rank, write to matches table
    """
    print(f"\n{'='*60}")
    print(f"Starting 3-stage matching pipeline")
    print(f"  Time: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}")

    # ── Load tenders ──
    print(f"\n[LOAD] Fetching tenders...")
    try:
        tenders_resp = db.table("tenders").select(
            "id, title, description, department, category, region, "
            "procurement_method, selection_criteria, closing_date, "
            "notice_type, solicitation_number, ai_summary"
        ).execute()
        tenders = tenders_resp.data or []
    except Exception as e:
        print(f"  ERROR loading tenders: {e}")
        return {"users_matched": 0, "total_matches": 0, "live_tenders": 0, "errors": 1}

    # Filter to live tenders
    now = datetime.now(timezone.utc).isoformat()
    live_tenders = [t for t in tenders if t.get("closing_date") and t["closing_date"] > now]
    print(f"  Total: {len(tenders)} | Live: {len(live_tenders)}")

    # ── Load profiles ──
    print(f"\n[LOAD] Fetching profiles...")
    profiles_resp = db.table("profiles").select("*").execute()
    profiles = [p for p in (profiles_resp.data or []) if p.get("onboarding_complete")]
    print(f"  Profiles to match: {len(profiles)}")

    # ── Load subscriptions ──
    subs_resp = db.table("subscriptions").select("user_id, plan, status").execute()
    subs_by_user = {s["user_id"]: s for s in (subs_resp.data or [])}

    # ── Init AI client ──
    ai_client = None
    if anthropic_key:
        try:
            ai_client = anthropic.Anthropic(api_key=anthropic_key)
            print(f"\n[AI] Claude Haiku enabled for semantic scoring")
        except Exception as e:
            print(f"\n[AI] Could not init client: {e}")

    total_matches = 0
    total_users_matched = 0
    errors = 0

    for profile in profiles:
        user_id = profile.get("id")
        if not user_id:
            continue

        company = profile.get("company_name", "Unknown")[:30]
        sub = subs_by_user.get(user_id, {})
        is_pro = sub.get("plan") == "pro" and sub.get("status") == "active"

        try:
            print(f"\n── Matching: {company} ({user_id[:8]}...) ──")

            # ════════════════════════════════
            # STAGE 1: Keyword Pre-Filter
            # ════════════════════════════════
            print(f"  [Stage 1] Keyword pre-filter...")
            prefiltered = []
            for tender in live_tenders:
                kw_score, kw_matches = keyword_score(tender, profile)
                if kw_score > 0:
                    prefiltered.append({
                        "tender": tender,
                        "kw_score": kw_score,
                        "kw_matches": kw_matches,
                    })

            # Sort by keyword score, take top N
            prefiltered.sort(key=lambda x: x["kw_score"], reverse=True)
            candidates = prefiltered[:prefilter_top]
            print(f"    {len(prefiltered)} had keyword hits → top {len(candidates)} candidates")

            if not candidates:
                print(f"    No keyword matches found")
                continue

            # ════════════════════════════════
            # STAGE 2: AI Semantic Scoring
            # ════════════════════════════════
            ai_scores = {}
            if ai_client and candidates:
                print(f"  [Stage 2] AI semantic scoring ({len(candidates)} tenders)...")
                try:
                    ai_scores = score_batch(
                        ai_client,
                        profile,
                        [c["tender"] for c in candidates],
                        batch_size=10
                    )
                    scored_count = sum(1 for v in ai_scores.values() if v["score"] > 0)
                    print(f"    AI scored {scored_count}/{len(candidates)} with >0 relevance")
                except Exception as e:
                    print(f"    AI scoring failed: {e}")

            # ════════════════════════════════
            # STAGE 3: Historical Win Boost
            # ════════════════════════════════
            print(f"  [Stage 3] Historical win data boost...")
            patterns = load_history_patterns(db, profile)
            if patterns:
                cat_count = len(patterns["categories"])
                term_count = len(patterns["description_terms"])
                print(f"    Found patterns: {cat_count} categories, {term_count} description terms")
            else:
                print(f"    No history found for {company}")

            # ════════════════════════════════
            # FINAL: Blend Scores
            # ════════════════════════════════
            print(f"  [Final] Blending scores...")
            final_scored = []
            for cand in candidates:
                tender = cand["tender"]
                tid = tender.get("id", "")

                kw = cand["kw_score"]                          # 0-40
                ai = ai_scores.get(tid, {}).get("score", 0)    # 0-40
                hist = history_boost(tender, patterns)           # 0-20
                total = min(kw + ai + hist, 100)

                ai_reason = ai_scores.get(tid, {}).get("reason", "")

                final_scored.append({
                    "tender": tender,
                    "score": total,
                    "kw_score": kw,
                    "ai_score": ai,
                    "history_score": hist,
                    "kw_matches": cand["kw_matches"],
                    "ai_reason": ai_reason,
                })

            # Sort by final score
            final_scored.sort(key=lambda x: x["score"], reverse=True)

            # Apply minimum score threshold
            qualified = [f for f in final_scored if f["score"] >= min_score]
            top_matches = qualified[:max_matches]

            if not top_matches:
                print(f"    No matches above threshold ({min_score})")
                continue

            print(f"    Top match: {top_matches[0]['score']}% "
                  f"(kw:{top_matches[0]['kw_score']} ai:{top_matches[0]['ai_score']} "
                  f"hist:{top_matches[0]['history_score']})")

            # ── Write to matches table ──
            db.table("matches").delete().eq("user_id", user_id).execute()

            rows = []
            for rank, m in enumerate(top_matches):
                tender = m["tender"]
                is_locked = False if is_pro else (rank > 0)

                rows.append({
                    "user_id":         user_id,
                    "tender_id":       tender["id"],
                    "score":           m["score"],
                    "kw_score":        m["kw_score"],
                    "ai_score":        m["ai_score"],
                    "history_score":   m["history_score"],
                    "keyword_matches": m["kw_matches"],
                    "ai_explanation":  m["ai_reason"],
                    "is_locked":       is_locked,
                    "matched_at":      datetime.now(timezone.utc).isoformat(),
                })

            db.table("matches").insert(rows).execute()
            total_matches += len(rows)
            total_users_matched += 1
            print(f"    ✓ Wrote {len(rows)} matches")

        except Exception as e:
            errors += 1
            print(f"  ERROR for {company}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'='*60}")
    print(f"Pipeline complete.")
    print(f"  Users matched: {total_users_matched}/{len(profiles)}")
    print(f"  Total matches: {total_matches}")
    print(f"  Errors: {errors}")
    print(f"{'='*60}\n")

    return {
        "users_matched": total_users_matched,
        "total_matches": total_matches,
        "live_tenders": len(live_tenders),
        "errors": errors,
        "pipeline": "v2-3stage",
    }


def run_matching_single(db, user_id, anthropic_key=None):
    """
    Run the full 3-stage pipeline for a single user.
    Called when a user completes onboarding for instant results.
    """
    print(f"\n── Single-user match: {user_id[:8]}... ──")

    # Load profile
    profile_resp = db.table("profiles").select("*").eq("id", user_id).single().execute()
    profile = profile_resp.data
    if not profile:
        return {"error": "Profile not found"}

    # Load live tenders
    now = datetime.now(timezone.utc).isoformat()
    tenders_resp = db.table("tenders").select("*").gt("closing_date", now).execute()
    live_tenders = tenders_resp.data or []

    # Load subscription
    sub_resp = db.table("subscriptions").select("plan, status").eq("user_id", user_id).execute()
    sub = (sub_resp.data or [{}])[0] if sub_resp.data else {}
    is_pro = sub.get("plan") == "pro" and sub.get("status") == "active"

    # Stage 1: Keyword pre-filter
    prefiltered = []
    for tender in live_tenders:
        kw, matches = keyword_score(tender, profile)
        if kw > 0:
            prefiltered.append({"tender": tender, "kw_score": kw, "kw_matches": matches})

    prefiltered.sort(key=lambda x: x["kw_score"], reverse=True)
    candidates = prefiltered[:50]

    # Stage 2: AI scoring
    ai_scores = {}
    if anthropic_key and candidates:
        try:
            client = anthropic.Anthropic(api_key=anthropic_key)
            ai_scores = score_batch(client, profile, [c["tender"] for c in candidates])
        except Exception as e:
            print(f"  AI scoring failed: {e}")

    # Stage 3: History boost
    patterns = load_history_patterns(db, profile)

    # Blend
    final = []
    for cand in candidates:
        t = cand["tender"]
        tid = t.get("id", "")
        kw = cand["kw_score"]
        ai = ai_scores.get(tid, {}).get("score", 0)
        hist = history_boost(t, patterns)
        total = min(kw + ai + hist, 100)
        ai_reason = ai_scores.get(tid, {}).get("reason", "")

        final.append({
            "tender": t, "score": total,
            "kw_score": kw, "ai_score": ai, "history_score": hist,
            "kw_matches": cand["kw_matches"], "ai_reason": ai_reason,
        })

    final.sort(key=lambda x: x["score"], reverse=True)
    top = [f for f in final if f["score"] >= 15][:25]

    # Write
    db.table("matches").delete().eq("user_id", user_id).execute()
    if top:
        rows = [{
            "user_id": user_id,
            "tender_id": m["tender"]["id"],
            "score": m["score"],
            "kw_score": m["kw_score"],
            "ai_score": m["ai_score"],
            "history_score": m["history_score"],
            "keyword_matches": m["kw_matches"],
            "ai_explanation": m["ai_reason"],
            "is_locked": False if is_pro else (rank > 0),
            "matched_at": datetime.now(timezone.utc).isoformat(),
        } for rank, m in enumerate(top)]
        db.table("matches").insert(rows).execute()

    return {
        "status": "ok",
        "user_id": user_id,
        "matches": len(top),
        "top_score": top[0]["score"] if top else 0,
        "pipeline": "v2-3stage",
    }
