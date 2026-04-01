"""
matcher.py — Scores tenders against user profiles and writes to matches table.

Scoring breakdown (max 100):
  - Keyword match in title:       +15 per keyword (max 45)
  - Keyword match in description: +5  per keyword (max 25)
  - Category match (SRV/GD/CNST): +15
  - Region match:                 +10
  - Contract size in range:       +5
  Total possible:                 100
"""

import re
from datetime import datetime, timezone


def normalize(text):
    """Lowercase and strip punctuation for keyword matching."""
    if not text:
        return ""
    return re.sub(r'[^\w\s]', ' ', text.lower())


def score_tender(tender, profile):
    """
    Score a single tender against a user profile.
    Returns (score: int, matched_keywords: list[str])
    """
    score = 0
    matched_keywords = []

    keywords = [k.lower().strip() for k in (profile.get("keywords") or []) if k]
    if not keywords:
        return 0, []

    title = normalize(tender.get("title") or "")
    desc = normalize(tender.get("description") or "")
    dept = normalize(tender.get("department") or "")

    # ── Keyword matching ──
    for kw in keywords:
        kw_norm = normalize(kw)
        if not kw_norm:
            continue

        in_title = kw_norm in title
        in_desc = kw_norm in desc
        in_dept = kw_norm in dept

        if in_title:
            score += 15
            if kw not in matched_keywords:
                matched_keywords.append(kw)
        elif in_desc:
            score += 5
            if kw not in matched_keywords:
                matched_keywords.append(kw)
        elif in_dept:
            score += 3
            if kw not in matched_keywords:
                matched_keywords.append(kw)

    # Cap keyword score at 70
    score = min(score, 70)

    if score == 0:
        return 0, []  # No keyword matches at all — not relevant

    # ── Category match ──
    tender_cats = [c.strip() for c in (tender.get("category") or "").split(",")]
    profile_cats = profile.get("categories") or []
    if any(c in tender_cats for c in profile_cats):
        score += 15

    # ── Region match ──
    tender_region = normalize(tender.get("region") or "")
    profile_province = normalize(profile.get("province") or "")
    profile_provs = [normalize(p) for p in (profile.get("provinces_operating") or [])]

    all_profile_regions = ([profile_province] if profile_province else []) + profile_provs
    if any(r and r in tender_region for r in all_profile_regions):
        score += 10
    elif "national" in tender_region or "canada" in tender_region:
        score += 5  # National tenders are relevant to everyone

    # ── Contract size ──
    contract_min = profile.get("contract_min")
    contract_max = profile.get("contract_max")
    # We don't have contract value on tenders, so skip if not available
    # This will be improved when we add value estimation

    # Cap at 100
    score = min(score, 100)
    return score, matched_keywords


def run_matching(db, min_score=20, max_matches_per_user=25):
    """
    Main matching job. Reads all profiles and tenders, computes scores,
    writes results to the matches table.
    
    Returns summary dict with counts.
    """
    print(f"\n{'='*50}")
    print(f"Starting matching job at {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*50}")

    print(f"\nLoading tenders...")
    try:
        tenders_resp = db.table("tenders").select(
            "id, title, description, department, category, region, procurement_method, selection_criteria, closing_date, notice_type, solicitation_number"
        ).execute()
        tenders = tenders_resp.data or []
    except Exception as e:
        print(f"ERROR loading tenders: {e}")
        return {"users_matched": 0, "total_matches": 0, "live_tenders": 0, "errors": 1}

    # Filter to tenders with a future closing date
    now = datetime.now(timezone.utc).isoformat()
    live_tenders = [
        t for t in tenders
        if t.get("closing_date") and t["closing_date"] > now
    ]
    print(f"  Total tenders: {len(tenders)}, Live: {len(live_tenders)}")

    # ── Load all profiles with completed onboarding ──
    print("\nLoading profiles...")
    profiles_resp = db.table("profiles").select("*").execute()
    profiles = [p for p in (profiles_resp.data or []) if p.get("onboarding_complete") == True]
    print(f"  Profiles to match: {len(profiles)}")

    # ── Load all subscriptions ──
    subs_resp = db.table("subscriptions").select("user_id, plan, status").execute()
    subs_by_user = {s["user_id"]: s for s in (subs_resp.data or [])}

    total_matches = 0
    total_users_matched = 0
    errors = 0

    for profile in profiles:
        user_id = profile.get("id")
        if not user_id:
            continue

        # Determine plan
        sub = subs_by_user.get(user_id, {})
        is_pro = sub.get("plan") == "pro" and sub.get("status") == "active"

        try:
            # Score all live tenders
            scored = []
            for tender in live_tenders:
                score, keywords = score_tender(tender, profile)
                if score >= min_score:
                    scored.append({
                        "score": score,
                        "keywords": keywords,
                        "tender": tender,
                    })

            # Sort by score descending
            scored.sort(key=lambda x: x["score"], reverse=True)
            top_matches = scored[:max_matches_per_user]

            if not top_matches:
                print(f"  User {user_id[:8]}...: no matches above threshold")
                continue

            # ── Delete existing matches for this user ──
            db.table("matches").delete().eq("user_id", user_id).execute()

            # ── Insert new matches ──
            rows = []
            for rank, m in enumerate(top_matches):
                tender = m["tender"]
                # Free users: only first match is unlocked
                is_locked = False if is_pro else (rank > 0)

                rows.append({
                    "user_id":        user_id,
                    "tender_id":      tender["id"],
                    "score":          m["score"],
                    "keyword_matches": m["keywords"],
                    "is_locked":      is_locked,
                    "matched_at":     datetime.now(timezone.utc).isoformat(),
                })

            db.table("matches").insert(rows).execute()
            total_matches += len(rows)
            total_users_matched += 1
            print(f"  User {user_id[:8]}...: {len(rows)} matches (top score: {top_matches[0]['score']}%)")

        except Exception as e:
            errors += 1
            print(f"  ERROR for user {user_id[:8]}...: {e}")

    print(f"\n{'='*50}")
    print(f"Matching complete.")
    print(f"  Users matched: {total_users_matched}/{len(profiles)}")
    print(f"  Total matches written: {total_matches}")
    print(f"  Errors: {errors}")
    print(f"{'='*50}\n")

    return {
        "users_matched": total_users_matched,
        "total_matches": total_matches,
        "live_tenders": len(live_tenders),
        "errors": errors,
    }
