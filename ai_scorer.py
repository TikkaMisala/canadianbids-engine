"""
ai_scorer.py — Semantic matching using Claude Haiku (v4, capability-focused).

In the v4 pipeline, the AI is responsible for ONLY two things:
  1. domain_match (gate)  — does the company's actual industry align
                            with the tender's actual industry?
  2. capability_score (0-15) — given that the domains align, can the
                            company actually deliver this specific work?

Geography, clearance, scale, certs, and procurement vehicle are all
handled deterministically in requirements.py. The AI never sees those
checks because they're facts, not judgment calls.

This produces a sharper, cheaper, more reliable score than asking the
AI to judge five dimensions at once.

Returns: {tender_id: {"score": int (0-15), "reason": str,
                      "domain_match": bool, "tender_domain": str,
                      "company_domain": str}}

The score is bounded to 0-15 (capability only). The matcher uses
domain_match to drop tenders with hallucinated overlap, and surfaces
tender_domain / company_domain for debugging.
"""

import anthropic
import json
import time

SCORING_PROMPT = """You are a Canadian federal procurement matching engine. Decide whether this tender is a real fit for the company below.

Geography, clearance level, contract size, certifications, and procurement vehicle have already been checked separately. Your job is ONLY to decide:
1. Do the company and tender operate in the same business domain (industry)?
2. If yes, does the company have the specific capability to deliver this work?

CRITICAL RULE — DOMAIN MATCH
A tender is only a match if the company actually operates in the tender's industry. Surface-level overlap (both involve "consulting", "advisory", "project management", "services", "support") is NOT a match. The underlying business domain must align.

EXAMPLES OF DOMAIN MATCH
  TRUE MATCH:
    IT consulting firm + IT advisory tender — same industry
    Construction contractor + building renovation — same industry
    Cybersecurity firm + penetration testing — same industry

  ADJACENT (judgment call):
    General IT firm + specialized cybersecurity tender
      → match if profile shows cyber experience, otherwise no
    Management consulting + digital transformation
      → match if profile shows digital work, otherwise no
    Lean toward MATCH if the company's described work overlaps even
    partially. Lean toward NO MATCH if the gap requires capabilities
    the profile doesn't show.

  MISMATCH:
    Real estate cost consulting + cybersecurity advisory — different industries
    Janitorial services + software development — different industries
    HR consulting + bridge engineering — different industries

GUARDRAIL
If the company's description and service_types/goods_types/keywords together provide weak signal about what they actually do, do NOT aggressively reject. Use whatever signal exists and lean toward inclusion. Only set domain_match=false when there is clear positive evidence of mismatch.

CAPABILITY SCORING (0-15, only when domain_match=true)
  15 — Profile explicitly lists the exact services or goods required
  10 — Profile shows clear adjacent capability the company likely has
   5 — Profile suggests possible capability but it's a stretch
   0 — Profile shows no relevant capability for this specific work

COMPANY PROFILE
Company: {company_name}
Description: {description}
Service types: {service_types}
Goods types: {goods_types}
Construction types: {construction_types}
Industry keywords: {keywords}
Categories: {categories}

TENDER
Title: {tender_title}
Department: {tender_department}
Description: {tender_description}
Category: {tender_category}
Notice type: {tender_notice_type}
Procurement method: {tender_procurement_method}
Selection criteria: {tender_selection_criteria}

REASONING STEPS
1. Identify the tender's core business domain in 2-5 words (what kind of company would actually deliver this work?).
2. Identify the company's domain in 2-5 words (what they actually do, based on the profile).
3. Decide domain_match: TRUE if same/adjacent industry, FALSE if fundamentally different industries.
4. If domain_match=false → capability score must be 0.
5. If domain_match=true → score capability 0-15 using the rubric above.

OUTPUT FORMAT
Respond with ONLY a JSON object, no markdown, no preamble:
{{"tender_domain": "...", "company_domain": "...", "domain_match": true, "score": <0-15>, "reason": "<one sentence>"}}"""


BATCH_PROMPT = """You are a Canadian federal procurement matching engine. For each tender, decide whether it's a real fit for the company below.

Geography, clearance level, contract size, certifications, and procurement vehicle have already been checked separately. Your job is ONLY to decide:
1. Do the company and tender operate in the same business domain (industry)?
2. If yes, does the company have the specific capability to deliver this work?

CRITICAL RULE — DOMAIN MATCH
A tender is only a match if the company actually operates in the tender's industry. Surface-level overlap (both involve "consulting", "advisory", "project management", "services", "support") is NOT a match. The underlying business domain must align.

EXAMPLES OF DOMAIN MATCH
  TRUE MATCH:
    IT consulting firm + IT advisory tender — same industry
    Construction contractor + building renovation — same industry
    Cybersecurity firm + penetration testing — same industry

  ADJACENT (judgment call):
    General IT firm + specialized cybersecurity tender
      → match if profile shows cyber experience, otherwise no
    Management consulting + digital transformation
      → match if profile shows digital work, otherwise no
    Lean toward MATCH if the company's described work overlaps even
    partially. Lean toward NO MATCH if the gap requires capabilities
    the profile doesn't show.

  MISMATCH:
    Real estate cost consulting + cybersecurity advisory — different industries
    Janitorial services + software development — different industries
    HR consulting + bridge engineering — different industries

GUARDRAIL
If the company's description and service_types/goods_types/keywords together provide weak signal about what they actually do, do NOT aggressively reject. Use whatever signal exists and lean toward inclusion. Only set domain_match=false when there is clear positive evidence of mismatch.

BOILERPLATE GUARDRAIL
Tender language like "all qualified bidders may respond" or "open to all suppliers" is boilerplate and doesn't mean the tender has no real requirements. Always read the actual scope of work.

CAPABILITY SCORING (0-15, only when domain_match=true)
  15 — Profile explicitly lists the exact services or goods required
  10 — Profile shows clear adjacent capability the company likely has
   5 — Profile suggests possible capability but it's a stretch
   0 — Profile shows no relevant capability for this specific work

COMPANY PROFILE
Company: {company_name}
Description: {description}
Service types: {service_types}
Goods types: {goods_types}
Construction types: {construction_types}
Industry keywords: {keywords}
Categories: {categories}

TENDERS TO SCORE
{tender_list}

FOR EACH TENDER
1. Identify the tender's core business domain in 2-5 words.
2. Identify the company's domain in 2-5 words.
3. Decide domain_match: TRUE if same/adjacent industry, FALSE if fundamentally different industries.
4. If domain_match=false → capability score must be 0.
5. If domain_match=true → score capability 0-15 using the rubric above.

OUTPUT FORMAT
Respond with ONLY a JSON array, no markdown, no preamble. Use the tender number from the list as the id:
[{{"id": 1, "tender_domain": "...", "company_domain": "...", "domain_match": true, "score": 12, "reason": "..."}}, ...]

When domain_match=false, score MUST be 0 and reason should briefly explain the domain mismatch."""


def build_profile_context(profile):
    """Extract profile fields with safe defaults."""
    return {
        "company_name": profile.get("company_name") or "Unknown",
        "description": (profile.get("description") or "No description provided")[:500],
        "province": profile.get("province") or "Not specified",
        "provinces_operating": ", ".join(profile.get("provinces_operating") or []) or "Not specified",
        "categories": ", ".join(profile.get("categories") or []),
        "service_types": ", ".join(profile.get("service_types") or []) or "None",
        "goods_types": ", ".join(profile.get("goods_types") or []) or "None",
        "construction_types": ", ".join(profile.get("construction_types") or []) or "None",
        "keywords": ", ".join(profile.get("keywords") or []),
        "certifications": ", ".join(profile.get("certifications") or []) or "None",
        "clearance_level": profile.get("clearance_level") or "None",
        "employee_count": profile.get("employee_count") or "Unknown",
        "contract_min": profile.get("contract_min") or "0",
        "contract_max": profile.get("contract_max") or "Any",
        "delivers_nationally": "Yes" if profile.get("delivers_nationally") else "No",
    }


def build_tender_context(tender):
    """Extract tender fields with safe defaults."""
    desc = (tender.get("description") or "")[:600]
    return {
        "tender_title": tender.get("title") or "",
        "tender_department": tender.get("department") or "",
        "tender_description": desc,
        "tender_category": tender.get("category") or "",
        "tender_region": tender.get("region") or "",
        "tender_notice_type": tender.get("notice_type") or "",
        "tender_procurement_method": tender.get("procurement_method") or "",
        "tender_selection_criteria": tender.get("selection_criteria") or "",
    }


def score_batch(client, profile, tenders, batch_size=10):
    """
    Score a batch of tenders against a profile using Claude Haiku.
    Uses batched prompts to reduce API calls.
    Uses numeric indices instead of UUIDs for reliable ID matching.

    Returns dict: {tender_id: {"score": int, "reason": str, "domain_match": bool}}

    domain_match defaults to True on any parse failure or missing field,
    so the matcher's domain filter fails-safe (keeps the tender, falls
    back to keyword + history scoring).
    """
    pctx = build_profile_context(profile)
    results = {}

    # Process in batches
    for i in range(0, len(tenders), batch_size):
        batch = tenders[i:i + batch_size]

        # Map numeric indices to real tender IDs
        idx_to_id = {}
        tender_lines = []
        for j, t in enumerate(batch):
            idx = j + 1  # 1-based index
            idx_to_id[str(idx)] = t.get("id", "unknown")
            title = t.get("title", "")[:100]
            dept = t.get("department", "")
            desc = (t.get("description") or "")[:500]   # was 200 — more context for domain detection
            cat = t.get("category", "")
            region = t.get("region", "")
            tender_lines.append(
                f"Tender {idx}:\n  Title: {title}\n  Department: {dept}\n  "
                f"Category: {cat} | Region: {region}\n  Description: {desc}"
            )

        prompt = BATCH_PROMPT.format(
            **pctx,
            tender_list="\n\n".join(tender_lines)
        )

        # Retry up to 2 times for overload errors
        for attempt in range(3):
            try:
                message = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=1500,
                    messages=[{"role": "user", "content": prompt}]
                )

                raw = message.content[0].text.strip()
                # Clean potential markdown fences
                if raw.startswith("```"):
                    raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
                    if raw.endswith("```"):
                        raw = raw[:-3]
                    raw = raw.strip()

                scores = json.loads(raw)

                for item in scores:
                    # Map numeric index back to real tender ID
                    idx_str = str(item.get("id", ""))
                    real_id = idx_to_id.get(idx_str, "")
                    if not real_id:
                        # Try matching by position if index doesn't map
                        continue
                    # Capability score is 0-15 in v4 (was 0-35 in v3)
                    score = max(0, min(15, int(item.get("score", 0))))
                    reason = item.get("reason", "")
                    # domain_match: default True on missing field (fail-safe).
                    # Only an explicit False from the AI causes the matcher
                    # to drop this tender.
                    domain_match = item.get("domain_match", True)
                    if domain_match is False and score > 0:
                        # AI contradicted itself — domain mismatch but non-zero
                        # score. Force score to 0 to be consistent with the
                        # prompt rule.
                        score = 0
                    results[real_id] = {
                        "score": score,
                        "reason": reason,
                        "domain_match": bool(domain_match),
                        # Debug/UI fields — show what the AI thought each side was
                        "tender_domain": item.get("tender_domain", ""),
                        "company_domain": item.get("company_domain", ""),
                    }

                break  # Success, exit retry loop

            except json.JSONDecodeError as e:
                print(f"  AI batch JSON error: {e}")
                print(f"  Raw response: {raw[:200]}")
                for j, t in enumerate(batch):
                    results[t["id"]] = {
                        "score": 0,
                        "reason": "AI scoring unavailable",
                        "domain_match": True,  # fail-safe: keep tender
                        "tender_domain": "",
                        "company_domain": "",
                    }
                break
            except Exception as e:
                err_msg = str(e)
                if "529" in err_msg or "overloaded" in err_msg.lower():
                    if attempt < 2:
                        print(f"  AI overloaded, retrying in {2 * (attempt+1)}s...")
                        time.sleep(2 * (attempt + 1))
                        continue
                print(f"  AI batch error: {e}")
                for j, t in enumerate(batch):
                    results[t["id"]] = {
                        "score": 0,
                        "reason": "AI scoring unavailable",
                        "domain_match": True,  # fail-safe: keep tender
                        "tender_domain": "",
                        "company_domain": "",
                    }
                break

        # Rate limiting between batches
        if i + batch_size < len(tenders):
            time.sleep(0.5)

    return results


def score_single(client, profile, tender):
    """
    Score a single tender against a profile. Used for on-demand matching
    (e.g., when a user completes onboarding).

    Returns {"score": int, "reason": str, "domain_match": bool}
    """
    pctx = build_profile_context(profile)
    tctx = build_tender_context(tender)

    prompt = SCORING_PROMPT.format(**pctx, **tctx)

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )

        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()

        result = json.loads(raw)
        # Capability score is 0-15 in v4 (was 0-35 in v3)
        score = max(0, min(15, int(result.get("score", 0))))
        reason = result.get("reason", "")
        # domain_match: default True if missing (fail-safe — keep tender)
        domain_match = result.get("domain_match", True)
        if domain_match is False and score > 0:
            # AI contradicted itself — force score to 0 for consistency
            score = 0
        return {
            "score": score,
            "reason": reason,
            "domain_match": bool(domain_match),
            "tender_domain": result.get("tender_domain", ""),
            "company_domain": result.get("company_domain", ""),
        }

    except Exception as e:
        print(f"  AI single score error: {e}")
        return {
            "score": 0,
            "reason": "AI scoring unavailable",
            "domain_match": True,  # fail-safe: keep tender
            "tender_domain": "",
            "company_domain": "",
        }
