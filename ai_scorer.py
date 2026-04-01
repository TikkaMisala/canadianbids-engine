"""
ai_scorer.py — Semantic matching using Claude Haiku.

Takes a user profile and a pre-filtered list of tender candidates,
scores each for relevance using Claude, returns scores + explanations.
"""

import anthropic
import json
import time

SCORING_PROMPT = """You are a Canadian federal procurement matching engine. Score how relevant this government tender is for the company below.

COMPANY PROFILE:
Company: {company_name}
Description: {description}
Province: {province}
Operates in: {provinces_operating}
Categories: {categories}
Service types: {service_types}
Goods types: {goods_types}
Construction types: {construction_types}
Keywords: {keywords}
Certifications: {certifications}
Clearance level: {clearance_level}
Employee count: {employee_count}
Contract range: ${contract_min} – ${contract_max}
Delivers nationally: {delivers_nationally}

TENDER:
Title: {tender_title}
Department: {tender_department}
Description: {tender_description}
Category: {tender_category}
Region: {tender_region}
Notice type: {tender_notice_type}
Procurement method: {tender_procurement_method}
Selection criteria: {tender_selection_criteria}

Score this tender from 0 to 40 based on:
- How well the company's capabilities match what the tender requires (0-20)
- How appropriate the tender scope/size is for this company (0-10)
- Geographic and category fit (0-10)

Respond with ONLY a JSON object, no other text:
{{"score": <number 0-40>, "reason": "<one sentence explanation>"}}"""


BATCH_PROMPT = """You are a Canadian federal procurement matching engine. Score how relevant each tender is for the company below.

COMPANY PROFILE:
Company: {company_name}
Description: {description}
Province: {province}
Operates in: {provinces_operating}
Categories: {categories}
Keywords: {keywords}
Service types: {service_types}
Certifications: {certifications}
Clearance level: {clearance_level}
Employee count: {employee_count}
Contract range: ${contract_min} – ${contract_max}

TENDERS TO SCORE:
{tender_list}

For each tender, score from 0 to 40 based on capability match, scope fit, and geographic fit.

Respond with ONLY a JSON array, no other text:
[{{"id": "<tender_id>", "score": <0-40>, "reason": "<one sentence>"}}, ...]"""


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
    
    Returns dict: {tender_id: {"score": int, "reason": str}}
    """
    pctx = build_profile_context(profile)
    results = {}
    
    # Process in batches
    for i in range(0, len(tenders), batch_size):
        batch = tenders[i:i + batch_size]
        
        # Build tender list for batch prompt
        tender_lines = []
        for t in batch:
            tid = t.get("id", "unknown")
            title = t.get("title", "")[:100]
            dept = t.get("department", "")
            desc = (t.get("description") or "")[:200]
            cat = t.get("category", "")
            region = t.get("region", "")
            tender_lines.append(
                f"ID: {tid}\n  Title: {title}\n  Department: {dept}\n  "
                f"Category: {cat} | Region: {region}\n  Description: {desc}"
            )
        
        prompt = BATCH_PROMPT.format(
            **pctx,
            tender_list="\n\n".join(tender_lines)
        )
        
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
                tid = item.get("id", "")
                score = max(0, min(40, int(item.get("score", 0))))
                reason = item.get("reason", "")
                results[tid] = {"score": score, "reason": reason}
                
        except json.JSONDecodeError as e:
            print(f"  AI batch JSON error: {e}")
            print(f"  Raw response: {raw[:200]}")
            # Fall back to individual scoring for this batch
            for t in batch:
                results[t["id"]] = {"score": 0, "reason": "AI scoring unavailable"}
        except Exception as e:
            print(f"  AI batch error: {e}")
            for t in batch:
                results[t["id"]] = {"score": 0, "reason": "AI scoring unavailable"}
        
        # Rate limiting between batches
        if i + batch_size < len(tenders):
            time.sleep(0.5)
    
    return results


def score_single(client, profile, tender):
    """
    Score a single tender against a profile. Used for on-demand matching
    (e.g., when a user completes onboarding).
    
    Returns {"score": int, "reason": str}
    """
    pctx = build_profile_context(profile)
    tctx = build_tender_context(tender)
    
    prompt = SCORING_PROMPT.format(**pctx, **tctx)
    
    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()
        
        result = json.loads(raw)
        score = max(0, min(40, int(result.get("score", 0))))
        return {"score": score, "reason": result.get("reason", "")}
        
    except Exception as e:
        print(f"  AI single score error: {e}")
        return {"score": 0, "reason": "AI scoring unavailable"}
