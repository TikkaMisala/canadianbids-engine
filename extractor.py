"""
extractor.py
------------
Uses Claude API to extract structured fields from tender descriptions.
Stores results back into the tenders table as proper columns.

Run standalone:   python extractor.py
Called from:      app.py via /api/extract endpoint
Also called by:   run_all() alongside summarizer

Extracted fields per tender:
  - security_clearance:     None | Reliability | Secret | Top Secret
  - contract_duration:      e.g. '24 months + 3 x 12-month options'
  - estimated_value_text:   e.g. '$1,000,000' (from description text)
  - eligibility:            e.g. 'Canadian suppliers only'
  - set_aside:              e.g. 'Indigenous PSIB' or null
  - delivery_location:      e.g. 'Oromocto, NB'
  - mandatory_requirements: list of strings e.g. ['Mandatory site visit', 'Security clearance required']
"""

import os
import json
import logging
import anthropic
from supabase import create_client, Client
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

EXTRACTION_PROMPT = """You are extracting structured procurement data from a Canadian federal government tender description.

Analyze the tender text below and extract the following fields. Return ONLY a valid JSON object with exactly these keys — no preamble, no markdown, no explanation:

{
  "security_clearance": "None" | "Reliability" | "Secret" | "Top Secret",
  "contract_duration": "string describing contract length, e.g. '24 months + 3 x 12-month options'" | null,
  "estimated_value_text": "dollar amount mentioned in description, e.g. '$500,000'" | null,
  "eligibility": "who can bid, e.g. 'Canadian suppliers only' or 'Open to all suppliers'" | null,
  "set_aside": "e.g. 'Indigenous PSIB set-aside' or null if not a set-aside",
  "delivery_location": "specific city/province/base where work is performed" | null,
  "mandatory_requirements": ["array", "of", "mandatory", "items", "e.g. Mandatory site visit, Security clearance, Bonding required"]
}

Rules:
- security_clearance: look for words like "Reliability", "Secret", "Top Secret". If no clearance mentioned, use "None".
- contract_duration: look for contract period, option periods. Summarize concisely.
- estimated_value_text: look for dollar amounts, budget ranges. Use the most specific one found.
- eligibility: look for "Canadian suppliers", "limited to", "set-aside", trade agreement restrictions.
- set_aside: specifically look for Indigenous PSIB, women-owned, etc. Null if not mentioned.
- delivery_location: the physical place where work happens, not the buyer's office city.
- mandatory_requirements: ONLY truly mandatory items (site visits, clearances, certifications). Keep each item to 5 words max. Empty array [] if none.

TENDER TEXT:
"""


def extract_fields(tender_id: int, title: str, description: str) -> dict | None:
    """
    Call Claude to extract structured fields from a tender description.
    Returns a dict of fields, or None on failure.
    """
    if not description or len(description.strip()) < 50:
        log.warning(f"[{tender_id}] Description too short to extract, skipping")
        return None

    # Truncate very long descriptions to save tokens (first 3000 chars is enough)
    text = description[:3000].strip()
    prompt = EXTRACTION_PROMPT + f"TITLE: {title}\n\n{text}"

    try:
        response = claude.messages.create(
            model="claude-haiku-4-5-20251001",  # Use Haiku — fast and cheap for extraction
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()

        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        extracted = json.loads(raw)
        return extracted

    except json.JSONDecodeError as e:
        log.error(f"[{tender_id}] JSON parse error: {e} — raw: {raw[:200]}")
        return None
    except Exception as e:
        log.error(f"[{tender_id}] Claude API error: {e}")
        return None


def save_extracted_fields(tender_id: int, fields: dict) -> bool:
    """Save extracted fields back to the tenders table."""
    try:
        update = {
            "security_clearance":     fields.get("security_clearance"),
            "contract_duration":      fields.get("contract_duration"),
            "estimated_value_text":   fields.get("estimated_value_text"),
            "eligibility":            fields.get("eligibility"),
            "set_aside":              fields.get("set_aside"),
            "delivery_location":      fields.get("delivery_location"),
            "mandatory_requirements": fields.get("mandatory_requirements") or [],
            "extracted_at":           datetime.now(timezone.utc).isoformat(),
        }
        supabase.table("tenders").update(update).eq("id", tender_id).execute()
        return True
    except Exception as e:
        log.error(f"[{tender_id}] Supabase update error: {e}")
        return False


def run_extractor(batch_size: int = 25) -> dict:
    """
    Extract structured fields for tenders that haven't been processed yet.
    Returns summary dict.
    """
    log.info(f"=== Extractor started {datetime.now(timezone.utc).isoformat()} ===")

    # Fetch unextracted tenders that have a description
    result = supabase.table("tenders") \
        .select("id, title, description") \
        .is_("extracted_at", "null") \
        .not_.is_("description", "null") \
        .limit(batch_size) \
        .execute()

    tenders = result.data or []
    log.info(f"Found {len(tenders)} tenders to extract")

    extracted = 0
    errors = 0

    for tender in tenders:
        tender_id   = tender["id"]
        title       = tender.get("title", "")
        description = tender.get("description", "")

        fields = extract_fields(tender_id, title, description)

        if fields:
            success = save_extracted_fields(tender_id, fields)
            if success:
                extracted += 1
                log.info(f"[{tender_id}] Extracted: clearance={fields.get('security_clearance')}, "
                         f"duration={fields.get('contract_duration')}, "
                         f"reqs={len(fields.get('mandatory_requirements') or [])}")
            else:
                errors += 1
        else:
            # Mark as attempted so we don't retry forever on bad descriptions
            supabase.table("tenders").update({
                "extracted_at": datetime.now(timezone.utc).isoformat()
            }).eq("id", tender_id).execute()
            errors += 1

    log.info(f"=== Done. {extracted} extracted, {errors} errors ===")
    return {"extracted": extracted, "errors": errors, "total": len(tenders)}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract structured fields from tender descriptions")
    parser.add_argument("--batch", type=int, default=25, help="Tenders to process per run")
    args = parser.parse_args()
    run_extractor(batch_size=args.batch)
