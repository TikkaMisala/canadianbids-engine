"""
extract_quebec_leads.py
-----------------------
Extracts contract award winners from Quebec SEAO open data.
Loads them into the vendor_history table for:
  1. Lead enrichment (Serper → Hunter pipeline)
  2. Matching engine history scoring
  3. Onboarding company autocomplete

These are companies that have ACTUALLY WON Quebec government contracts —
the highest-intent leads possible for CanadianBids.ai.

Usage:
  python extract_quebec_leads.py                # last 6 months of awards
  python extract_quebec_leads.py --weeks 52     # full year
  python extract_quebec_leads.py --dry-run      # preview without DB writes
"""

import os
import json
import logging
import requests
from datetime import datetime, timezone
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CKAN_API = "https://www.donneesquebec.ca/recherche/api/3/action/package_show"
DATASET_ID = "systeme-electronique-dappel-doffres-seao"

HEADERS = {
    "User-Agent": "CanadianBidsAI/1.0 (procurement-intelligence)",
    "Accept": "application/json",
}

# OCDS category → our category codes
CATEGORY_MAP = {
    "goods": "GD",
    "services": "SRV",
    "works": "CNST",
    "consultingServices": "SRV",
}


def fetch_resource_list() -> list[dict]:
    """Query CKAN API for SEAO dataset resources."""
    try:
        resp = requests.get(CKAN_API, params={"id": DATASET_ID}, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("result", {}).get("resources", [])
    except Exception as e:
        log.error(f"CKAN API error: {e}")
        return []


def find_weekly_files(resources: list[dict], weeks: int = 26) -> list[dict]:
    """Find the most recent weekly JSON files."""
    weekly = []
    for r in resources:
        name = (r.get("name") or "").lower()
        url = r.get("url") or ""
        fmt = (r.get("format") or "").upper()

        if "hebdo_" in name and (fmt == "JSON" or url.endswith(".json")):
            try:
                parts = name.replace("hebdo_", "").replace(".json", "").split("_")
                if len(parts) >= 2:
                    r["_sort_date"] = parts[1][:8]
                    weekly.append(r)
            except (IndexError, ValueError):
                weekly.append(r)

    weekly.sort(key=lambda x: x.get("_sort_date", ""), reverse=True)

    # Deduplicate by URL
    seen = set()
    deduped = []
    for r in weekly:
        url = r.get("url", "")
        if url not in seen:
            seen.add(url)
            deduped.append(r)

    return deduped[:weeks]


def download_json(url: str) -> dict | None:
    """Download and parse a SEAO JSON file."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.error(f"Download error: {e}")
        return None


def extract_award_winners(ocds_data: dict) -> list[dict]:
    """
    Extract companies that won contracts from OCDS releases.
    Returns list of vendor_history-compatible dicts.
    """
    releases = ocds_data.get("releases", [])
    winners = []

    for release in releases:
        tags = release.get("tag", [])
        awards = release.get("awards", [])
        tender_obj = release.get("tender", {})

        # We want releases that contain award data
        if not awards:
            continue

        # Tender context for the award
        tender_title = tender_obj.get("title", "")
        tender_desc = tender_obj.get("description", "")
        main_cat = (tender_obj.get("mainProcurementCategory") or "").lower()
        category = CATEGORY_MAP.get(main_cat, "SRV")

        # Buyer info
        buyer = release.get("buyer", {})
        buyer_name = buyer.get("name", "")

        # GSIN/UNSPSC from items
        gsin_desc = ""
        for item in tender_obj.get("items", []):
            classification = item.get("classification", {})
            desc = classification.get("description", "")
            if desc:
                gsin_desc = desc
                break

        for award in awards:
            award_status = (award.get("status") or "").lower()
            # Only active/completed awards — skip cancelled/pending
            if award_status not in ("active", ""):
                if award_status in ("cancelled", "unsuccessful"):
                    continue

            award_value = 0
            value_obj = award.get("value", {})
            if value_obj and value_obj.get("amount"):
                try:
                    award_value = float(value_obj["amount"])
                except (ValueError, TypeError):
                    pass

            award_date = award.get("date", "")

            # Extract each winning supplier
            suppliers = award.get("suppliers", [])
            for supplier in suppliers:
                supplier_name = (supplier.get("name") or "").strip()
                if not supplier_name or len(supplier_name) < 3:
                    continue

                # Get address from supplier or from parties list
                province = "Quebec"
                city = ""
                address_obj = supplier.get("address", {})
                if address_obj:
                    city = address_obj.get("locality", "")
                    region = address_obj.get("region", "")
                    if region:
                        province = region

                # If no address in supplier, check parties
                if not city:
                    supplier_id = supplier.get("id", "")
                    for party in release.get("parties", []):
                        if party.get("id") == supplier_id:
                            addr = party.get("address", {})
                            city = addr.get("locality", "")
                            region = addr.get("region", "")
                            if region:
                                province = region
                            break

                winners.append({
                    "supplier_legal_name": supplier_name[:500],
                    "supplier_province": province,
                    "total_contract_value": award_value if award_value > 0 else None,
                    "contract_amount": award_value if award_value > 0 else None,
                    "tender_description_en": tender_title[:500] if tender_title else tender_desc[:500] if tender_desc else None,
                    "gsin_description_en": gsin_desc[:500] if gsin_desc else None,
                    "procurement_category": category,
                    "source_system": "seao",
                })

    return winners


def deduplicate_winners(all_winners: list[dict]) -> list[dict]:
    """
    Deduplicate and aggregate winners by company name.
    Keeps the highest contract value and concatenates descriptions.
    """
    company_map = {}

    for w in all_winners:
        name = w["supplier_legal_name"].upper().strip()

        if name not in company_map:
            company_map[name] = {
                **w,
                "_contract_count": 1,
                "_total_value": w.get("total_contract_value") or 0,
                "_descriptions": set(),
            }
            if w.get("tender_description_en"):
                company_map[name]["_descriptions"].add(w["tender_description_en"])
        else:
            existing = company_map[name]
            existing["_contract_count"] += 1

            # Keep the highest contract value
            new_val = w.get("total_contract_value") or 0
            existing["_total_value"] += new_val
            if new_val > (existing.get("total_contract_value") or 0):
                existing["total_contract_value"] = new_val

            # Collect unique descriptions
            if w.get("tender_description_en"):
                existing["_descriptions"].add(w["tender_description_en"])

            # Keep province if we didn't have it
            if not existing.get("supplier_province") or existing["supplier_province"] == "Quebec":
                if w.get("supplier_province") and w["supplier_province"] != "Quebec":
                    existing["supplier_province"] = w["supplier_province"]

    # Clean up internal tracking fields
    results = []
    for name, data in company_map.items():
        # Use original casing from first occurrence
        row = {
            "supplier_legal_name": data["supplier_legal_name"],
            "supplier_province": data.get("supplier_province", "Quebec"),
            "total_contract_value": data.get("_total_value") or data.get("total_contract_value"),
            "contract_amount": data.get("contract_amount"),
            "tender_description_en": data.get("tender_description_en"),
            "gsin_description_en": data.get("gsin_description_en"),
            "procurement_category": data.get("procurement_category"),
        }
        results.append(row)

    return results


def upsert_vendors(rows: list[dict]) -> int:
    """
    Insert vendor records into vendor_history table.
    Uses insert (not upsert) because vendor_history stores one row per contract,
    and the same company can have multiple contracts.
    Skips rows where supplier_legal_name already exists to avoid bloating on re-runs.
    """
    if not rows:
        return 0

    # Get existing Quebec vendors to avoid duplicates on re-run
    existing_names = set()
    try:
        for i in range(0, len(rows), 500):
            names_batch = [r["supplier_legal_name"] for r in rows[i:i + 500]]
            result = supabase.table("vendor_history") \
                .select("supplier_legal_name") \
                .in_("supplier_legal_name", names_batch) \
                .execute()
            for r in (result.data or []):
                existing_names.add(r["supplier_legal_name"])
    except Exception as e:
        log.warning(f"  Could not check existing vendors: {e}")

    # Filter to only new companies
    new_rows = [r for r in rows if r["supplier_legal_name"] not in existing_names]
    log.info(f"  {len(existing_names)} already in DB, {len(new_rows)} new companies to insert")

    if not new_rows:
        return 0

    success = 0
    for i in range(0, len(new_rows), 200):
        batch = new_rows[i:i + 200]
        try:
            supabase.table("vendor_history").insert(batch).execute()
            success += len(batch)
        except Exception as e:
            log.warning(f"  Batch insert error: {e}")
            for row in batch:
                try:
                    supabase.table("vendor_history").insert([row]).execute()
                    success += 1
                except Exception as e2:
                    pass  # Silently skip duplicates

    return success


def run_extract_leads(weeks: int = 26, dry_run: bool = False) -> dict:
    """
    Main entry point: extract Quebec contract winners for lead generation.

    Args:
        weeks: How many weekly files to process (26 = ~6 months)
        dry_run: Preview without writing to DB
    """
    log.info(f"=== Quebec lead extraction started {datetime.now(timezone.utc).isoformat()} ===")
    log.info(f"  Weeks: {weeks}, Dry run: {dry_run}")

    # Step 1: Get resource list
    resources = fetch_resource_list()
    if not resources:
        return {"error": "CKAN API returned no resources"}

    # Step 2: Find weekly files
    weekly_files = find_weekly_files(resources, weeks=weeks)
    log.info(f"  Found {len(weekly_files)} weekly files to process")

    if not weekly_files:
        return {"error": "No weekly files found"}

    # Step 3: Download and extract winners
    all_winners = []
    files_processed = 0

    for resource in weekly_files:
        url = resource.get("url", "")
        name = resource.get("name", "?")
        log.info(f"  Processing: {name}")

        ocds_data = download_json(url)
        if not ocds_data:
            continue

        winners = extract_award_winners(ocds_data)
        all_winners.extend(winners)
        files_processed += 1
        log.info(f"    Winners extracted: {len(winners)}")

    log.info(f"\n  Total raw winners: {len(all_winners)}")

    # Step 4: Deduplicate
    deduped = deduplicate_winners(all_winners)
    log.info(f"  Unique companies: {len(deduped)}")

    if dry_run:
        log.info("\nDRY RUN — Top 20 companies by contract value:")
        top = sorted(deduped, key=lambda x: x.get("total_contract_value") or 0, reverse=True)[:20]
        for v in top:
            val = v.get("total_contract_value") or 0
            log.info(f"    ${val:>12,.0f}  {v['supplier_legal_name'][:60]}  ({v.get('supplier_province', '?')})")
        return {
            "files_processed": files_processed,
            "raw_winners": len(all_winners),
            "unique_companies": len(deduped),
            "dry_run": True,
        }

    # Step 5: Upsert to vendor_history
    upserted = upsert_vendors(deduped)
    log.info(f"\n=== Done. {upserted} vendors upserted from {files_processed} files ===")

    return {
        "files_processed": files_processed,
        "raw_winners": len(all_winners),
        "unique_companies": len(deduped),
        "upserted": upserted,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract Quebec SEAO award winners for lead generation")
    parser.add_argument("--weeks", type=int, default=26, help="Weekly files to process (26 = ~6 months)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    args = parser.parse_args()
    run_extract_leads(weeks=args.weeks, dry_run=args.dry_run)
