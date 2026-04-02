"""
fetch_quebec_seao.py
--------------------
Fetches Quebec provincial tender data from SEAO (Système électronique d'appel d'offres).
SEAO publishes weekly JSON files in OCDS (Open Contracting Data Standard) format
via Données Québec open data portal.

Data covers: Quebec provincial ministries, agencies, municipalities, health networks,
education networks, and other public bodies.

Usage:
  python fetch_quebec_seao.py              # fetch latest 4 weeks of data
  python fetch_quebec_seao.py --weeks 8    # fetch last 8 weeks
  python fetch_quebec_seao.py --dry-run    # preview without DB writes
"""

import os
import io
import json
import logging
import requests
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── SEAO / Données Québec configuration ──────────────────────────────────────
CKAN_API = "https://www.donneesquebec.ca/recherche/api/3/action/package_show"
DATASET_ID = "systeme-electronique-dappel-doffres-seao"
SEAO_NOTICE_BASE = "https://seao.ca/OpportunityPublication/ConsulterAvis"

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

# OCDS tender status → filter for active
ACTIVE_STATUSES = {"active", "open", "planned", "planning"}


def fetch_resource_list() -> list[dict]:
    """
    Query the CKAN API to get all resources (JSON files) for the SEAO dataset.
    Returns list of resource dicts with 'url', 'name', 'created' fields.
    """
    log.info(f"Querying CKAN API for SEAO dataset resources...")
    try:
        resp = requests.get(
            CKAN_API,
            params={"id": DATASET_ID},
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            log.error(f"CKAN API returned success=false")
            return []

        resources = data.get("result", {}).get("resources", [])
        log.info(f"  Found {len(resources)} total resources")
        return resources

    except requests.RequestException as e:
        log.error(f"CKAN API error: {e}")
        return []


def find_weekly_files(resources: list[dict], weeks: int = 4) -> list[dict]:
    """
    Filter resources to find the most recent weekly JSON files (hebdo_*.json).
    Returns up to `weeks` most recent files, sorted newest first.
    """
    weekly = []
    for r in resources:
        name = (r.get("name") or "").lower()
        url = r.get("url") or ""
        fmt = (r.get("format") or "").upper()

        # Match weekly JSON files: hebdo_YYYYMMDD_YYYYMMDD.json
        if "hebdo_" in name and (fmt == "JSON" or url.endswith(".json")):
            # Extract date range from filename for sorting
            try:
                # Parse the start date from the filename pattern
                parts = name.replace("hebdo_", "").replace(".json", "").split("_")
                if len(parts) >= 2:
                    end_date = parts[1][:8]  # YYYYMMDD
                    r["_sort_date"] = end_date
                    weekly.append(r)
            except (IndexError, ValueError):
                weekly.append(r)

    # Sort by date descending (newest first), deduplicate by URL
    weekly.sort(key=lambda x: x.get("_sort_date", ""), reverse=True)

    # Deduplicate — SEAO sometimes has duplicate entries
    seen_urls = set()
    deduped = []
    for r in weekly:
        url = r.get("url", "")
        if url not in seen_urls:
            seen_urls.add(url)
            deduped.append(r)

    result = deduped[:weeks]
    log.info(f"  Selected {len(result)} weekly files (requested {weeks})")
    for r in result:
        log.info(f"    - {r.get('name', '?')}")
    return result


def download_json(url: str) -> dict | None:
    """Download and parse a single SEAO JSON file."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.error(f"Download error for {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        log.error(f"JSON parse error for {url}: {e}")
        return None


def extract_tenders(ocds_data: dict) -> list[dict]:
    """
    Extract active tender records from an OCDS JSON file.
    Returns list of normalized tender dicts ready for our schema.
    """
    releases = ocds_data.get("releases", [])
    if not releases:
        return []

    now = datetime.now(timezone.utc)
    tenders = []
    seen_ids = set()

    for release in releases:
        tags = release.get("tag", [])
        tender_obj = release.get("tender", {})

        if not tender_obj:
            continue

        # We want tender-related releases
        if not any(t in ["tender", "tenderUpdate", "tenderAmendment", "planning"] for t in tags):
            # Also include releases that have tender data even without explicit tag
            if not tender_obj.get("title"):
                continue

        # Check if tender is still open
        tender_period = tender_obj.get("tenderPeriod", {})
        end_date_str = tender_period.get("endDate")

        if end_date_str:
            try:
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                if end_date < now:
                    continue  # Already closed
            except (ValueError, TypeError):
                pass

        # Build a unique ID from OCDS data
        ocid = release.get("ocid", "")
        tender_id = tender_obj.get("id", "")
        seao_id = ocid or tender_id or ""

        if not seao_id:
            continue

        # Deduplicate within this file
        if seao_id in seen_ids:
            continue
        seen_ids.add(seao_id)

        # Extract buyer/department
        buyer = release.get("buyer", {})
        buyer_name = buyer.get("name", "")

        # If no buyer, check parties for the procuringEntity
        if not buyer_name:
            procuring = tender_obj.get("procuringEntity", {})
            buyer_name = procuring.get("name", "")

        if not buyer_name:
            for party in release.get("parties", []):
                roles = party.get("roles", [])
                if "buyer" in roles or "procuringEntity" in roles:
                    buyer_name = party.get("name", "")
                    break

        # Category mapping
        main_cat = (tender_obj.get("mainProcurementCategory") or "").lower()
        category = CATEGORY_MAP.get(main_cat, "SRV")

        # Procurement method
        proc_method_raw = tender_obj.get("procurementMethod", "")
        proc_method_map = {
            "open": "Competitive - Open bidding",
            "selective": "Competitive - Selective tendering",
            "limited": "Non-competitive",
            "direct": "Non-competitive",
        }
        procurement_method = proc_method_map.get(proc_method_raw.lower(), proc_method_raw)

        # Description
        description = tender_obj.get("description", "")

        # Title
        title = tender_obj.get("title", "")
        if not title:
            continue

        # Notice URL
        notice_url = ""
        for doc in tender_obj.get("documents", []):
            doc_url = doc.get("url", "")
            if "seao.ca" in doc_url:
                notice_url = doc_url
                break

        if not notice_url and seao_id:
            # Construct SEAO URL from ID
            clean_id = seao_id.replace("ocds-", "").split("-")[-1] if "-" in seao_id else seao_id
            notice_url = f"https://seao.ca/OpportunityPublication/ConsulterAvis/{clean_id}"

        # Publication date
        pub_date = tender_period.get("startDate") or release.get("date", "")

        # Closing date
        closing_date = end_date_str or ""

        # Estimated value
        value_obj = tender_obj.get("value", {})
        estimated_value = None
        if value_obj and value_obj.get("amount"):
            try:
                amt = float(value_obj["amount"])
                currency = value_obj.get("currency", "CAD")
                estimated_value = f"${amt:,.0f} {currency}"
            except (ValueError, TypeError):
                pass

        # Region — extract from buyer address or delivery locations
        region = "Quebec"
        for party in release.get("parties", []):
            roles = party.get("roles", [])
            if "buyer" in roles or "procuringEntity" in roles:
                addr = party.get("address", {})
                locality = addr.get("locality", "")
                if locality:
                    region = f"{locality}, Quebec"
                    break

        # Contact info
        contact_name = ""
        contact_email = ""
        contact_point = tender_obj.get("contactPoint", {})
        if contact_point:
            contact_name = contact_point.get("name", "")
            contact_email = contact_point.get("email", "")

        # Selection criteria
        selection = ""
        award_criteria = tender_obj.get("awardCriteria", "")
        if award_criteria:
            criteria_map = {
                "priceOnly": "Lowest Price",
                "ratedCriteria": "Highest Combined Rating of Technical Merit and Price",
                "qualityOnly": "Technical Merit Only",
            }
            selection = criteria_map.get(award_criteria, award_criteria)

        # Notice type
        notice_type_raw = tender_obj.get("procurementMethodDetails", "") or ""
        notice_type = "Request for Proposal"  # default
        if "invitation" in notice_type_raw.lower():
            notice_type = "Invitation to Tender"
        elif "qualification" in notice_type_raw.lower():
            notice_type = "Request for Qualification"
        elif "information" in notice_type_raw.lower():
            notice_type = "Request for Information"

        # Build the normalized tender record
        tender_record = {
            "solicitation_number": seao_id,
            "title": title[:500],
            "description": description[:5000] if description else None,
            "department": buyer_name[:300] if buyer_name else "Quebec Public Body",
            "category": category,
            "region": region,
            "notice_type": notice_type,
            "procurement_method": procurement_method,
            "selection_criteria": selection or None,
            "closing_date": closing_date or None,
            "publication_date": pub_date or None,
            "notice_url": notice_url or None,
            "contact_name": contact_name or None,
            "contact_email": contact_email or None,
            "estimated_value_text": estimated_value,
            "source_level": "provincial",
            "source_province": "QC",
            "source_system": "seao",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        tenders.append(tender_record)

    return tenders


def extract_documents(ocds_data: dict, tender_sol_map: dict) -> list[dict]:
    """
    Extract document/attachment URLs from OCDS releases.
    Returns list of document dicts ready for tender_documents table.

    tender_sol_map: { solicitation_number: tender_id } — used to link docs to DB tender IDs
    """
    releases = ocds_data.get("releases", [])
    documents = []

    for release in releases:
        tender_obj = release.get("tender", {})
        ocid = release.get("ocid", "")
        tender_id_field = tender_obj.get("id", "")
        seao_id = ocid or tender_id_field

        if not seao_id or seao_id not in tender_sol_map:
            continue

        db_tender_id = tender_sol_map[seao_id]

        for doc in tender_obj.get("documents", []):
            doc_url = doc.get("url", "")
            if not doc_url:
                continue

            doc_title = doc.get("title", "") or doc.get("description", "") or "Document"
            doc_format = (doc.get("format") or "").lower()

            # Map OCDS format to our file_type
            format_map = {
                "application/pdf": "pdf",
                "application/msword": "doc",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
                "application/vnd.ms-excel": "xls",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
                "application/zip": "zip",
                "text/plain": "txt",
            }
            file_type = format_map.get(doc_format, "unknown")

            # Infer from URL if format field is empty
            if file_type == "unknown":
                url_lower = doc_url.lower()
                for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip"]:
                    if url_lower.endswith(ext):
                        file_type = ext.lstrip(".")
                        break

            requires_login = "seao.ca" in doc_url and "/Document/" in doc_url

            documents.append({
                "tender_id": db_tender_id,
                "document_url": doc_url,
                "document_name": doc_title[:500],
                "file_type": file_type,
                "source": "seao",
                "requires_login": requires_login,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })

    return documents


def upsert_tenders(rows: list[dict]) -> int:
    """Upsert tenders into the tenders table using solicitation_number + source_system."""
    if not rows:
        return 0
    try:
        supabase.table("tenders").upsert(
            rows,
            on_conflict="solicitation_number,source_system"
        ).execute()
        return len(rows)
    except Exception as e:
        log.error(f"Supabase tender upsert error: {e}")
        # Try one at a time to identify bad rows
        success = 0
        for row in rows:
            try:
                supabase.table("tenders").upsert(
                    [row],
                    on_conflict="solicitation_number,source_system"
                ).execute()
                success += 1
            except Exception as e2:
                log.warning(f"  Skipping tender {row.get('solicitation_number', '?')}: {e2}")
        return success


def upsert_documents(docs: list[dict]) -> int:
    """Upsert documents into tender_documents table."""
    if not docs:
        return 0

    # Deduplicate by (tender_id, document_url)
    seen = set()
    deduped = []
    for d in docs:
        key = (d["tender_id"], d["document_url"])
        if key not in seen:
            seen.add(key)
            deduped.append(d)

    try:
        supabase.table("tender_documents").upsert(
            deduped,
            on_conflict="tender_id,document_url"
        ).execute()
        return len(deduped)
    except Exception as e:
        log.error(f"Supabase document upsert error: {e}")
        return 0


def run_fetch(weeks: int = 4, dry_run: bool = False) -> dict:
    """
    Main entry point: fetch SEAO data and upsert to database.

    Args:
        weeks: Number of weekly files to process (4 = ~1 month of coverage)
        dry_run: If True, print results without writing to DB
    """
    log.info(f"=== Quebec SEAO fetch started {datetime.now(timezone.utc).isoformat()} ===")
    log.info(f"  Weeks to fetch: {weeks}, Dry run: {dry_run}")

    # Step 1: Get resource list from CKAN API
    resources = fetch_resource_list()
    if not resources:
        log.error("No resources found from CKAN API")
        return {"fetched": 0, "tenders": 0, "documents": 0, "error": "CKAN API returned no resources"}

    # Step 2: Find the latest weekly JSON files
    weekly_files = find_weekly_files(resources, weeks=weeks)
    if not weekly_files:
        log.error("No weekly JSON files found")
        return {"fetched": 0, "tenders": 0, "documents": 0, "error": "No weekly files found"}

    # Step 3: Download and process each file
    all_tenders = {}  # keyed by solicitation_number to deduplicate across weeks
    all_ocds = []     # store raw OCDS data for document extraction

    for resource in weekly_files:
        url = resource.get("url", "")
        name = resource.get("name", "?")
        log.info(f"\n  Processing: {name}")

        ocds_data = download_json(url)
        if not ocds_data:
            continue

        release_count = len(ocds_data.get("releases", []))
        log.info(f"    Releases in file: {release_count}")

        tenders = extract_tenders(ocds_data)
        log.info(f"    Active tenders extracted: {len(tenders)}")

        # Keep latest version of each tender (newest file wins)
        for t in tenders:
            sol_num = t["solicitation_number"]
            if sol_num not in all_tenders:
                all_tenders[sol_num] = t

        all_ocds.append(ocds_data)

    tender_list = list(all_tenders.values())
    log.info(f"\n  Total unique active tenders: {len(tender_list)}")

    if dry_run:
        log.info("DRY RUN — first 5 tenders:")
        for t in tender_list[:5]:
            log.info(f"    {t['solicitation_number']}: {t['title'][:80]}")
            log.info(f"      Dept: {t['department']}, Category: {t['category']}, Closes: {t['closing_date']}")
        return {"fetched": len(weekly_files), "tenders": len(tender_list), "documents": 0, "dry_run": True}

    # Step 4: Upsert tenders
    total_upserted = 0
    for i in range(0, len(tender_list), 100):
        batch = tender_list[i:i + 100]
        count = upsert_tenders(batch)
        total_upserted += count
        log.info(f"  Batch {i // 100 + 1}: {count} tenders upserted")

    # Step 5: Build solicitation_number → tender_id map for document linking
    sol_nums = [t["solicitation_number"] for t in tender_list]
    tender_sol_map = {}

    # Query in chunks of 50
    for i in range(0, len(sol_nums), 50):
        chunk = sol_nums[i:i + 50]
        try:
            result = supabase.table("tenders") \
                .select("id, solicitation_number") \
                .eq("source_system", "seao") \
                .in_("solicitation_number", chunk) \
                .execute()
            for row in (result.data or []):
                tender_sol_map[row["solicitation_number"]] = row["id"]
        except Exception as e:
            log.warning(f"  Could not look up tender IDs: {e}")

    # Step 6: Extract and upsert documents
    total_docs = 0
    for ocds_data in all_ocds:
        docs = extract_documents(ocds_data, tender_sol_map)
        if docs:
            count = upsert_documents(docs)
            total_docs += count

    log.info(f"\n=== Done. {total_upserted} tenders, {total_docs} documents ===")
    return {
        "fetched": len(weekly_files),
        "tenders": total_upserted,
        "documents": total_docs,
        "source": "seao",
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch Quebec SEAO tender data")
    parser.add_argument("--weeks", type=int, default=4, help="Number of weekly files to process")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    args = parser.parse_args()
    run_fetch(weeks=args.weeks, dry_run=args.dry_run)
