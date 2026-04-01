"""
fetch_canadabuys.py
-------------------
Fetches live tender data directly from CanadaBuys free open data CSV.
Also ingests document attachments directly from the CSV — no page scraping needed.

Sources:
  Open tenders (all active):  https://canadabuys.canada.ca/opendata/pub/openTenderNotice-ouvertAvisAppelOffres.csv
  New tenders (today only):   https://canadabuys.canada.ca/opendata/pub/newTenderNotice-nouvelAvisAppelOffres.csv

Usage:
  python fetch_canadabuys.py              # reload all open tenders + their documents
  python fetch_canadabuys.py --new-only   # only today's new tenders
  python fetch_canadabuys.py --dry-run    # print first 5 rows, no DB writes
"""

import os
import io
import csv
import logging
import requests
from supabase import create_client, Client
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

OPEN_TENDERS_URL = "https://canadabuys.canada.ca/opendata/pub/openTenderNotice-ouvertAvisAppelOffres.csv"
NEW_TENDERS_URL  = "https://canadabuys.canada.ca/opendata/pub/newTenderNotice-nouvelAvisAppelOffres.csv"
NOTICE_BASE_URL  = "https://canadabuys.canada.ca/en/tender-opportunities/tender-notice/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/csv,text/plain,*/*",
}

# ── Correct column map from actual CSV headers ───────────────────────────────
COLUMN_MAP = {
    "title-titre-eng":                                          "title",
    "referenceNumber-numeroReference":                          "record_id",
    "solicitationNumber-numeroSollicitation":                   "solicitation_number",
    "publicationDate-datePublication":                          "publication_date",
    "tenderClosingDate-appelOffresDateCloture":                 "closing_date",
    "tenderStatus-appelOffresStatut-eng":                       "status",
    "gsin-nibs":                                                "gsin_code",
    "procurementCategory-categorieApprovisionnement":           "category",
    "noticeType-avisType-eng":                                  "notice_type",
    "procurementMethod-methodeApprovisionnement-eng":           "procurement_method",
    "selectionCriteria-criteresSelection-eng":                  "selection_criteria",
    "tradeAgreements-accordsCommerciaux-eng":                   "trade_agreement",
    "regionsOfOpportunity-regionAppelOffres-eng":               "region",
    "contractingEntityName-nomEntitContractante-eng":           "department",
    "contactInfoName-informationsContactNom":                   "contact_name",
    "contactInfoEmail-informationsContactCourriel":             "contact_email",
    "noticeURL-URLavis-eng":                                    "notice_url",
    "tenderDescription-descriptionAppelOffres-eng":             "description",
}

# Attachment column — comma-separated direct PDF URLs
ATTACHMENT_COL = "attachment-piecesJointes-eng"


def fetch_csv(url: str) -> list[dict]:
    """Download and parse a CanadaBuys CSV."""
    log.info(f"Fetching {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        content = resp.content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        log.info(f"  -> {len(rows)} rows fetched")
        return rows
    except requests.RequestException as e:
        log.error(f"Failed to fetch {url}: {e}")
        return []


def normalize_row(raw: dict) -> dict | None:
    """Map a CSV row to our tenders table schema."""
    mapped = {}
    for csv_col, db_col in COLUMN_MAP.items():
        val = raw.get(csv_col, "").strip() or None
        mapped[db_col] = val

    record_id = mapped.get("record_id")
    sol_num   = mapped.get("solicitation_number")

    if not record_id and not sol_num:
        return None

    # If noticeURL is empty in CSV, construct from record_id
    if not mapped.get("notice_url") and record_id:
        mapped["notice_url"] = NOTICE_BASE_URL + record_id

    # Use record_id as solicitation_number fallback
    if not sol_num:
        mapped["solicitation_number"] = record_id

    mapped["updated_at"] = datetime.now(timezone.utc).isoformat()
    return mapped


def parse_attachments(raw: dict, sol_num: str) -> list[dict]:
    """
    Parse the attachment column into tender_documents rows.
    The field is a comma-separated list of direct PDF/doc URLs.
    """
    attachment_str = raw.get(ATTACHMENT_COL, "").strip()
    if not attachment_str:
        return []

    urls = [u.strip() for u in attachment_str.split(",") if u.strip()]
    docs = []
    for url in urls:
        filename = url.split("/")[-1] or "Document"
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "unknown"
        # Clean up double extensions like .pdf.pdf
        if ext == "pdf" and filename.endswith(".pdf.pdf"):
            filename = filename[:-4]

        docs.append({
            "document_url":   url,
            "document_name":  filename[:500],
            "file_type":      ext,
            "source":         "canadabuys",
            "requires_login": False,
            "scraped_at":     datetime.now(timezone.utc).isoformat(),
            "_sol_num":       sol_num,  # temporary key, stripped before upsert
        })
    return docs


def upsert_tenders(rows: list[dict]) -> int:
    """Upsert batch into tenders table."""
    if not rows:
        return 0
    try:
        supabase.table("tenders").upsert(
            rows,
            on_conflict="solicitation_number"
        ).execute()
        return len(rows)
    except Exception as e:
        log.error(f"Supabase upsert error: {e}")
        return 0


def upsert_documents_for_batch(docs_by_sol: dict) -> int:
    """
    Look up tender IDs by solicitation_number then upsert documents.
    docs_by_sol: { solicitation_number: [doc, ...] }
    """
    if not docs_by_sol:
        return 0

    sol_nums = list(docs_by_sol.keys())
    try:
        result = supabase.table("tenders") \
            .select("id, solicitation_number") \
            .in_("solicitation_number", sol_nums) \
            .execute()
    except Exception as e:
        log.error(f"Could not look up tender IDs: {e}")
        return 0

    sol_to_id = {r["solicitation_number"]: r["id"] for r in (result.data or [])}
    all_docs  = []
    doc_counts = {}

    for sol_num, docs in docs_by_sol.items():
        tender_id = sol_to_id.get(sol_num)
        if not tender_id:
            continue
        doc_counts[tender_id] = len(docs)
        for doc in docs:
            d = {k: v for k, v in doc.items() if not k.startswith("_")}
            d["tender_id"] = tender_id
            all_docs.append(d)

    if not all_docs:
        return 0

    # Deduplicate by (tender_id, document_url) within this batch
    seen = set()
    deduped_docs = []
    for doc in all_docs:
        key = (doc["tender_id"], doc["document_url"])
        if key not in seen:
            seen.add(key)
            deduped_docs.append(doc)

    try:
        supabase.table("tender_documents").upsert(
            deduped_docs,
            on_conflict="tender_id,document_url"
        ).execute()

        # Update doc_count + docs_scraped_at on each tender
        for tender_id, count in doc_counts.items():
            supabase.table("tenders").update({
                "doc_count":       count,
                "docs_scraped_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", tender_id).execute()

        log.info(f"  Upserted {len(deduped_docs)} documents across {len(doc_counts)} tenders")
        return len(all_docs)
    except Exception as e:
        log.error(f"Document upsert error: {e}")
        return 0


def run_fetch(new_only: bool = False, dry_run: bool = False) -> dict:
    """Main entry point."""
    url   = NEW_TENDERS_URL if new_only else OPEN_TENDERS_URL
    label = "new" if new_only else "open"
    log.info(f"=== CanadaBuys fetch ({label}) started {datetime.now(timezone.utc).isoformat()} ===")

    raw_rows = fetch_csv(url)
    if not raw_rows:
        return {"fetched": 0, "upserted": 0, "documents": 0}

    if dry_run:
        log.info("DRY RUN — first 5 normalized rows:")
        for r in raw_rows[:5]:
            norm = normalize_row(r)
            log.info(f"  {norm}")
            if norm:
                docs = parse_attachments(r, norm.get("solicitation_number", "?"))
                if docs:
                    log.info(f"    -> {len(docs)} attachment(s): {[d['document_name'] for d in docs]}")
        return {"fetched": len(raw_rows), "upserted": 0, "documents": 0}

    normalized  = []
    docs_by_sol = {}
    skipped     = 0
    seen_keys   = set()

    for row in raw_rows:
        norm = normalize_row(row)
        if not norm:
            skipped += 1
            continue
        key = norm["solicitation_number"]
        if key in seen_keys:
            skipped += 1
            continue
        seen_keys.add(key)
        normalized.append(norm)

        docs = parse_attachments(row, key)
        if docs:
            docs_by_sol[key] = docs

    # Upsert tenders in batches of 200
    total_upserted = 0
    for i in range(0, len(normalized), 200):
        batch = normalized[i:i + 200]
        count = upsert_tenders(batch)
        total_upserted += count
        log.info(f"  Batch {i // 200 + 1}: {count} tenders upserted")

    # Upsert documents after tenders are in DB
    total_docs = upsert_documents_for_batch(docs_by_sol)

    log.info(f"=== Done. {total_upserted} tenders, {total_docs} documents, {skipped} skipped ===")
    return {
        "fetched":   len(raw_rows),
        "upserted":  total_upserted,
        "documents": total_docs,
        "skipped":   skipped,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch CanadaBuys open tender data")
    parser.add_argument("--new-only", action="store_true", help="Only fetch today's new tenders")
    parser.add_argument("--dry-run",  action="store_true", help="Print rows without writing to DB")
    args = parser.parse_args()
    run_fetch(new_only=args.new_only, dry_run=args.dry_run)
