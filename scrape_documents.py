"""
scrape_documents.py
-------------------
Scrapes document/attachment URLs from CanadaBuys tender notice pages.
Stores results in Supabase `tender_documents` table.

Run standalone:   python scrape_documents.py
Run via Flask:    imported and called from app.py endpoint

Two modes:
  - Full scan:    iterates all tenders with a notice_url (batch, run weekly)
  - Single:       scrape_tender_documents(tender_id, notice_url)
"""

import os
import time
import logging
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # service role key for writes
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CANADABUYS_BASE = "https://canadabuys.canada.ca"
SAP_DOMAIN = "businessnetwork.sap.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-CA,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# File extensions we treat as downloadable documents
DOC_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".ppt", ".pptx", ".zip", ".txt", ".rtf", ".odt",
}

# How long to wait between requests (be polite to CanadaBuys)
REQUEST_DELAY_SECONDS = 1.5


def classify_document_url(url: str) -> dict:
    """
    Returns metadata about a document URL:
      - source: 'canadabuys' | 'sap' | 'third_party'
      - requires_login: bool
      - file_type: pdf | docx | xlsx | ... | unknown
    """
    parsed = urlparse(url)
    hostname = parsed.netloc.lower()
    path = parsed.path.lower()

    source = "third_party"
    requires_login = False

    if "canadabuys.canada.ca" in hostname or "buyandsell.gc.ca" in hostname:
        source = "canadabuys"
    elif SAP_DOMAIN in hostname or "ariba.com" in hostname:
        source = "sap"
        requires_login = True

    # Infer file type from extension
    file_type = "unknown"
    for ext in DOC_EXTENSIONS:
        if path.endswith(ext):
            file_type = ext.lstrip(".")
            break

    return {
        "source": source,
        "requires_login": requires_login,
        "file_type": file_type,
    }


def scrape_tender_documents(tender_id: str, notice_url: str) -> list[dict]:
    """
    Scrape a single CanadaBuys tender notice page and return a list of
    document dicts ready to upsert into `tender_documents`.

    Returns [] on failure (logs the error).
    """
    if not notice_url:
        log.warning(f"[{tender_id}] No notice_url, skipping")
        return []

    try:
        resp = requests.get(notice_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error(f"[{tender_id}] Failed to fetch {notice_url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    documents = []
    seen_urls = set()

    # ── Strategy 1: Look for the "Documents" section / attachments section ──
    # CanadaBuys typically renders docs in a table or list with class hints.
    # Common patterns observed: <a href="..."> inside .field--name-field-attachments,
    # or inside a <section> labelled "Documents", or near text "documents below".

    # Cast a wide net: find all <a> tags with href pointing to downloadable files
    # OR to SAP/third-party systems
    all_links = soup.find_all("a", href=True)

    for tag in all_links:
        href = tag.get("href", "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue

        # Resolve relative URLs
        full_url = urljoin(CANADABUYS_BASE, href) if href.startswith("/") else href

        # Only collect if it looks like a document or a known procurement system
        parsed = urlparse(full_url)
        path_lower = parsed.path.lower()
        is_doc_file = any(path_lower.endswith(ext) for ext in DOC_EXTENSIONS)
        is_sap = SAP_DOMAIN in parsed.netloc or "ariba.com" in parsed.netloc
        is_procurement_system = any(
            domain in parsed.netloc
            for domain in ["canadabuys.canada.ca", "buyandsell.gc.ca"]
        ) and ("/document" in path_lower or "/attachment" in path_lower or "/file" in path_lower)

        if not (is_doc_file or is_sap or is_procurement_system):
            continue

        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        meta = classify_document_url(full_url)
        label = tag.get_text(strip=True) or tag.get("title", "") or "Document"

        documents.append({
            "tender_id": tender_id,
            "document_url": full_url,
            "document_name": label[:500],  # cap label length
            "file_type": meta["file_type"],
            "source": meta["source"],
            "requires_login": meta["requires_login"],
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    # ── Strategy 2: Look for SAP link even if it's not a file link ──
    # PSPC tenders redirect to SAP Business Network — capture that entry point.
    sap_links = [
        tag for tag in all_links
        if SAP_DOMAIN in tag.get("href", "") or "ariba.com" in tag.get("href", "")
    ]
    for tag in sap_links:
        full_url = tag["href"]
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)
        label = tag.get_text(strip=True) or "View in SAP Business Network"
        documents.append({
            "tender_id": tender_id,
            "document_url": full_url,
            "document_name": label[:500],
            "file_type": "unknown",
            "source": "sap",
            "requires_login": True,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    log.info(f"[{tender_id}] Found {len(documents)} document(s) at {notice_url}")
    return documents


def upsert_documents(documents: list[dict]) -> None:
    """Upsert documents into the tender_documents table."""
    if not documents:
        return
    try:
        supabase.table("tender_documents").upsert(
            documents,
            on_conflict="tender_id,document_url"
        ).execute()
        log.info(f"Upserted {len(documents)} document record(s)")
    except Exception as e:
        log.error(f"Supabase upsert error: {e}")


def mark_tender_scraped(tender_id: str, doc_count: int) -> None:
    """Update the tenders table to record that docs have been scraped."""
    try:
        supabase.table("tenders").update({
            "docs_scraped_at": datetime.now(timezone.utc).isoformat(),
            "doc_count": doc_count,
        }).eq("id", tender_id).execute()
    except Exception as e:
        log.warning(f"[{tender_id}] Could not update tenders.docs_scraped_at: {e}")


def run_full_scan(limit: int = 500, only_unscraped: bool = True) -> None:
    """
    Iterate all tenders with a notice_url and scrape their documents.

    Args:
        limit:          Max tenders to process per run (stay within API limits)
        only_unscraped: If True, skip tenders already scraped (docs_scraped_at IS NOT NULL)
    """
    log.info(f"Starting full document scan (limit={limit}, only_unscraped={only_unscraped})")

    query = supabase.table("tenders") \
        .select("id, solicitation_number, notice_url, docs_scraped_at") \
        .not_.is_("notice_url", "null") \
        .limit(limit)

    if only_unscraped:
        query = query.is_("docs_scraped_at", "null")

    result = query.execute()
    tenders = result.data or []
    log.info(f"Found {len(tenders)} tender(s) to scrape")

    total_docs = 0
    for i, tender in enumerate(tenders):
        tender_id = tender["id"]
        notice_url = tender.get("notice_url")

        docs = scrape_tender_documents(tender_id, notice_url)
        upsert_documents(docs)
        mark_tender_scraped(tender_id, len(docs))
        total_docs += len(docs)

        if i < len(tenders) - 1:
            time.sleep(REQUEST_DELAY_SECONDS)

    log.info(f"Done. Scraped {total_docs} document(s) across {len(tenders)} tender(s).")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape CanadaBuys tender documents")
    parser.add_argument("--tender-id", help="Scrape a single tender by ID")
    parser.add_argument("--notice-url", help="Notice URL for single tender scrape")
    parser.add_argument("--limit", type=int, default=200, help="Max tenders for full scan")
    parser.add_argument("--rescrape", action="store_true", help="Rescrape already-scraped tenders")
    args = parser.parse_args()

    if args.tender_id and args.notice_url:
        docs = scrape_tender_documents(args.tender_id, args.notice_url)
        upsert_documents(docs)
        mark_tender_scraped(args.tender_id, len(docs))
    else:
        run_full_scan(limit=args.limit, only_unscraped=not args.rescrape)
