"""
ADD THESE ROUTES TO app.py
===========================
Paste these route definitions into your existing Flask app.py.
Imports needed (add to top of app.py if not already present):
    from scrape_documents import scrape_tender_documents, upsert_documents, mark_tender_scraped
"""

# ── GET /api/tenders/<tender_id>/documents ──────────────────────────────────
# Returns all scraped documents for a tender.
# Frontend calls this to render the Documents section on the tender detail page.

@app.route("/api/tenders/<tender_id>/documents", methods=["GET"])
def get_tender_documents(tender_id):
    try:
        result = supabase.table("tender_documents") \
            .select("id, document_url, document_name, file_type, source, requires_login, scraped_at") \
            .eq("tender_id", tender_id) \
            .order("requires_login", desc=False)  # direct downloads first
            .execute()

        return jsonify({
            "tender_id": tender_id,
            "documents": result.data or [],
            "count": len(result.data or []),
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── POST /api/tenders/<tender_id>/scrape-documents ──────────────────────────
# Triggers an on-demand scrape for a single tender.
# Call this from the frontend when a user opens a tender detail with no docs yet.
# Auth: require a valid JWT (user must be logged in).

@app.route("/api/tenders/<tender_id>/scrape-documents", methods=["POST"])
def trigger_document_scrape(tender_id):
    # Verify auth header (your existing pattern)
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 401

    try:
        # Get the tender's notice_url
        tender = supabase.table("tenders") \
            .select("id, notice_url, docs_scraped_at") \
            .eq("id", tender_id) \
            .single() \
            .execute()

        if not tender.data:
            return jsonify({"error": "Tender not found"}), 404

        notice_url = tender.data.get("notice_url")
        if not notice_url:
            return jsonify({"error": "No notice_url for this tender"}), 400

        # Run the scrape
        docs = scrape_tender_documents(tender_id, notice_url)
        upsert_documents(docs)
        mark_tender_scraped(tender_id, len(docs))

        return jsonify({
            "tender_id": tender_id,
            "documents_found": len(docs),
            "documents": docs,
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── POST /api/scrape-documents/batch ────────────────────────────────────────
# Batch scrape endpoint — called by your cron job alongside the matcher.
# Scrapes up to `limit` unscraped tenders per run.

@app.route("/api/scrape-documents/batch", methods=["POST"])
def batch_scrape_documents():
    # Protect with your cron secret (same pattern as /api/run-matcher)
    secret = request.headers.get("X-Cron-Secret", "")
    if secret != os.environ.get("CRON_SECRET", ""):
        return jsonify({"error": "Forbidden"}), 403

    from scrape_documents import run_full_scan
    limit = request.json.get("limit", 100) if request.is_json else 100

    try:
        run_full_scan(limit=limit, only_unscraped=True)
        return jsonify({"status": "ok", "message": f"Batch scrape triggered (limit={limit})"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
