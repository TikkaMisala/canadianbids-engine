"""
app.py — CanadianBids.ai Railway backend

Endpoints:
  GET  /             → health check
  POST /api/match    → run matching job for all users
  POST /api/summarize → generate AI summaries for unsummarized tenders
  POST /api/run-all  → run both matching + summarizing (used by daily cron)

Environment variables required:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  ANTHROPIC_API_KEY
  CRON_SECRET  (a secret string to protect the cron endpoints)
"""

import os
import json
import stripe
from flask import Flask, jsonify, request
from supabase import create_client
from dotenv import load_dotenv
from matcher import run_matching, run_matching_single
from summarizer import run_summarizer
from extractor import run_extractor
from datetime import datetime, timezone
from scrape_documents import scrape_tender_documents, upsert_documents, mark_tender_scraped, run_full_scan
from fetch_canadabuys import run_fetch
from fetch_quebec_seao import run_fetch as run_fetch_quebec

load_dotenv()

app = Flask(__name__)

@app.after_request
def add_cors_headers(response):
    origin = request.headers.get('Origin', '')
    allowed = ['https://canadianbidsai.ca', 'http://localhost:8788']
    if origin in allowed:
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return response

# ── Config ──
SUPABASE_URL        = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
ANTHROPIC_API_KEY   = os.environ.get("ANTHROPIC_API_KEY")
CRON_SECRET         = os.environ.get("CRON_SECRET", "change-me-in-railway")
STRIPE_SECRET_KEY   = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
FRONTEND_URL        = os.environ.get("FRONTEND_URL", "https://canadianbidsai.ca")

stripe.api_key = STRIPE_SECRET_KEY

db = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def check_secret():
    """Validate the CRON_SECRET header on protected endpoints."""
    secret = request.headers.get("X-Cron-Secret") or request.args.get("secret")
    if secret != CRON_SECRET:
        return False
    return True


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "CanadianBids.ai matching engine",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/debug", methods=["GET"])
def debug():
    """Debug endpoint to check DB connectivity and data."""
    if not check_secret():
        return jsonify({"error": "Unauthorized"}), 401
    try:
        # Test profiles
        profiles = db.table("profiles").select("id, company_name, onboarding_complete, keywords").execute()
        # Test tenders count
        tenders = db.table("tenders").select("id", count="exact").execute()
        return jsonify({
            "status": "ok",
            "profiles": profiles.data,
            "profile_count": len(profiles.data or []),
            "tender_count": tenders.count,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/api/match", methods=["GET", "POST"])
def match():
    """Run the matching job for all users."""
    if not check_secret():
        return jsonify({"error": "Unauthorized"}), 401

    try:
        result = run_matching(db, anthropic_key=ANTHROPIC_API_KEY)
        return jsonify({"status": "ok", **result})
    except Exception as e:
        print(f"Match job error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/summarize", methods=["GET", "POST"])
def summarize():
    """Generate AI summaries for tenders that don't have one yet."""
    if not check_secret():
        return jsonify({"error": "Unauthorized"}), 401

    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "ANTHROPIC_API_KEY not set"}), 500

    batch_size = int(request.args.get("batch", 50))

    try:
        result = run_summarizer(db, ANTHROPIC_API_KEY, batch_size=batch_size)
        return jsonify({"status": "ok", **result})
    except Exception as e:
        print(f"Summarize job error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/extract", methods=["GET", "POST"])
def extract():
    """Extract structured fields from tender descriptions using Claude."""
    if not check_secret():
        return jsonify({"error": "Unauthorized"}), 401
    batch_size = int(request.args.get("batch", 25))
    try:
        result = run_extractor(batch_size=batch_size)
        return jsonify({"status": "ok", **result})
    except Exception as e:
        print(f"Extractor error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/run-all", methods=["GET", "POST"])
def run_all():
    """Run both matching and summarizing — called by daily cron.
    Loops through ALL unsummarized tenders in batches of 25."""
    if not check_secret():
        return jsonify({"error": "Unauthorized"}), 401

    results = {}

    # 1. Generate summaries — loop until all tenders are done
    if ANTHROPIC_API_KEY:
        total_summarized = 0
        total_errors = 0
        try:
            for batch_num in range(20):  # Max 20 loops = 500 tenders
                batch_result = run_summarizer(db, ANTHROPIC_API_KEY, batch_size=25)
                total_summarized += batch_result.get("summarized", 0)
                total_errors += batch_result.get("errors", 0)
                if batch_result.get("summarized", 0) == 0:
                    break  # All done
            results["summarize"] = {
                "summarized": total_summarized,
                "errors": total_errors,
                "batches_run": batch_num + 1,
            }
        except Exception as e:
            results["summarize"] = {"error": str(e), "summarized_before_error": total_summarized}
    else:
        results["summarize"] = {"skipped": "No ANTHROPIC_API_KEY set"}

    # 2. Run matching for all users (with AI scoring)
    try:
        results["match"] = run_matching(db, anthropic_key=ANTHROPIC_API_KEY)
    except Exception as e:
        results["match"] = {"error": str(e)}

    # 3. Extract structured fields from new tenders
    try:
        results["extract"] = run_extractor(batch_size=25)
    except Exception as e:
        results["extract"] = {"error": str(e)}

    return jsonify({"status": "ok", **results})


@app.route("/api/match-user/<user_id>", methods=["POST"])
def match_user(user_id):
    """
    Run matching for a single user — called when a user completes onboarding.
    Uses full 3-stage pipeline for instant results.
    """
    if not check_secret():
        return jsonify({"error": "Unauthorized"}), 401

    try:
        result = run_matching_single(db, user_id, anthropic_key=ANTHROPIC_API_KEY)
        if "error" in result:
            return jsonify(result), 404
        return jsonify(result)
    except Exception as e:
        print(f"Single user match error: {e}")
        return jsonify({"error": str(e)}), 500




# ═══════════════════════════════════════════════════════════
# STRIPE ENDPOINTS
# ═══════════════════════════════════════════════════════════

@app.route("/api/create-checkout", methods=["POST"])
def create_checkout():
    """Create a Stripe Checkout session for Pro subscription."""
    data = request.get_json() or {}
    user_id = data.get("user_id")
    email = data.get("email")
    plan = data.get("plan", "monthly")  # 'monthly' or 'annual'

    if not user_id or not email:
        return jsonify({"error": "user_id and email required"}), 400

    if not STRIPE_SECRET_KEY:
        return jsonify({"error": "Stripe not configured"}), 500

    # Price IDs — set these in Railway env vars
    MONTHLY_PRICE_ID = os.environ.get("STRIPE_MONTHLY_PRICE_ID")
    ANNUAL_PRICE_ID = os.environ.get("STRIPE_ANNUAL_PRICE_ID")

    price_id = ANNUAL_PRICE_ID if plan == "annual" else MONTHLY_PRICE_ID
    if not price_id:
        return jsonify({"error": f"Price ID for '{plan}' not configured"}), 500

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            customer_email=email,
            metadata={"user_id": user_id},
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=f"{FRONTEND_URL}?checkout=success&session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{FRONTEND_URL}?checkout=cancelled",
            allow_promotion_codes=True,
        )
        return jsonify({"url": session.url, "session_id": session.id})
    except Exception as e:
        print(f"Stripe checkout error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/stripe-webhook", methods=["POST"])
def stripe_webhook():
    """Handle Stripe webhook events (subscription created, cancelled, etc.)."""
    payload = request.get_data(as_text=True)
    sig = request.headers.get("Stripe-Signature")

    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
    except (stripe.error.SignatureVerificationError, ValueError) as e:
        print(f"Webhook signature error: {e}")
        return jsonify({"error": "Invalid signature"}), 400

    event_type = event.get("type", "")
    data = event.get("data", {}).get("object", {})
    print(f"Stripe webhook: {event_type}")

    if event_type == "checkout.session.completed":
        user_id = data.get("metadata", {}).get("user_id")
        customer_id = data.get("customer")
        subscription_id = data.get("subscription")

        if user_id:
            try:
                db.table("subscriptions").upsert({
                    "user_id": user_id,
                    "plan": "pro",
                    "status": "active",
                    "stripe_customer_id": customer_id,
                    "stripe_subscription_id": subscription_id,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                }).execute()
                print(f"  ✓ Activated Pro for user {user_id[:8]}...")
            except Exception as e:
                print(f"  DB error activating subscription: {e}")

    elif event_type in ("customer.subscription.deleted", "customer.subscription.updated"):
        subscription_id = data.get("id")
        status = data.get("status")

        if subscription_id:
            try:
                # Find subscription by stripe_subscription_id
                resp = db.table("subscriptions") \
                    .select("user_id") \
                    .eq("stripe_subscription_id", subscription_id) \
                    .execute()
                if resp.data:
                    user_id = resp.data[0]["user_id"]
                    new_status = "active" if status == "active" else "cancelled"
                    new_plan = "pro" if status == "active" else "free"
                    db.table("subscriptions").update({
                        "status": new_status,
                        "plan": new_plan,
                    }).eq("user_id", user_id).execute()
                    print(f"  ✓ Updated subscription for {user_id[:8]}...: {new_status}")
            except Exception as e:
                print(f"  DB error updating subscription: {e}")

    return jsonify({"received": True})


# ═══════════════════════════════════════════════════════════
# CANADABUYS TENDER FETCH
# ═══════════════════════════════════════════════════════════

@app.route("/api/fetch-tenders", methods=["POST", "OPTIONS"])
def fetch_tenders():
    """Fetch latest tenders from CanadaBuys open data CSV. Called by cron."""
    if request.method == "OPTIONS":
        return jsonify({}), 200
    if not check_secret():
        return jsonify({"error": "Forbidden"}), 403
    new_only = request.json.get("new_only", False) if request.is_json else False
    try:
        result = run_fetch(new_only=new_only)
        return jsonify({"status": "ok", **result}), 200
    except Exception as e:
        print(f"fetch_tenders error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/fetch-quebec", methods=["POST", "OPTIONS"])
def fetch_quebec():
    """Fetch Quebec provincial tenders from SEAO open data. Called by cron."""
    if request.method == "OPTIONS":
        return jsonify({}), 200
    if not check_secret():
        return jsonify({"error": "Forbidden"}), 403
    weeks = request.json.get("weeks", 4) if request.is_json else 4
    try:
        result = run_fetch_quebec(weeks=weeks)
        return jsonify({"status": "ok", **result}), 200
    except Exception as e:
        print(f"fetch_quebec error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/fetch-all-sources", methods=["POST", "OPTIONS"])
def fetch_all_sources():
    """Fetch tenders from ALL sources — federal + provincial. Called by daily cron."""
    if request.method == "OPTIONS":
        return jsonify({}), 200
    if not check_secret():
        return jsonify({"error": "Forbidden"}), 403

    results = {}

    # Federal — CanadaBuys
    try:
        results["federal"] = run_fetch(new_only=False)
    except Exception as e:
        results["federal"] = {"error": str(e)}
        print(f"fetch federal error: {e}")

    # Quebec — SEAO
    try:
        results["quebec"] = run_fetch_quebec(weeks=2)
    except Exception as e:
        results["quebec"] = {"error": str(e)}
        print(f"fetch quebec error: {e}")

    return jsonify({"status": "ok", "sources": results}), 200


# ═══════════════════════════════════════════════════════════
# DOCUMENT SCRAPER ENDPOINTS
# ═══════════════════════════════════════════════════════════

@app.route("/api/tenders/<int:tender_id>/documents", methods=["GET", "OPTIONS"])
def get_tender_documents(tender_id):
    """Return all scraped documents for a tender."""
    if request.method == "OPTIONS":
        return jsonify({}), 200
    try:
        result = db.table("tender_documents") \
            .select("id, document_url, document_name, file_type, source, requires_login, scraped_at") \
            .eq("tender_id", tender_id) \
            .order("requires_login") \
            .execute()
        return jsonify({
            "tender_id": tender_id,
            "documents": result.data or [],
            "count": len(result.data or []),
        }), 200
    except Exception as e:
        print(f"get_tender_documents error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/tenders/<int:tender_id>/scrape-documents", methods=["POST", "OPTIONS"])
def trigger_document_scrape(tender_id):
    """On-demand scrape for a single tender — called when user opens a tender with no docs yet."""
    if request.method == "OPTIONS":
        return jsonify({}), 200
    try:
        tender = db.table("tenders") \
            .select("id, notice_url, docs_scraped_at") \
            .eq("id", tender_id) \
            .single() \
            .execute()

        if not tender.data:
            return jsonify({"error": "Tender not found"}), 404

        notice_url = tender.data.get("notice_url")
        if not notice_url:
            return jsonify({"error": "No notice_url for this tender"}), 400

        docs = scrape_tender_documents(tender_id, notice_url)
        upsert_documents(docs)
        mark_tender_scraped(tender_id, len(docs))

        return jsonify({
            "tender_id": tender_id,
            "documents_found": len(docs),
            "documents": docs,
        }), 200
    except Exception as e:
        print(f"trigger_document_scrape error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/scrape-documents/batch", methods=["POST", "OPTIONS"])
def batch_scrape_documents():
    """Batch scrape — called by cron job to scrape up to N unscraped tenders."""
    if request.method == "OPTIONS":
        return jsonify({}), 200
    if not check_secret():
        return jsonify({"error": "Forbidden"}), 403
    limit = request.json.get("limit", 100) if request.is_json else 100
    try:
        run_full_scan(limit=limit, only_unscraped=True)
        return jsonify({"status": "ok", "message": f"Batch scrape triggered (limit={limit})"}), 200
    except Exception as e:
        print(f"batch_scrape_documents error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
