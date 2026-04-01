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
from flask import Flask, jsonify, request
from supabase import create_client
from dotenv import load_dotenv
from matcher import run_matching, run_matching_single
from summarizer import run_summarizer
from datetime import datetime, timezone

load_dotenv()

app = Flask(__name__)

# ── Config ──
SUPABASE_URL        = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
ANTHROPIC_API_KEY   = os.environ.get("ANTHROPIC_API_KEY")
CRON_SECRET         = os.environ.get("CRON_SECRET", "change-me-in-railway")

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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
