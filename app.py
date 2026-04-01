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
from matcher import run_matching
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
        result = run_matching(db)
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
    """Run both matching and summarizing — called by Railway cron daily."""
    if not check_secret():
        return jsonify({"error": "Unauthorized"}), 401

    results = {}

    # 1. Generate summaries for new tenders
    if ANTHROPIC_API_KEY:
        try:
            results["summarize"] = run_summarizer(db, ANTHROPIC_API_KEY, batch_size=100)
        except Exception as e:
            results["summarize"] = {"error": str(e)}
    else:
        results["summarize"] = {"skipped": "No ANTHROPIC_API_KEY set"}

    # 2. Run matching for all users
    try:
        results["match"] = run_matching(db)
    except Exception as e:
        results["match"] = {"error": str(e)}

    return jsonify({"status": "ok", **results})


@app.route("/api/match-user/<user_id>", methods=["POST"])
def match_user(user_id):
    """
    Run matching for a single user — called when a user completes onboarding.
    Useful so new users see matches immediately without waiting for daily cron.
    """
    if not check_secret():
        return jsonify({"error": "Unauthorized"}), 401

    try:
        # Load just this user's profile
        profile_resp = db.table("profiles").select("*").eq("id", user_id).single().execute()
        profile = profile_resp.data
        if not profile:
            return jsonify({"error": "Profile not found"}), 404

        # Load all live tenders
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        tenders_resp = db.table("tenders").select("*").gt("closing_date", now).execute()
        tenders = tenders_resp.data or []

        # Load subscription
        sub_resp = db.table("subscriptions").select("plan, status").eq("user_id", user_id).execute()
        sub_data = sub_resp.data
        sub = sub_data[0] if sub_data else {}
        is_pro = sub.get("plan") == "pro" and sub.get("status") == "active"

        from matcher import score_tender
        scored = []
        for tender in tenders:
            score, keywords = score_tender(tender, profile)
            if score >= 20:
                scored.append({"score": score, "keywords": keywords, "tender": tender})

        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[:25]

        # Delete existing matches
        db.table("matches").delete().eq("user_id", user_id).execute()

        # Insert new
        if top:
            rows = [{
                "user_id":         user_id,
                "tender_id":       m["tender"]["id"],
                "score":           m["score"],
                "keyword_matches": m["keywords"],
                "is_locked":       False if is_pro else (rank > 0),
                "matched_at":      datetime.now(timezone.utc).isoformat(),
            } for rank, m in enumerate(top)]
            db.table("matches").insert(rows).execute()

        return jsonify({
            "status": "ok",
            "user_id": user_id,
            "matches": len(top),
            "top_score": top[0]["score"] if top else 0,
        })

    except Exception as e:
        print(f"Single user match error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
