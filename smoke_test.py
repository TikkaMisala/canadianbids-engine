"""
smoke_test.py
-------------
Tests all CanadianBids.ai Railway backend endpoints.
Run before every deploy to catch regressions.

Usage:
    python smoke_test.py
    python smoke_test.py --verbose    # show full response bodies
"""

import os
import sys
import json
import time
import argparse
import requests
from datetime import datetime

BASE_URL  = os.environ.get("API_BASE", "https://canadianbids-engine-production.up.railway.app")
SECRET    = os.environ.get("CRON_SECRET", "canadianbids2026")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://qplxzlnhykxknooqbtpa.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")  # optional for DB checks

TIMEOUT   = 30
PASSED    = []
FAILED    = []
WARNED    = []

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def log(symbol, color, label, detail=""):
    print(f"  {color}{symbol}{RESET} {label}" + (f"  →  {detail}" if detail else ""))


def test(name, method, path, expected_status=200, check_keys=None,
         body=None, headers=None, warn_keys=None, verbose=False):
    """Run a single endpoint test."""
    url = BASE_URL + path
    h   = {"Content-Type": "application/json", **(headers or {})}

    try:
        start = time.time()
        resp  = requests.request(method, url, json=body, headers=h, timeout=TIMEOUT)
        ms    = int((time.time() - start) * 1000)

        # Status check
        if resp.status_code != expected_status:
            FAILED.append(name)
            log("✗", RED, name, f"HTTP {resp.status_code} (expected {expected_status})")
            if verbose:
                print(f"      Body: {resp.text[:300]}")
            return

        # Parse JSON
        data = {}
        try:
            data = resp.json()
        except Exception:
            pass

        # Required keys
        if check_keys:
            missing = [k for k in check_keys if k not in data]
            if missing:
                FAILED.append(name)
                log("✗", RED, name, f"Missing keys: {missing}")
                return

        # Warn keys (non-fatal)
        if warn_keys:
            for k, condition, msg in warn_keys:
                val = data.get(k)
                if condition(val):
                    WARNED.append(f"{name}: {msg}")
                    log("⚠", YELLOW, name, msg)

        PASSED.append(name)
        detail = f"{ms}ms"
        if check_keys and data:
            # Show a useful value from the response
            for k in check_keys:
                v = data.get(k)
                if v is not None:
                    detail += f"  {k}={json.dumps(v)[:60]}"
                    break
        log("✓", GREEN, name, detail)

        if verbose and data:
            print(f"      {json.dumps(data, indent=2)[:400]}")

    except requests.exceptions.ConnectionError:
        FAILED.append(name)
        log("✗", RED, name, "Connection refused — is Railway up?")
    except requests.exceptions.Timeout:
        FAILED.append(name)
        log("✗", RED, name, f"Timeout after {TIMEOUT}s")
    except Exception as e:
        FAILED.append(name)
        log("✗", RED, name, str(e))


def run_tests(verbose=False):
    print(f"\n{BOLD}CanadianBids.ai Smoke Tests{RESET}")
    print(f"Target: {BASE_URL}")
    print(f"Time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 60)

    # ── 1. Health ──────────────────────────────────────────────────
    print(f"\n{BOLD}Health{RESET}")
    test("Health check", "GET", "/",
         check_keys=["status"],
         warn_keys=[("status", lambda v: v != "ok", "status is not 'ok'")],
         verbose=verbose)

    # ── 2. Auth-protected endpoints (should reject without secret) ──
    print(f"\n{BOLD}Auth protection{RESET}")
    test("Debug - no secret → 401", "GET", "/api/debug",
         expected_status=401, verbose=verbose)
    test("Match - no secret → 401", "POST", "/api/match",
         expected_status=401, verbose=verbose)
    test("Run-all - no secret → 401", "POST", "/api/run-all",
         expected_status=401, verbose=verbose)

    # ── 3. Debug endpoint ──────────────────────────────────────────
    print(f"\n{BOLD}Database connectivity{RESET}")
    test("Debug - with secret", "GET", f"/api/debug?secret={SECRET}",
         check_keys=["status", "tender_count"],
         warn_keys=[
             ("tender_count", lambda v: v is None or v == 0, "No tenders in DB — run fetch_canadabuys.py"),
             ("profile_count", lambda v: v is not None and v == 0, "No user profiles yet"),
         ],
         verbose=verbose)

    # ── 4. Tender documents ────────────────────────────────────────
    print(f"\n{BOLD}Tender documents API{RESET}")
    # Use a known tender ID that should have documents
    test("Documents - valid tender", "GET", "/api/tenders/71/documents",
         check_keys=["documents", "count"],
         warn_keys=[("count", lambda v: v == 0, "Tender 71 has no documents — check tender_documents table")],
         verbose=verbose)
    test("Documents - invalid tender", "GET", "/api/tenders/999999/documents",
         check_keys=["documents"],
         verbose=verbose)

    # ── 5. Fetch tenders endpoint ──────────────────────────────────
    print(f"\n{BOLD}CanadaBuys fetch endpoint{RESET}")
    # Route only exists after latest app.py deploy — check it exists
    test("Fetch tenders - no secret → 403", "POST", "/api/fetch-tenders",
         expected_status=403, verbose=verbose)

    # ── 6. Stripe endpoint ────────────────────────────────────────
    print(f"\n{BOLD}Stripe{RESET}")
    test("Checkout - missing params → 400", "POST", "/api/create-checkout",
         body={},
         expected_status=400,
         verbose=verbose)
    # Stripe is live — a valid request returns 200 with a checkout URL
    test("Checkout - valid request → 200 with URL", "POST", "/api/create-checkout",
         body={"user_id": "test-123", "email": "test@test.com", "plan": "monthly"},
         expected_status=200,
         check_keys=["url"],
         verbose=verbose)

    # ── 7. CORS headers ───────────────────────────────────────────
    print(f"\n{BOLD}CORS{RESET}")
    try:
        resp = requests.options(
            BASE_URL + "/api/tenders/71/documents",
            headers={"Origin": "https://canadianbidsai.ca"},
            timeout=TIMEOUT
        )
        if "Access-Control-Allow-Origin" in resp.headers:
            PASSED.append("CORS headers present")
            log("✓", GREEN, "CORS headers present",
                resp.headers.get("Access-Control-Allow-Origin", ""))
        else:
            WARNED.append("CORS: Access-Control-Allow-Origin header missing")
            log("⚠", YELLOW, "CORS headers missing",
                "OPTIONS response has no Access-Control-Allow-Origin")
    except Exception as e:
        WARNED.append(f"CORS check failed: {e}")
        log("⚠", YELLOW, "CORS check failed", str(e))

    # ── 8. Supabase direct checks ─────────────────────────────────
    print(f"\n{BOLD}Supabase data checks{RESET}")
    if SUPABASE_ANON_KEY:
        sb_headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}"
        }
        try:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/tenders?select=count&limit=1",
                headers=sb_headers, timeout=TIMEOUT
            )
            if r.ok:
                PASSED.append("Supabase tenders accessible")
                log("✓", GREEN, "Supabase tenders table accessible")
            else:
                WARNED.append("Supabase tenders not accessible via anon key")
                log("⚠", YELLOW, "Supabase tenders not accessible", f"HTTP {r.status_code}")
        except Exception as e:
            WARNED.append(f"Supabase check failed: {e}")
    else:
        log("⚠", YELLOW, "Supabase direct check skipped",
            "Set SUPABASE_ANON_KEY env var to enable")

    # ── Summary ───────────────────────────────────────────────────
    total = len(PASSED) + len(FAILED)
    print("\n" + "─" * 60)
    print(f"{BOLD}Results: {GREEN}{len(PASSED)} passed{RESET}  "
          f"{RED}{len(FAILED)} failed{RESET}  "
          f"{YELLOW}{len(WARNED)} warnings{RESET}  "
          f"/ {total} total")

    if WARNED:
        print(f"\n{YELLOW}Warnings:{RESET}")
        for w in WARNED:
            print(f"  ⚠ {w}")

    if FAILED:
        print(f"\n{RED}Failed tests:{RESET}")
        for f in FAILED:
            print(f"  ✗ {f}")
        print()
        sys.exit(1)
    else:
        print(f"\n{GREEN}All tests passed!{RESET}\n")
        sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CanadianBids.ai smoke tests")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show full response bodies")
    parser.add_argument("--base-url", help="Override API base URL")
    args = parser.parse_args()

    if args.base_url:
        BASE_URL = args.base_url

    run_tests(verbose=args.verbose)
