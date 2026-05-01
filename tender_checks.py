"""
requirements.py — Deterministic procurement requirement checks.

Handles the parts of tender-vs-profile matching that don't need AI:
  - Geography           (pass/fail hard gate)
  - Security clearance  (pass/fail hard gate)
  - Certifications      (0-3 score)
  - Contract scale      (0-5 score)
  - Procurement vehicle (0-2 bonus)

Design principles:
  - Default to inclusion: when data is missing or ambiguous, PASS.
    Only reject when there is positive evidence of mismatch.
  - Hard gates only fire on confident mismatches.
  - Use structured tender fields first; fall back to text parsing.

The matcher.py pipeline calls run_deterministic_checks() between the
keyword pre-filter and the AI scoring stage. Tenders that fail either
hard gate are dropped before reaching the AI, saving tokens.
"""

import re


# ═══════════════════════════════════════════════════════════════════
# CLEARANCE HIERARCHY
# ═══════════════════════════════════════════════════════════════════

# Numeric levels for comparison. Higher number = more access.
# Per spec: Protected A/B map to Reliability tier (rank 1).
#           Protected C maps to Secret tier (rank 2).
CLEARANCE_LEVELS = {
    "":              0,
    "none":          0,
    "reliability":   1,
    "protected a":   1,
    "protected b":   1,
    "secret":        2,
    "protected c":   2,
    "top secret":    3,
}


def clearance_rank(level_text):
    """
    Map a clearance level string (any casing/format) to a numeric rank.
    Returns 0 if unknown or 'None'. Higher = more access.
    """
    if not level_text:
        return 0
    s = level_text.lower().strip()
    if s in CLEARANCE_LEVELS:
        return CLEARANCE_LEVELS[s]
    # Substring match (most specific first to avoid double-matching)
    for term in ("top secret", "protected c", "secret",
                 "protected b", "protected a", "reliability"):
        if term in s:
            return CLEARANCE_LEVELS[term]
    return 0


# Regex patterns for detecting clearance requirements in tender text.
# Order matters: more specific patterns first.
CLEARANCE_PATTERNS = [
    (r'\btop\s*secret\b',                            "top secret"),
    (r'\bprotected\s*c\b',                           "protected c"),
    (r'\bsecret\s+(clearance|level|status)\b',       "secret"),
    (r'\brequires?\s+secret\s+clearance\b',          "secret"),
    (r'\bsecret\s+security\s+clearance\b',           "secret"),
    (r'\bprotected\s*b\b',                           "protected b"),
    (r'\bprotected\s*a\b',                           "protected a"),
    (r'\breliability\s+(status|clearance|level)\b',  "reliability"),
    (r'\brequires?\s+reliability\b',                 "reliability"),
]


def detect_required_clearance(tender):
    """
    Detect the highest required clearance level from tender text.
    Returns the level string (e.g. "secret"), or "" if none detected.

    Conservative: only matches strong patterns. Casual mentions of the
    word "secret" or "protected" alone won't trigger a hard reject.
    """
    text_blob = " ".join([
        (tender.get("title") or ""),
        (tender.get("description") or ""),
        (tender.get("selection_criteria") or ""),
    ]).lower()

    if not text_blob.strip():
        return ""

    detected = []
    for pattern, level in CLEARANCE_PATTERNS:
        if re.search(pattern, text_blob):
            detected.append(level)

    if not detected:
        return ""

    # Return the highest-rank level among those detected
    return max(detected, key=clearance_rank)


# ═══════════════════════════════════════════════════════════════════
# GEOGRAPHY GATE
# ═══════════════════════════════════════════════════════════════════

# Province name → 2-letter code. Includes both English and French names.
PROVINCES = {
    "alberta": "ab",
    "british columbia": "bc", "colombie-britannique": "bc",
    "manitoba": "mb",
    "new brunswick": "nb", "nouveau-brunswick": "nb",
    "newfoundland": "nl",
    "newfoundland and labrador": "nl",
    "terre-neuve": "nl", "terre-neuve-et-labrador": "nl",
    "nova scotia": "ns", "nouvelle-écosse": "ns", "nouvelle-ecosse": "ns",
    "ontario": "on",
    "prince edward island": "pe", "île-du-prince-édouard": "pe",
    "ile-du-prince-edouard": "pe",
    "quebec": "qc", "québec": "qc",
    "saskatchewan": "sk",
    "northwest territories": "nt", "territoires du nord-ouest": "nt",
    "nunavut": "nu",
    "yukon": "yt",
}

# 2-letter codes (treated separately because they need word-boundary care)
PROVINCE_CODES = {"ab", "bc", "mb", "nb", "nl", "ns", "on", "pe", "qc",
                  "sk", "nt", "nu", "yt"}

NATIONAL_TERMS = ["national", "canada-wide", "canadawide",
                  "all of canada", "across canada"]


def normalize_province(text):
    """Normalize a province string to its 2-letter code, or '' if unknown."""
    if not text:
        return ""
    t = text.lower().strip()
    # Check 2-letter code first
    if t in PROVINCE_CODES:
        return t
    # Then full name
    return PROVINCES.get(t, "")


def parse_tender_regions(tender):
    """
    Extract the region info from a tender.

    Returns:
      {
        "is_national": bool,
        "provinces": set of 2-letter codes,
        "is_known": bool — True if we extracted something usable,
                          False means we have no region data
      }
    """
    region_field = (tender.get("region") or "").lower()

    if not region_field.strip():
        return {"is_national": False, "provinces": set(), "is_known": False}

    is_national = any(term in region_field for term in NATIONAL_TERMS)

    provinces = set()
    # Match full names
    for province_name, code in PROVINCES.items():
        if re.search(rf'\b{re.escape(province_name)}\b', region_field):
            provinces.add(code)
    # Match 2-letter codes (must be standalone with word boundaries)
    for code in PROVINCE_CODES:
        if re.search(rf'\b{code}\b', region_field):
            provinces.add(code)

    return {
        "is_national": is_national,
        "provinces": provinces,
        "is_known": is_national or len(provinces) > 0,
    }


def check_geography(tender, profile):
    """
    Geography hard gate.

    Rules:
      - National tender → PASS (always)
      - Tender region overlaps user's province / provinces_operating → PASS
      - Tender region missing or ambiguous → PASS (default-include)
      - Tender clearly outside user's footprint → FAIL

    Returns: {"pass": bool, "reason": str}
    """
    tender_regions = parse_tender_regions(tender)

    if not tender_regions["is_known"]:
        return {"pass": True, "reason": "Tender region unknown — assumed compatible"}

    if tender_regions["is_national"]:
        return {"pass": True, "reason": "National tender — open to all provinces"}

    # Tender has known specific provinces. Check user footprint.
    user_provinces = set()
    p = normalize_province(profile.get("province"))
    if p:
        user_provinces.add(p)
    for prov_text in (profile.get("provinces_operating") or []):
        c = normalize_province(prov_text)
        if c:
            user_provinces.add(c)

    if not user_provinces:
        # User hasn't specified provinces — default to PASS
        return {"pass": True, "reason": "User province(s) not set — assumed compatible"}

    overlap = user_provinces & tender_regions["provinces"]
    if overlap:
        return {
            "pass": True,
            "reason": f"User operates in {', '.join(sorted(overlap)).upper()}",
        }

    # User has provinces, tender has provinces, no overlap → fail
    tender_prov_str = ", ".join(sorted(tender_regions["provinces"])).upper()
    user_prov_str = ", ".join(sorted(user_provinces)).upper()
    return {
        "pass": False,
        "reason": f"Tender is {tender_prov_str}-only; user operates in {user_prov_str}",
    }


# ═══════════════════════════════════════════════════════════════════
# CLEARANCE GATE
# ═══════════════════════════════════════════════════════════════════

def check_clearance(tender, profile):
    """
    Security clearance hard gate.

    Rules:
      - Tender doesn't mention clearance → PASS
      - User's clearance ≥ tender's required level → PASS
      - Tender requires X, user has < X (or none) → FAIL

    Returns: {"pass": bool, "reason": str, "required_level": str, "user_level": str}
    """
    required = detect_required_clearance(tender)
    user_level = (profile.get("clearance_level") or "").lower().strip()

    if not required:
        return {
            "pass": True,
            "reason": "Tender does not require clearance",
            "required_level": "",
            "user_level": user_level,
        }

    if clearance_rank(user_level) >= clearance_rank(required):
        return {
            "pass": True,
            "reason": f"User has {user_level or 'sufficient'} clearance for {required} requirement",
            "required_level": required,
            "user_level": user_level,
        }

    return {
        "pass": False,
        "reason": f"Tender requires {required} clearance; user has {user_level or 'none'}",
        "required_level": required,
        "user_level": user_level,
    }


# ═══════════════════════════════════════════════════════════════════
# CERTIFICATIONS (0-3)
# ═══════════════════════════════════════════════════════════════════

# Words that suggest a tender is asking for certifications
CERT_INDICATORS = [
    "iso", "certified", "certification", "accredited", "accreditation",
    "license", "licensed", "licence", "p.eng", "professional engineer",
    "cgsb", "cisa", "cissp", "pmp", "csp", "ceh", "cpa",
]


def score_certifications(tender, profile):
    """
    Cert score 0-3. No hard reject.

    Logic:
      - Tender doesn't mention certs at all → 3 (default-include)
      - Tender mentions certs AND user has matching cert → 3
      - Tender mentions certs AND user has certs but none match → 1
      - Tender mentions certs AND user has no certs listed → 0

    Returns: {"score": int, "matched_certs": list, "tender_mentions_certs": bool}
    """
    tender_text = " ".join([
        (tender.get("title") or ""),
        (tender.get("description") or ""),
        (tender.get("selection_criteria") or ""),
    ]).lower()

    user_certs = [c for c in (profile.get("certifications") or []) if c]

    tender_mentions_certs = any(ind in tender_text for ind in CERT_INDICATORS)

    if not tender_mentions_certs:
        return {"score": 3, "matched_certs": [], "tender_mentions_certs": False}

    if not user_certs:
        return {"score": 0, "matched_certs": [], "tender_mentions_certs": True}

    matched = [c for c in user_certs if c.lower() in tender_text]
    if matched:
        return {"score": 3, "matched_certs": matched, "tender_mentions_certs": True}

    return {"score": 1, "matched_certs": [], "tender_mentions_certs": True}


# ═══════════════════════════════════════════════════════════════════
# SCALE (0-5)
# ═══════════════════════════════════════════════════════════════════

# Match $X, CAD X, X with optional thousand/million suffix
DOLLAR_RE = re.compile(
    r'(?:\$|cad\s*|cdn\s*)\s*'
    r'(\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?)'
    r'\s*'
    r'(k|m|million|thousand|bn|billion)?',
    re.IGNORECASE,
)


def parse_dollar_amount(match):
    """Convert a regex match into a dollar value (float), or None."""
    raw = match.group(1).replace(",", "").replace(" ", "")
    try:
        amount = float(raw)
    except ValueError:
        return None

    suffix = (match.group(2) or "").lower()
    if suffix in ("k", "thousand"):
        amount *= 1_000
    elif suffix in ("m", "million"):
        amount *= 1_000_000
    elif suffix in ("bn", "billion"):
        amount *= 1_000_000_000

    return amount


def extract_tender_value(tender):
    """
    Extract an estimated contract value from tender text.
    Strategy: take the largest plausible dollar amount in title/description.
    Returns None if nothing extractable.

    Sanity bounds: $100 to $1B (anything outside is likely junk).
    """
    text = " ".join([
        tender.get("title") or "",
        tender.get("description") or "",
    ])

    if not text.strip():
        return None

    candidates = []
    for m in DOLLAR_RE.finditer(text):
        val = parse_dollar_amount(m)
        if val and 100 <= val <= 1_000_000_000:
            candidates.append(val)

    if not candidates:
        return None

    return max(candidates)


def score_scale(tender, profile):
    """
    Scale score 0-5 — does the contract size fit the user's typical range?

    Per spec: if value can't be determined OR user range is unset,
    award full points (assume in range — no penalty for unknowns).

    Returns: {"score": int, "tender_value": float|None, "in_range": bool|None}
    """
    contract_min = profile.get("contract_min")
    contract_max = profile.get("contract_max")

    # Coerce to numeric
    try:
        contract_min = float(contract_min) if contract_min not in (None, "", "0") else None
    except (ValueError, TypeError):
        contract_min = None
    try:
        contract_max = float(contract_max) if contract_max not in (None, "", "Any", "any") else None
    except (ValueError, TypeError):
        contract_max = None

    if contract_min is None and contract_max is None:
        # User hasn't set a range — full points
        return {"score": 5, "tender_value": None, "in_range": None}

    tender_value = extract_tender_value(tender)
    if tender_value is None:
        # Tender value not parseable — full points (assume in range)
        return {"score": 5, "tender_value": None, "in_range": None}

    lo = contract_min if contract_min is not None else 0
    hi = contract_max if contract_max is not None else float('inf')

    if lo <= tender_value <= hi:
        return {"score": 5, "tender_value": tender_value, "in_range": True}

    # Out of range — degrade by how far
    if tender_value < lo:
        ratio = tender_value / lo if lo > 0 else 0
    else:
        ratio = hi / tender_value if tender_value > 0 else 0

    if ratio >= 0.5:
        score = 3
    elif ratio >= 0.2:
        score = 2
    else:
        score = 0

    return {"score": score, "tender_value": tender_value, "in_range": False}


# ═══════════════════════════════════════════════════════════════════
# PROCUREMENT VEHICLE (0-2 bonus)
# ═══════════════════════════════════════════════════════════════════

def score_vehicle(tender, profile):
    """
    Bonus 0-2 if user is on a supply arrangement / standing offer
    that this tender uses.
    """
    arrangements = profile.get("supply_arrangements") or []
    if not arrangements:
        return {"score": 0, "matched_arrangements": []}

    tender_text = " ".join([
        (tender.get("title") or ""),
        (tender.get("description") or ""),
        (tender.get("procurement_method") or ""),
    ]).lower()

    matched = [a for a in arrangements if a and a.lower() in tender_text]
    if matched:
        return {"score": 2, "matched_arrangements": matched}

    return {"score": 0, "matched_arrangements": []}


# ═══════════════════════════════════════════════════════════════════
# UNIFIED ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

def run_deterministic_checks(tender, profile):
    """
    Run all deterministic checks for a tender against a profile.

    Returns full breakdown dict:
      {
        "geo":       {"pass": bool, "reason": str},
        "clearance": {"pass": bool, "reason": str, ...},
        "certs":     {"score": 0-3, ...},
        "scale":     {"score": 0-5, ...},
        "vehicle":   {"score": 0-2, ...},
        "hard_reject":        bool,   # True if either gate failed
        "hard_reject_reason": str,    # populated when hard_reject True
        "deterministic_score": int,   # certs + scale + vehicle (0-10)
      }
    """
    geo = check_geography(tender, profile)
    clearance = check_clearance(tender, profile)
    certs = score_certifications(tender, profile)
    scale = score_scale(tender, profile)
    vehicle = score_vehicle(tender, profile)

    hard_reject = not (geo["pass"] and clearance["pass"])

    if not geo["pass"]:
        reject_reason = f"Geography: {geo['reason']}"
    elif not clearance["pass"]:
        reject_reason = f"Clearance: {clearance['reason']}"
    else:
        reject_reason = ""

    return {
        "geo": geo,
        "clearance": clearance,
        "certs": certs,
        "scale": scale,
        "vehicle": vehicle,
        "hard_reject": hard_reject,
        "hard_reject_reason": reject_reason,
        "deterministic_score": certs["score"] + scale["score"] + vehicle["score"],
    }
