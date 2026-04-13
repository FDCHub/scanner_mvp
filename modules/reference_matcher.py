"""
reference_matcher.py
--------------------
Fuzzy matching of extracted document fields against reference_table.csv.

Matching strategy:
  1. account_number  — digit-normalized exact match (strongest signal)
  2. vendor_name     — fuzzy similarity >= vendor_threshold (default 0.80)
  3. service_address — fuzzy similarity >= address_threshold (default 0.72)
  4. property        — fuzzy similarity >= vendor_threshold
  5. unit            — case-insensitive exact match

A record is "matched" when >= min_matches (default 3) of the five fields agree.
When matched, static fields are pulled from the reference row and Claude is only
asked to extract the dynamic financial fields.
"""

import csv
import re
from difflib import SequenceMatcher
from pathlib import Path

REFERENCE_TABLE_PATH = Path(__file__).parent.parent / "data" / "reference_table.csv"

# Vendor categories where account_number is absent — matching uses vendor+property+unit instead
ACCOUNT_OPTIONAL_CATEGORIES: frozenset[str] = frozenset({
    "handyman services", "handyman", "insurance", "tax", "hoa",
})


def _is_account_optional(category: str) -> bool:
    return (category or "").lower().strip() in ACCOUNT_OPTIONAL_CATEGORIES


# Fields owned by the reference table — canonical, not re-extracted from document
STATIC_FIELDS = [
    "vendor_name",
    "vendor_category",
    "account_number",
    "property",
    "unit",
    "service_address",
    "property_folder_name",
    "document_subfolder",
]

# Fields that must be extracted from the document on every scan
DYNAMIC_FIELDS = [
    "due_date",
    "amount_due",
    "current_charges",
    "previous_balance",
    "payments_received",
    "late_fees",
]


# ── Text normalisation helpers ────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Lowercase, strip all non-alphanumeric — for fuzzy comparison."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _norm_account(s: str) -> str:
    """Digits only — for account number comparison."""
    return re.sub(r"[^0-9]", "", str(s or ""))


def _similarity(a: str, b: str) -> float:
    """
    SequenceMatcher ratio on normalised strings.
    Returns 0.0–1.0.  Empty inputs always return 0.0.
    """
    a_n, b_n = _norm(a), _norm(b)
    if not a_n or not b_n:
        return 0.0
    if a_n == b_n:
        return 1.0
    return SequenceMatcher(None, a_n, b_n).ratio()


# ── Reference table I/O ───────────────────────────────────────────────────────

def load_reference_table(path=None) -> list[dict]:
    path = Path(path) if path else REFERENCE_TABLE_PATH
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── Core fuzzy match ──────────────────────────────────────────────────────────

def match_reference_record(
    extracted: dict,
    reference_table: list[dict],
    min_matches: int = 3,
    vendor_threshold: float = 0.80,
    address_threshold: float = 0.72,
) -> tuple[dict | None, int, dict]:
    """
    Compare *extracted* fields against every active row in *reference_table*.

    Standard matching (account-based vendors):
      account_number  — digit-normalised exact match
      vendor_name     — fuzzy >= vendor_threshold
      service_address — fuzzy >= address_threshold
      property        — fuzzy >= vendor_threshold
      unit            — case-insensitive exact
      Requires min_matches hits (default 3).

    No-account matching (handyman services, insurance, tax, HOA):
      account_number is ignored; instead requires vendor_name + property
      (and optionally unit).  min_matches is effectively 2 for these rows.

    Returns:
      (best_row, match_count, match_details)
      match_details maps field_name -> score (0.0–1.0)
      Returns (None, 0, {}) if no row meets the threshold.
    """
    ext_account  = _norm_account(extracted.get("account_number", ""))
    ext_vendor   = (
        extracted.get("vendor_name_normalized")
        or extracted.get("vendor_name_raw")
        or extracted.get("vendor_name")
        or ""
    )
    ext_address  = extracted.get("service_address", "") or ""
    ext_property = extracted.get("property", "") or ""
    ext_unit     = (extracted.get("unit", "") or "").strip().lower()

    best_row, best_count, best_details = None, 0, {}

    for ref in reference_table:
        status = (ref.get("active_status", "active") or "active").lower()
        if status not in ("active", ""):
            continue

        ref_category = (ref.get("vendor_category") or "").lower().strip()
        no_account   = _is_account_optional(ref_category)

        hits: dict[str, float] = {}

        # 1 — Account number (exact, digits only) — skipped for no-account categories
        if not no_account:
            ref_account = _norm_account(ref.get("account_number", ""))
            if ext_account and ref_account and ext_account == ref_account:
                hits["account_number"] = 1.0

        # 2 — Vendor name (best of vendor_name / vendor_normalized_name)
        if ext_vendor:
            score = max(
                _similarity(ext_vendor, ref.get("vendor_name", "")),
                _similarity(ext_vendor, ref.get("vendor_normalized_name", "")),
            )
            if score >= vendor_threshold:
                hits["vendor_name"] = score

        # 3 — Service address
        if ext_address:
            score = _similarity(ext_address, ref.get("service_address", ""))
            if score >= address_threshold:
                hits["service_address"] = score

        # 4 — Property
        if ext_property:
            score = _similarity(ext_property, ref.get("property", ""))
            if score >= vendor_threshold:
                hits["property"] = score

        # 5 — Unit (exact, case-insensitive)
        ref_unit = (ref.get("unit", "") or "").strip().lower()
        if ext_unit and ref_unit and ext_unit == ref_unit:
            hits["unit"] = 1.0

        # Determine if this row matches:
        # No-account rows: need vendor_name + at least one of property/unit
        # Standard rows:   need min_matches hits total
        if no_account:
            qualifies = (
                "vendor_name" in hits and
                len(hits) >= 2 and
                len(hits) > best_count
            )
        else:
            qualifies = (
                len(hits) >= min_matches and
                len(hits) > best_count
            )

        if qualifies:
            best_count   = len(hits)
            best_row     = ref
            best_details = hits

    return best_row, best_count, best_details


def get_static_fields_from_match(ref_row: dict) -> dict:
    """Extract the canonical static fields from a matched reference row."""
    return {field: (ref_row.get(field) or "") for field in STATIC_FIELDS}


# ── Address canonicalisation ──────────────────────────────────────────────────

def canonicalize_service_address(
    extracted_address: str,
    reference_table: list[dict],
    threshold: float = 0.70,
) -> tuple[str, bool]:
    """
    Fuzzy-match an OCR/Claude address against every canonical service_address
    in the reference table.

    Returns (canonical_address, was_changed).
    If no match is found above *threshold*, returns (extracted_address, False).

    Examples of what gets corrected:
      "3715 Lincoln Ave"              → "3715 Lincoln Avenue, Oakland, CA 94602"
      "3715 Lincoln Avenue Oakland CA"→ "3715 Lincoln Avenue, Oakland, CA 94602"
      "1423 CENTRAL AVE"              → "1423 CENTRAL AVE HSE"
    """
    if not extracted_address:
        return extracted_address, False

    best_canonical = extracted_address
    best_score     = 0.0

    for ref in reference_table:
        canonical = (ref.get("service_address") or "").strip()
        if not canonical:
            continue
        score = _similarity(extracted_address, canonical)
        if score > best_score:
            best_score     = score
            best_canonical = canonical

    if best_score >= threshold and best_canonical != extracted_address:
        return best_canonical, True
    return extracted_address, False


# ── Pre-Claude text-based identifier extraction ───────────────────────────────

_ACCOUNT_PATS = [
    re.compile(r"account\s+(?:number\s+)?:?\s*([A-Z0-9][\w\-]{4,24})", re.I),
    re.compile(r"acct\.?\s*#?\s*:?\s*([A-Z0-9][\w\-]{4,24})", re.I),
    re.compile(r"account\s+([0-9]{6,20})", re.I),
]

_STREET_PAT = re.compile(
    r"\b(\d{3,5}\s+[A-Z][a-zA-Z]+(?:\s+[A-Za-z]+){0,3}\s+"
    r"(?:Ave(?:nue)?|St(?:reet)?|Rd|Road|Blvd|Boulevard|Dr(?:ive)?|Ln|Lane|Way|Ct|Pl)[\.,]?)",
    re.I,
)


def extract_identifiers_from_text(
    raw_text: str,
    reference_table: list[dict],
) -> dict:
    """
    Best-effort extraction of identifier fields from raw PDF text.
    Used for the pre-Claude reference-table lookup.

    Strategy:
      • Account number — regex patterns
      • Vendor name    — keyword search against known vendor names from reference table
      • Service address— street address regex + canonical fuzzy promotion
      • Property / unit— inferred from vendor/address match against reference table
    """
    result    = {}
    text_norm = _norm(raw_text)

    # 1. Account number
    for pat in _ACCOUNT_PATS:
        m = pat.search(raw_text)
        if m:
            result["account_number"] = m.group(1).strip()
            break

    # 2. Vendor name — scan known names from reference table
    for ref in reference_table:
        for name_field in ("vendor_name", "vendor_normalized_name"):
            name = (ref.get(name_field) or "").strip()
            if len(name) < 5:
                continue
            if _norm(name) in text_norm:
                result["vendor_name"] = ref.get("vendor_name", name)
                # Carry along known property/unit as bonus signals
                if ref.get("property"):
                    result["property"] = ref["property"]
                if ref.get("unit"):
                    result["unit"] = ref["unit"]
                break
        if "vendor_name" in result:
            break

    # 3. Service address — street regex, then try canonical upgrade
    m = _STREET_PAT.search(raw_text)
    if m:
        raw_addr = m.group(1).strip()
        canonical, changed = canonicalize_service_address(raw_addr, reference_table)
        result["service_address"] = canonical

    return result


# ── Legacy compatibility shim (used by main.py) ───────────────────────────────

def normalize_account_number(value: str) -> str:
    return _norm_account(value)


def reference_check(extracted: dict, reference_table: list[dict]) -> dict:
    """
    Legacy wrapper called by main.py.

    Accepts the old *extracted* key names (property_name, category_guess) and
    returns a decision dict in the old format.
    """
    # Normalise old key names so match_reference_record can find them
    mapped = {**extracted}
    if "property_name" in mapped and "property" not in mapped:
        mapped["property"] = mapped.pop("property_name")

    matched_row, count, details = match_reference_record(mapped, reference_table)

    if matched_row and count >= 2:
        return {
            "match_status":            "matched",
            "property_name":           matched_row.get("property", ""),
            "unit":                    matched_row.get("unit", ""),
            "category":                matched_row.get("vendor_category", ""),
            "needs_user_confirmation": count < 3,
            "routing_basis":           "+".join(details.keys()),
            "match_count":             count,
        }

    property_name = extracted.get("property_name") or extracted.get("property", "")
    return {
        "match_status":            "proposed",
        "property_name":           property_name,
        "unit":                    extracted.get("unit", ""),
        "category":                extracted.get("category_guess", ""),
        "needs_user_confirmation": True,
        "routing_basis":           "unknown",
        "match_count":             0,
    }
