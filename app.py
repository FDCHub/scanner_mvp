"""
app.py
------
Flask web server for scanner_mvp dashboard.
Run with:  python app.py
Open:      http://localhost:5000
"""

import os
import re
import json
import shutil
import threading
import time
import traceback
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

from google_drive import drive_sync, do_daily_backup, should_run_daily_backup

from PyPDF2 import PdfReader

from claude_analyzer import analyze_document, analyze_document_dynamic, get_overall_confidence
from vendor_normalizer import normalize_vendor_name
from vendor_profile_store import load_vendor_profiles
from modules.reference_matcher import (
    load_reference_table,
    match_reference_record,
    get_static_fields_from_match,
    canonicalize_service_address,
    extract_identifiers_from_text,
)
from csv_manager import (
    initialize_csv_files,
    upsert_reference_record,
    append_document_master_record,
    delete_master_log_record,
    check_for_duplicate,
    read_csv_as_dicts,
    get_all_master_records,
    get_record_by_index,
    update_record_by_index,
    delete_record_by_index,
    delete_reference_if_orphaned,
    MASTER_LOG_CSV,
)

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)


# ================================================================
# DOCUMENT TYPE / CATEGORY HELPERS  (mirrors main.py)
# ================================================================

def normalize_document_type(doc_type: str) -> str:
    if not doc_type:
        return ""
    clean = str(doc_type).strip().lower()
    if clean in {"utility_bill", "utility bill", "bill"}:
        return "bill"
    if clean == "invoice":
        return "invoice"
    if clean == "receipt":
        return "receipt"
    return clean


# ── Category normalisation ────────────────────────────────────────────────────
_CATEGORY_MAP: dict[str, str] = {
    "utility":           "Utilities",
    "utilities":         "Utilities",
    "utility bill":      "Utilities",
    "utility_bill":      "Utilities",
    "handyman services": "Handyman",
    "handyman":          "Handyman",
    "financial":         "Financial",
    "insurance":         "Insurance",
    "maintenance":       "Maintenance",
    "repairs":           "Repairs",
    "repair":            "Repairs",
    "permits":           "Permits",
    "licenses":          "Licenses",
}


def _normalize_category(category: str) -> str:
    """Map any raw vendor_category string to a standard PropertyDocs folder name."""
    key = (category or "").strip().lower()
    return _CATEGORY_MAP.get(key, "NeedsReview")


def infer_vendor_category(result: dict) -> str:
    """Infer vendor category from vendor name / document type when Claude hasn't set it."""
    vendor = str(
        result.get("vendor_name_normalized")
        or result.get("vendor_name_raw")
        or result.get("vendor", "")
    ).lower()
    doc_type = normalize_document_type(result.get("document_type", ""))

    handyman_kw = ["handyman", "repair", "plumbing", "electric", "hvac", "maintenance", "contractor"]
    utility_kw  = ["utility", "power", "water", "gas", "electricity", "comcast", "xfinity",
                   "internet", "public utilities"]

    if any(kw in vendor for kw in handyman_kw):
        return "Handyman"
    if doc_type == "receipt":
        return "NeedsReview"
    if doc_type == "invoice":
        return "NeedsReview"
    if any(kw in vendor for kw in utility_kw):
        return "Utilities"
    if doc_type == "bill":
        return "Utilities"
    return "NeedsReview"

INCOMING_DIR      = Path("D:/Scans/Incoming")
WORKING_DIR       = Path("D:/Scans/Working")
FILED_DIR         = Path("D:/Scans/Filed")
ERROR_DIR         = Path("D:/Scans/Error")
DUP_DIR           = Path("D:/Scans/Duplicates")
DELETED_DIR       = Path("D:/Scans/Deleted")
PROPERTY_DOCS_DIR = Path("D:/PropertyDocs")
CONFIG_PATH       = Path(__file__).parent / "config.json"

for folder in [INCOMING_DIR, WORKING_DIR, FILED_DIR, ERROR_DIR, DUP_DIR, PROPERTY_DOCS_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

queue        = {}
_queue_lock  = threading.Lock()   # guards all mutations of `queue`
activity_log = []


# ================================================================
# SIDECAR + QUEUE HELPERS
# ================================================================

def _load_sidecar(f: Path) -> "tuple[dict, dict] | tuple[None, None]":
    """
    Read a sidecar .review.json file.
    Retries up to 3 times with a short delay to tolerate partial writes
    (the watcher thread may still be flushing when Flask first reads it).
    Returns (sidecar_data, validation) or (None, None) on failure.
    """
    sidecar_path = INCOMING_DIR / (f.name + ".review.json")
    if not sidecar_path.exists():
        return None, None

    sidecar_data = None
    for attempt in range(3):
        try:
            with open(sidecar_path, "r", encoding="utf-8") as sf:
                sidecar_data = json.load(sf)
            break  # success
        except Exception as exc:
            if attempt < 2:
                time.sleep(0.2)   # wait for watcher to finish flushing
            else:
                log_activity(f"Sidecar JSON parse error for {f.name}: {exc}", "warning")
                return None, None

    try:
        vendor_profiles = load_vendor_profiles()
        validation = run_validation(sidecar_data, vendor_profiles)
    except Exception as exc:
        log_activity(f"Sidecar validation error for {f.name}: {exc}", "warning")
        validation = {}
    return sidecar_data, validation


def _notify_review(filename: str, path_str: str, sidecar_data: dict) -> None:
    """
    Called by the watcher thread immediately after it writes the sidecar and
    moves the PDF back to Incoming.  Updates the in-memory queue directly so
    the web dashboard picks up the document without waiting for the next poll.
    Thread-safe: acquires _queue_lock before mutating `queue`.
    """
    try:
        vendor_profiles = load_vendor_profiles()
        validation      = run_validation(sidecar_data, vendor_profiles)
    except Exception:
        validation = {}

    entry = {
        "id":         filename,
        "filename":   filename,
        "path":       path_str,
        "status":     "review",
        "result":     sidecar_data,
        "validation": validation,
        "error":      None,
        "added":      datetime.now().strftime("%H:%M:%S"),
    }
    with _queue_lock:
        queue[filename] = entry

    vendor = (sidecar_data.get("vendor_name_normalized")
              or sidecar_data.get("vendor_name_raw")
              or filename)
    log_activity(f"Ready for review: {vendor}", "info")
    print(f"[Queue] Watcher notified → {filename} is now in review")


# ================================================================
# CONFIG HELPERS
# ================================================================

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_config(config: dict):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


def add_to_config_list(list_name: str, value: str) -> bool:
    """
    Add a new approved value to a config dropdown list.
    Returns True if added, False if already exists.
    """
    config = load_config()
    lst    = config.get(list_name, [])
    if value.lower() in [v.lower() for v in lst]:
        return False
    lst.append(value)
    config[list_name] = lst
    save_config(config)
    return True


# ================================================================
# REFERENCE TABLE HELPERS
# ================================================================

REFERENCE_CSV_PATH = Path(__file__).parent / "data" / "reference_table.csv"

_ref_table_cache: list[dict] | None = None
_ref_table_mtime: float = 0.0


def _get_reference_table() -> list[dict]:
    """Return reference table, reloading if the file has changed on disk."""
    global _ref_table_cache, _ref_table_mtime
    try:
        mtime = REFERENCE_CSV_PATH.stat().st_mtime if REFERENCE_CSV_PATH.exists() else 0.0
    except OSError:
        mtime = 0.0
    if _ref_table_cache is None or mtime != _ref_table_mtime:
        _ref_table_cache = load_reference_table(REFERENCE_CSV_PATH)
        _ref_table_mtime = mtime
    return _ref_table_cache


def _pdf_quick_text(file_path: Path, max_chars: int = 2000) -> str:
    """
    Extract raw text from the first two pages of a PDF using PyPDF2.
    Returns empty string on any error (graceful degradation).
    """
    try:
        reader = PdfReader(str(file_path))
        text = ""
        for page in reader.pages[:2]:
            text += (page.extract_text() or "")
            if len(text) >= max_chars:
                break
        return text[:max_chars]
    except Exception:
        return ""


def _try_early_reference_match(
    file_path: Path,
) -> tuple[dict | None, dict]:
    """
    Attempt to identify a document against the reference table BEFORE calling
    Claude, using only cheap PDF text extraction + regex + fuzzy matching.

    Returns (matched_ref_row, static_fields_dict) if >= 3 fields match,
    or (None, {}) if no confident match is found.
    """
    ref_table = _get_reference_table()
    if not ref_table:
        return None, {}

    raw_text = _pdf_quick_text(file_path)
    if not raw_text.strip():
        return None, {}

    identifiers = extract_identifiers_from_text(raw_text, ref_table)
    if not identifiers:
        return None, {}

    matched_row, count, details = match_reference_record(
        identifiers, ref_table, min_matches=3
    )

    if matched_row and count >= 3:
        static_fields = get_static_fields_from_match(matched_row)
        print(
            f"  [RefMatch] Early match: {matched_row.get('vendor_name')} "
            f"| {count} fields matched: {list(details.keys())}"
        )
        return matched_row, static_fields

    return None, {}


def _apply_canonical_values(result: dict) -> dict:
    """
    Post-process a Claude result dict:
      1. Fuzzy-match service_address against known canonicals and replace if
         similarity >= threshold (handles OCR address variations).
      2. If >= 3 fields match a reference row, override static fields with
         canonical values and set _reference_match = True.

    This is the fallback path for documents where the early match failed
    (e.g. blank PDF text layer) but Claude still extracted usable data.
    """
    ref_table = _get_reference_table()
    if not ref_table:
        return result

    # 1. Canonicalize service address
    raw_address = result.get("service_address", "") or ""
    if raw_address:
        canonical_addr, changed = canonicalize_service_address(raw_address, ref_table)
        if changed:
            result["service_address"] = canonical_addr
            print(f"  [Canon] Address corrected: '{raw_address}' → '{canonical_addr}'")

    # 2. Try post-hoc reference match with the now-enriched Claude result
    if not result.get("_reference_match"):
        matched_row, count, details = match_reference_record(
            result, ref_table, min_matches=3
        )
        if matched_row and count >= 3:
            static = get_static_fields_from_match(matched_row)
            for field, value in static.items():
                if value:
                    # Map to the Claude result key names
                    if field == "vendor_name":
                        result.setdefault("vendor_name_normalized", value)
                        result.setdefault("vendor_name_raw", value)
                    elif field == "vendor_category":
                        result["vendor_category"] = value
                    else:
                        result[field] = value
            result["_reference_match"]        = True
            result["_reference_match_fields"] = list(details.keys())
            result["_reference_match_source"] = "post_claude"
            print(
                f"  [RefMatch] Post-Claude match: {matched_row.get('vendor_name')} "
                f"| {count} fields: {list(details.keys())}"
            )

    return result


# ================================================================
# HELPERS
# ================================================================

def log_activity(message: str, level: str = "info"):
    activity_log.insert(0, {
        "time":    datetime.now().strftime("%H:%M:%S"),
        "message": message,
        "level":   level,
    })
    if len(activity_log) > 50:
        activity_log.pop()


# ================================================================
# GOOGLE DRIVE SYNC HELPERS
# ================================================================

def _trigger_drive_sync(filed_pdf_path: Path, property_name: str,
                         category: str = "", account_code: str = "") -> None:
    """
    Queue all post-filing Drive uploads (non-blocking — runs in background).
    Called immediately after a document is confirmed & filed.
    """
    status = drive_sync.get_status()
    if status["status"] == "unconfigured":
        return   # Drive not set up — skip silently

    # 1. Upload the filed PDF to its full 3-level folder path
    if filed_pdf_path.exists():
        drive_sync.queue_pdf_upload(filed_pdf_path, property_name,
                                     category=category, account_code=account_code)

    # 2. Upload updated master log
    master_log = Path("data/document_master_log.csv")
    if master_log.exists():
        drive_sync.queue_appdata_upload(master_log)

    # 3. Upload reference table (upsert_reference_record was called)
    ref_table = Path("data/reference_table.csv")
    if ref_table.exists():
        drive_sync.queue_appdata_upload(ref_table)


_ABBREV_STOPWORDS = frozenset({"of", "and", "the", "a", "an", "for", "in", "at", "by", "to"})


def _vendor_abbrev(vendor: str) -> str:
    """
    Generate a short uppercase folder-name abbreviation from a vendor name.

    Rules (in priority order):
      1. All-caps / acronym (e.g. EBMUD, PG&E)  → strip non-alpha, use up to 6 chars
      2. 2 significant words (e.g. Lacatis Construction) → first-initial + first-4 of word2
      3. 3+ significant words → first letter of each significant word
      4. 1 significant word  → first 4 alpha chars

    Significant words = words not in _ABBREV_STOPWORDS.
    """
    # All-caps / acronym check (digits and & are allowed in the original, ignore them)
    alpha_only = "".join(c for c in vendor if c.isalpha())
    if alpha_only and alpha_only == alpha_only.upper():
        return alpha_only[:6] or "UNKN"

    # Tokenise and filter stopwords
    tokens = re.findall(r"[A-Za-z]+", vendor)
    significant = [t for t in tokens if t.lower() not in _ABBREV_STOPWORDS]

    if not significant:
        return alpha_only[:4].upper() or "UNKN"

    if len(significant) == 1:
        return significant[0][:4].upper()

    if len(significant) == 2:
        # First initial of word 1 + first 4 letters of word 2
        return (significant[0][0] + significant[1][:4]).upper()

    # 3+ words → initial of each
    return "".join(t[0] for t in significant).upper()


def build_filing_path(result: dict) -> Path:
    prop     = (result.get("property") or "Unknown Property").strip()
    category = _normalize_category(result.get("vendor_category") or "")
    vendor   = (
        result.get("vendor_name_normalized")
        or result.get("vendor_name_raw")
        or result.get("vendor_name")
        or "unknown"
    ).strip()
    account  = (result.get("account_number") or "").strip()

    abbrev = _vendor_abbrev(vendor)

    # Account suffix: last 4 stripped digits, or "0000" if no account
    if account:
        digits_only  = re.sub(r"[^0-9]", "", account)
        acct_suffix  = digits_only[-4:] if digits_only else "0000"
    else:
        acct_suffix  = "0000"

    account_code = f"{abbrev}_{acct_suffix}"

    return PROPERTY_DOCS_DIR / prop / category / account_code


def build_filename(result: dict, original_name: str) -> str:
    vendor = (
        result.get("vendor_name_normalized")
        or result.get("vendor_name_raw")
        or "unknown"
    )
    # Safe vendor slug: keep alphanumeric, replace spaces with underscores
    vendor = "".join(c for c in vendor if c.isalnum() or c in " _-")[:28].strip().replace(" ", "_")
    # Property abbreviation: strip spaces (e.g. "1423CentralAve")
    prop   = (result.get("property") or "").replace(" ", "").replace(",", "")[:16]
    unit   = (result.get("unit") or "").strip()
    unit_s = f"_{unit}" if unit else ""
    date   = (result.get("bill_date") or datetime.now().strftime("%Y-%m-%d"))[:10]
    amount = result.get("amount_due", "")
    amt_s  = f"_${amount}" if amount else ""
    suffix = Path(original_name).suffix or ".pdf"
    return f"{vendor}_{prop}{unit_s}_{date}{amt_s}{suffix}"


def run_validation(result: dict, vendor_profiles: dict) -> dict:
    vendor_raw        = result.get("vendor_name_raw", "")
    vendor_normalized = result.get("vendor_name_normalized", "") or vendor_raw
    account_number    = result.get("account_number", "")
    property_val      = result.get("property", "")
    report            = {}

    # 1 — Vendor normalization
    if vendor_normalized and vendor_normalized != vendor_raw and vendor_raw:
        report["vendor_normalization"] = {
            "status": "matched", "label": "Matched known vendor",
            "detail": f"{vendor_raw} → {vendor_normalized}", "level": "success"}
    elif vendor_normalized:
        in_profiles = vendor_normalized in vendor_profiles
        report["vendor_normalization"] = {
            "status": "known" if in_profiles else "new",
            "label":  "Known vendor" if in_profiles else "New vendor — please verify",
            "detail": vendor_normalized,
            "level":  "success" if in_profiles else "warning"}
    else:
        report["vendor_normalization"] = {
            "status": "unknown", "label": "Vendor not identified",
            "detail": "Could not extract vendor name", "level": "error"}

    # 2 — Reference table
    ref_rows  = read_csv_as_dicts(MASTER_LOG_CSV) if os.path.exists(MASTER_LOG_CSV) else []
    ref_match = next(
        (r for r in ref_rows
         if r.get("vendor_name", "").lower() == vendor_normalized.lower()
         and r.get("account_number", "") == account_number and account_number), None)
    if ref_match and ref_match.get("property"):
        report["reference_table"] = {
            "status": "full_match", "label": "Full match in reference table",
            "detail": f"Property: {ref_match['property']} | Unit: {ref_match.get('unit','—')}",
            "level": "success"}
    elif ref_match:
        report["reference_table"] = {
            "status": "partial_match", "label": "Partial match — property unknown",
            "detail": "Vendor matched but no property on record", "level": "warning"}
    else:
        report["reference_table"] = {
            "status": "no_match", "label": "No match in reference table",
            "detail": "First time seeing this vendor/account combination", "level": "info"}

    # 3 — Account number
    profile        = vendor_profiles.get(vendor_normalized, {})
    known_accounts = profile.get("known_accounts", {}) if profile else {}
    if account_number and account_number in known_accounts:
        acct_data = known_accounts[account_number]
        report["account_number"] = {
            "status": "known", "label": "Known account",
            "detail": f"Account {account_number} — {acct_data.get('property','?')}",
            "level": "success"}
    elif account_number and profile:
        report["account_number"] = {
            "status": "new_account", "label": "New account for known vendor",
            "detail": f"Account {account_number} not previously seen for {vendor_normalized}",
            "level": "warning"}
    elif account_number:
        report["account_number"] = {
            "status": "new", "label": "New account number",
            "detail": account_number, "level": "info"}
    else:
        report["account_number"] = {
            "status": "missing", "label": "No account number found",
            "detail": "Not present on document or could not be extracted",
            "level": "warning"}

    # 4 — Property
    if property_val:
        report["property"] = {
            "status": "confirmed", "label": "Property confirmed",
            "detail": property_val, "level": "success"}
    elif ref_match and ref_match.get("property"):
        report["property"] = {
            "status": "inferred", "label": "Property inferred from reference table",
            "detail": ref_match["property"], "level": "warning"}
    else:
        report["property"] = {
            "status": "unknown", "label": "Property unknown — assign manually",
            "detail": "Could not determine property from document or reference table",
            "level": "error"}

    # 5 — Duplicate check
    is_dup, dup_reason, dup_match = check_for_duplicate({
        "vendor_name":    vendor_normalized,
        "account_number": account_number,
        "document_date":  result.get("bill_date", ""),
        "amount_due":     result.get("amount_due", ""),
    })
    if is_dup:
        report["duplicate_check"] = {
            "status": "duplicate", "label": "Duplicate detected",
            "detail": f"Already filed on {dup_match.get('timestamp','?')}",
            "level": "error", "match": dup_match}
    else:
        report["duplicate_check"] = {
            "status": "ok", "label": "No duplicate found",
            "detail": "This document has not been filed before", "level": "success"}

    # 6 — Overall
    levels = [v["level"] for v in report.values()]
    if "error" in levels:
        report["overall"] = {"status": "low",
            "label": "Low confidence — review required before filing", "level": "error"}
    elif levels.count("warning") >= 2:
        report["overall"] = {"status": "medium",
            "label": "Medium confidence — verify flagged fields", "level": "warning"}
    else:
        report["overall"] = {"status": "high",
            "label": "High confidence — ready to file", "level": "success"}

    return report


# ================================================================
# ROUTES — Static
# ================================================================

@app.route("/")
def index():
    return send_from_directory("templates", "index.html")


@app.route("/coverage")
def coverage():
    return send_from_directory("templates", "coverage.html")


@app.route("/api/view-file", methods=["GET"])
def view_file():
    """Serve a filed document inline for the coverage matrix View PDF button."""
    import mimetypes as _mime
    path_str = request.args.get("path", "").strip()
    if not path_str:
        return jsonify({"error": "No path provided"}), 400

    p = Path(path_str).resolve()

    # Safety: restrict to known document roots only
    _allowed_roots = [
        Path("D:/PropertyDocs").resolve(),
        Path("D:/Scans").resolve(),
    ]
    if not any(str(p).startswith(str(root)) for root in _allowed_roots):
        return jsonify({"error": "Access denied: path outside allowed directories"}), 403

    if not p.exists() or not p.is_file():
        return jsonify({"error": f"File not found: {path_str}"}), 404

    ext = p.suffix.lower()
    mime_map = {
        ".pdf":  "application/pdf",
        ".jpg":  "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png":  "image/png",
        ".heic": "image/heic",
        ".gif":  "image/gif",
        ".webp": "image/webp",
    }
    mime_type = mime_map.get(ext) or _mime.guess_type(str(p))[0] or "application/octet-stream"

    from flask import send_file as _send_file
    return _send_file(str(p), mimetype=mime_type, as_attachment=False)


@app.route("/api/view-scan", methods=["GET"])
def view_scan():
    """
    Return an HTML viewer page for a scan file (Working → Incoming fallback).
    Includes a minimal Print / Email toolbar above the embedded document.
    """
    import urllib.parse as _up
    filename = request.args.get("filename", "").strip()
    # Reject path traversal
    if not filename or "/" in filename or "\\" in filename or ".." in filename:
        return "Invalid filename", 400

    for search_dir in [WORKING_DIR, INCOMING_DIR]:
        p = search_dir / filename
        if p.exists() and p.is_file():
            view_url = "/api/view-file?path=" + _up.quote(str(p))
            safe_name = filename.replace('"', '').replace("'", "")
            html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{safe_name}</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ display:flex; flex-direction:column; height:100vh; font-family:-apple-system,sans-serif; background:#1a1a1a; }}
  .toolbar {{ padding:8px 16px; background:#1a1a1a; color:#fff; display:flex; align-items:center; gap:10px; flex-shrink:0; }}
  .toolbar .fname {{ font-size:13px; opacity:0.65; flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
  .tb-btn {{ padding:5px 14px; border:none; border-radius:5px; cursor:pointer; font-size:13px; font-weight:500; }}
  .tb-print {{ background:#2563eb; color:#fff; }}
  .tb-print:hover {{ background:#1d4ed8; }}
  .tb-email {{ background:#059669; color:#fff; }}
  .tb-email:hover {{ background:#047857; }}
  iframe {{ flex:1; border:none; width:100%; background:#fff; }}
</style>
</head>
<body>
  <div class="toolbar">
    <span class="fname">{safe_name}</span>
    <button class="tb-btn tb-print" onclick="document.getElementById('sf').contentWindow.print()">🖨 Print</button>
    <button class="tb-btn tb-email" onclick="window.location='mailto:?subject={_up.quote(safe_name)}'">✉ Email</button>
  </div>
  <iframe id="sf" src="{view_url}"></iframe>
</body>
</html>"""
            return html, 200, {"Content-Type": "text/html; charset=utf-8"}

    return "File not found", 404


# ================================================================
# ROUTES — Config / dropdowns
# ================================================================

@app.route("/api/config", methods=["GET"])
def get_config():
    """Return all approved dropdown values."""
    return jsonify(load_config())


@app.route("/api/config/add", methods=["POST"])
def add_config_value():
    """
    Add a new approved value to a dropdown list.
    Body: { "list": "properties", "value": "5500 Broadway" }
    """
    data      = request.get_json()
    list_name = data.get("list", "")
    value     = data.get("value", "").strip()
    if not list_name or not value:
        return jsonify({"error": "list and value are required"}), 400
    added = add_to_config_list(list_name, value)
    if added:
        log_activity(f"New approved value added: '{value}' → {list_name}", "info")
    return jsonify({"ok": True, "added": added, "already_existed": not added})


@app.route("/api/config/add-unit", methods=["POST"])
def add_unit_to_config():
    """
    Add a new unit to units_by_property for a given property.
    Body: { "property": "3715 Lincoln Ave", "unit": "11" }
    """
    data     = request.get_json()
    property = (data.get("property") or "").strip()
    unit     = (data.get("unit") or "").strip()
    if not property or not unit:
        return jsonify({"error": "property and unit are required"}), 400
    config = load_config()
    units  = config.setdefault("units_by_property", {}).setdefault(property, [])
    if unit.lower() in [u.lower() for u in units]:
        return jsonify({"ok": True, "added": False, "already_existed": True})
    units.append(unit)
    save_config(config)
    log_activity(f"New unit added: '{unit}' → {property}", "info")
    return jsonify({"ok": True, "added": True, "already_existed": False})


@app.route("/api/canonical-values", methods=["GET"])
def get_canonical_values():
    """
    Return all canonical dropdown values for the review modal.

    Combines:
      • config.json  — properties, units_by_property, vendor_categories,
                       payment_statuses, document_types
      • reference_table.csv — service addresses keyed by property, plus a
                              per-account lookup for service_address inference
    """
    config     = load_config()
    ref_table  = _get_reference_table()

    # Build service_addresses_by_property from reference table
    addresses_by_property: dict[str, list[str]] = {}
    account_address_map:   list[dict]            = []

    for ref in ref_table:
        if (ref.get("active_status") or "active").lower() not in ("active", ""):
            continue
        prop    = (ref.get("property") or "").strip()
        addr    = (ref.get("service_address") or "").strip()
        account = (ref.get("account_number") or "").strip()
        vendor  = (ref.get("vendor_name") or "").strip()

        if prop and addr:
            bucket = addresses_by_property.setdefault(prop, [])
            if addr not in bucket:
                bucket.append(addr)

        if account and addr:
            account_address_map.append({
                "vendor_name":    vendor,
                "account_number": account,
                "property":       prop,
                "unit":           (ref.get("unit") or "").strip(),
                "service_address": addr,
                "vendor_category": (ref.get("vendor_category") or "").strip(),
            })

    return jsonify({
        "properties":               config.get("properties", []),
        "units_by_property":        config.get("units_by_property", {}),
        "vendor_categories":        config.get("vendor_categories", []),
        "payment_statuses":         config.get("payment_statuses", []),
        "document_types":           config.get("document_types", ["bill", "invoice", "receipt"]),
        "service_addresses_by_property": addresses_by_property,
        "account_address_map":      account_address_map,
    })


# ================================================================
# ROUTES — Queue
# ================================================================

@app.route("/api/queue", methods=["GET"])
def get_queue():
    # Only files explicitly added by the user (via upload or browse-and-add)
    # live in the queue dict.  We never scan INCOMING_DIR automatically.
    # Prune any pending/error entries whose file has since disappeared from disk.
    with _queue_lock:
        stale = [
            k for k, v in queue.items()
            if v["status"] in ("pending", "error")
            and not Path(v.get("path", "")).exists()
        ]
        for k in stale:
            del queue[k]
        raw = list(queue.values())

    # Annotate each item with file mtime for the UI card display
    result = []
    for item in raw:
        out = dict(item)
        p = Path(item.get("path", ""))
        try:
            out["mtime"] = p.stat().st_mtime if p.exists() else None
        except OSError:
            out["mtime"] = None
        result.append(out)
    return jsonify(result)


def _add_to_queue(filename: str, path: Path) -> None:
    """Add a file to the in-memory queue as pending (thread-safe)."""
    with _queue_lock:
        if filename not in queue:
            queue[filename] = {
                "id":         filename,
                "filename":   filename,
                "path":       str(path),
                "status":     "pending",
                "result":     None,
                "validation": None,
                "error":      None,
                "added":      datetime.now().strftime("%H:%M:%S"),
            }


@app.route("/api/queue/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400
    existing = get_all_master_records()
    if any(r.get("source_file") == f.filename for r in existing):
        log_activity(f"Skipped duplicate source: {f.filename}", "warning")
        return jsonify({"ok": False, "duplicate_source": True, "filename": f.filename}), 409
    dest = INCOMING_DIR / f.filename
    f.save(str(dest))
    _add_to_queue(f.filename, dest)
    log_activity(f"Uploaded: {f.filename}", "info")
    return jsonify({"ok": True, "filename": f.filename})


@app.route("/api/queue/upload-path", methods=["POST"])
def upload_from_path():
    """Copy a file from a local path into the Incoming queue."""
    data = request.get_json() or {}
    path_str = data.get("path", "")
    if not path_str:
        return jsonify({"error": "No path provided"}), 400
    p = Path(path_str)
    if not p.exists() or not p.is_file():
        return jsonify({"error": "File not found", "path": path_str}), 404
    existing = get_all_master_records()
    if any(r.get("source_file") == p.name for r in existing):
        log_activity(f"Skipped duplicate source: {p.name}", "warning")
        return jsonify({"ok": False, "duplicate_source": True, "filename": p.name}), 409
    dest = INCOMING_DIR / p.name
    # If the file is already in Incoming, skip the copy and queue it directly
    if p.parent.resolve() == INCOMING_DIR.resolve():
        _add_to_queue(p.name, str(p))
        log_activity(f"Added from path: {p.name}", "info")
        return jsonify({"ok": True, "filename": p.name})
    shutil.copy(str(p), str(dest))
    _add_to_queue(p.name, str(dest))
    log_activity(f"Added from path: {p.name}", "info")
    return jsonify({"ok": True, "filename": p.name})


@app.route("/api/queue/remove/<filename>", methods=["DELETE"])
def remove_from_queue(filename):
    path = INCOMING_DIR / filename
    if path.exists():
        path.unlink()
    with _queue_lock:
        queue.pop(filename, None)
    log_activity(f"Removed from queue: {filename}", "info")
    return jsonify({"ok": True})


# ================================================================
# ROUTES — Processing with SSE stage progress
# ================================================================

@app.route("/api/process/<filename>", methods=["GET", "POST"])
def process_file(filename):
    file_path = INCOMING_DIR / filename
    if not file_path.exists():
        return jsonify({"error": f"File not found: {filename}"}), 404

    def generate():
        def emit(stage, pct, label, status="running", detail="", data=None):
            payload = {"stage": stage, "pct": pct, "label": label,
                       "status": status, "detail": detail}
            if data:
                payload["data"] = data
            yield f"data: {json.dumps(payload)}\n\n"

        try:
            with _queue_lock:
                queue[filename]["status"] = "processing"

            yield from emit("intake", 10, "Grabbing file from Incoming folder", "running")
            working_path = WORKING_DIR / filename
            shutil.copy(str(file_path), str(working_path))
            yield from emit("intake", 10, "File moved to Working folder", "success", filename)

            # ── Early reference-table match (before Claude) ──────────────
            yield from emit("reference_check", 20,
                            "Checking reference table for known account...", "running")
            early_ref_row, early_static = _try_early_reference_match(working_path)

            if early_ref_row:
                vendor_label = early_static.get("vendor_name", "?")
                match_detail = (
                    f"Vendor: {vendor_label} | "
                    f"Account: {early_static.get('account_number', '?')} | "
                    f"Property: {early_static.get('property', '?')}"
                )
                yield from emit("reference_check", 20,
                                "Reference match found — static fields confirmed",
                                "success", match_detail)

                yield from emit("claude_send", 35,
                                "Sending to Claude for financial fields only", "running",
                                "Targeted extraction — vendor/property already known")
                yield from emit("claude_analyze", 55,
                                "Claude extracting billing data...", "running",
                                "Reading amounts, dates, charges")
                result = analyze_document_dynamic(working_path, early_static)
                result["_reference_match"]        = True
                result["_reference_match_fields"] = list(early_static.keys())
                result["_reference_match_source"] = "early_pdf_text"
            else:
                yield from emit("reference_check", 20,
                                "No early reference match — full extraction", "info",
                                "Sending complete document to Claude")

                yield from emit("claude_send", 30, "Sending document to Claude API", "running",
                                "Uploading document image to Anthropic")
                yield from emit("claude_analyze", 55,
                                "Claude reading and extracting data...", "running",
                                "Analyzing document image")
                result = analyze_document(working_path)
                result["_reference_match"] = False
                # Post-hoc: canonicalize addresses + try reference match
                result = _apply_canonical_values(result)

            vendor_out = result.get("vendor_name_normalized") or result.get("vendor_name_raw", "?")
            yield from emit("claude_analyze", 55, "Extraction complete", "success",
                            f"Vendor: {vendor_out} | Amount: ${result.get('amount_due','?')} | Date: {result.get('bill_date','?')}")

            yield from emit("claude_results", 65, "Normalizing extracted data", "running")
            if result.get("vendor_name_raw") and not result.get("vendor_name_normalized"):
                result["vendor_name_normalized"] = normalize_vendor_name(result["vendor_name_raw"])
            claude_confidence = get_overall_confidence(result)
            # Reference match boosts confidence to high
            if result.get("_reference_match"):
                claude_confidence = "high"
            result["_confidence"]   = claude_confidence
            result["_filename"]     = filename
            result["_working_path"] = str(working_path)

            # ── Vendor category: fill in if missing ──────────────
            if not result.get("vendor_category"):
                result["vendor_category"] = infer_vendor_category(result)

            # ── Auto-detect PAID stamp via quick text scan ────────
            if not result.get("payment_status"):
                quick_txt = _pdf_quick_text(working_path).upper()
                if "PAID" in quick_txt:
                    result["payment_status"] = "paid"

            yield from emit("claude_results", 65, "Data normalized", "success",
                            f"Overall confidence: {claude_confidence.upper()}"
                            + (" ✓ Reference matched" if result.get("_reference_match") else ""))

            yield from emit("confidence", 70, "Calculating field confidence scores", "running")
            fc         = result.get("field_confidence", {})
            low_fields = [k for k, v in fc.items() if v == "low"]
            med_fields = [k for k, v in fc.items() if v == "medium"]
            conf_parts = []
            if low_fields:     conf_parts.append(f"Low: {', '.join(low_fields)}")
            if med_fields:     conf_parts.append(f"Medium: {', '.join(med_fields)}")
            if not conf_parts: conf_parts.append("All fields high confidence")
            yield from emit("confidence", 70, "Field confidence scored",
                            "warning" if low_fields else "success",
                            " | ".join(conf_parts))

            yield from emit("reference", 80,
                            "Checking reference table and vendor profiles", "running")
            vendor_profiles = load_vendor_profiles()
            validation      = run_validation(result, vendor_profiles)
            ref  = validation.get("reference_table", {})
            acct = validation.get("account_number", {})
            yield from emit("reference", 80,
                            f"Reference: {ref.get('label','done')} | Account: {acct.get('label','done')}",
                            "warning" if ref.get("level") == "warning" else "success",
                            f"{ref.get('detail','')} | {acct.get('detail','')}")

            yield from emit("duplicate", 90, "Checking for duplicate documents", "running")
            dup    = validation.get("duplicate_check", {})
            is_dup = dup.get("status") == "duplicate"
            yield from emit("duplicate", 90,
                            dup.get("label", "Duplicate check complete"),
                            "error" if is_dup else "success",
                            dup.get("detail", ""))

            with _queue_lock:
                queue[filename]["status"]     = "review"
                queue[filename]["result"]     = result
                queue[filename]["validation"] = validation
            overall = validation.get("overall", {})
            yield from emit("review", 100, "Ready for your review", "success",
                            overall.get("label", ""),
                            data={"result": result, "validation": validation,
                                  "confidence": claude_confidence, "filename": filename})

        except Exception as e:
            with _queue_lock:
                queue[filename]["status"] = "error"
                queue[filename]["error"]  = str(e)
            log_activity(f"Error: {filename} — {e}", "error")
            yield from emit("error", 100, "Processing failed", "error", str(e))

    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/process/all", methods=["POST"])
def process_all():
    with _queue_lock:
        pending = [v for v in queue.values() if v["status"] == "pending"]
    if not pending:
        return jsonify({"message": "No pending files"}), 200

    def run_all():
        for item in pending:
            try:
                working_path = WORKING_DIR / item["filename"]
                shutil.copy(item["path"], str(working_path))

                # Try early reference match first
                early_ref_row, early_static = _try_early_reference_match(working_path)
                if early_ref_row:
                    result = analyze_document_dynamic(working_path, early_static)
                    result["_reference_match"]        = True
                    result["_reference_match_fields"] = list(early_static.keys())
                    result["_reference_match_source"] = "early_pdf_text"
                else:
                    result = analyze_document(working_path)
                    result["_reference_match"] = False
                    result = _apply_canonical_values(result)

                if result.get("vendor_name_raw") and not result.get("vendor_name_normalized"):
                    result["vendor_name_normalized"] = normalize_vendor_name(
                        result["vendor_name_raw"])
                confidence = get_overall_confidence(result)
                if result.get("_reference_match"):
                    confidence = "high"
                result["_confidence"]   = confidence
                result["_filename"]     = item["filename"]
                result["_working_path"] = str(working_path)

                # Vendor category inference + PAID auto-detect
                if not result.get("vendor_category"):
                    result["vendor_category"] = infer_vendor_category(result)
                if not result.get("payment_status"):
                    quick_txt = _pdf_quick_text(working_path).upper()
                    if "PAID" in quick_txt:
                        result["payment_status"] = "paid"

                vendor_profiles = load_vendor_profiles()
                validation = run_validation(result, vendor_profiles)
                with _queue_lock:
                    queue[item["filename"]]["status"]     = "review"
                    queue[item["filename"]]["result"]     = result
                    queue[item["filename"]]["validation"] = validation
                log_activity(
                    f"Analyzed: {item['filename']} — "
                    f"{result.get('vendor_name_normalized','?')} ({confidence})"
                    + (" [ref matched]" if result.get("_reference_match") else ""), "info")
            except Exception as e:
                with _queue_lock:
                    queue[item["filename"]]["status"] = "error"
                    queue[item["filename"]]["error"]  = str(e)
                log_activity(f"Failed: {item['filename']} — {e}", "error")

    threading.Thread(target=run_all, daemon=True).start()
    return jsonify({"message": f"Processing {len(pending)} files",
                    "count": len(pending)})


# ================================================================
# ROUTES — Review & confirmation
# ================================================================

@app.route("/api/confirm/<filename>", methods=["POST"])
def confirm_document(filename):
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    with _queue_lock:
        item = queue.get(filename)
    if not item:
        return jsonify({"error": "File not in queue"}), 404

    working_path = Path(item.get("result", {}).get("_working_path", ""))
    try:
        # Canonicalize service_address before storing — even if the user typed
        # a variation, we always persist the canonical form.
        raw_service_addr = data.get("service_address", "")
        if raw_service_addr:
            canonical_addr, _ = canonicalize_service_address(
                raw_service_addr, _get_reference_table()
            )
            data["service_address"] = canonical_addr

        path_override = (data.pop("_path_override", "") or "").strip()
        if path_override:
            dest_path    = Path(path_override)
            dest_folder  = dest_path.parent
            new_filename = dest_path.name
        else:
            dest_folder  = build_filing_path(data)
            new_filename = build_filename(data, filename)
            dest_path    = dest_folder / new_filename
        dest_folder.mkdir(parents=True, exist_ok=True)

        # ── Destination collision check ─────────────────────────────
        if dest_path.exists():
            log_activity(
                f"Filing blocked — destination already exists: {dest_path.name}", "warning"
            )
            return jsonify({
                "ok": False, "duplicate": True,
                "reason": (
                    "A file already exists at this location — "
                    "this document may already be filed"
                ),
                "original": {"filed_on": "", "filed_path": str(dest_path)},
            }), 409

        if working_path.exists():
            shutil.move(str(working_path), str(dest_path))
        else:
            src = INCOMING_DIR / filename
            if src.exists():
                shutil.move(str(src), str(dest_path))

        incoming_path = INCOMING_DIR / filename
        if incoming_path.exists():
            incoming_path.unlink()

        master_record = {
            "document_type":              data.get("document_type", ""),
            "vendor_name":                data.get("vendor_name_normalized", "") or data.get("vendor_name_raw", ""),
            "vendor_category":            data.get("vendor_category", ""),
            "account_number":             data.get("account_number", ""),
            "property":                   data.get("property", ""),
            "unit":                       data.get("unit", ""),
            "service_address":            data.get("service_address", ""),
            "document_date":              data.get("bill_date", ""),
            "service_period_start":       data.get("service_period_start", ""),
            "service_period_end":         data.get("service_period_end", ""),
            "due_date":                   data.get("due_date", ""),
            "previous_balance":           data.get("previous_balance", ""),
            "payments_received":          data.get("payments_received", ""),
            "credits_adjustments":        data.get("credits_adjustments", ""),
            "late_fees":                  data.get("late_fees", ""),
            "penalties":                  data.get("penalties", ""),
            "taxes_fees":                 data.get("taxes_fees", ""),
            "current_charges":            data.get("current_charges", ""),
            "current_period_amount":      data.get("current_period_amount", ""),
            "amount_due":                 data.get("amount_due", ""),
            "payment_status":             data.get("payment_status", "unpaid"),
            "review_required_accounting": data.get("review_required_accounting", "no"),
            "accounting_notes":           data.get("accounting_notes", ""),
            "source_file":                filename,
            "final_storage_path":         str(dest_path),
            "confidence_score":           data.get("_confidence", ""),
        }

        written, dup_reason, dup_match = append_document_master_record(master_record)

        if not written:
            if dest_path.exists():
                shutil.move(str(dest_path), str(DUP_DIR / new_filename))
            with _queue_lock:
                queue[filename]["status"] = "error"
            log_activity(
                f"Duplicate blocked: {master_record['vendor_name']} "
                f"{master_record['document_date']}", "warning")
            return jsonify({
                "ok": False, "duplicate": True, "reason": dup_reason,
                "original": {
                    "filed_on":   dup_match.get("timestamp", ""),
                    "filed_path": dup_match.get("final_storage_path", ""),
                },
            }), 409

        upsert_reference_record({
            "vendor_name":       master_record["vendor_name"],
            "account_number":    master_record["account_number"],
            "vendor_category":   master_record["vendor_category"],
            "property":          master_record["property"],
            "unit":              master_record["unit"],
            "service_address":   master_record["service_address"],
            "billing_frequency": data.get("billing_frequency", ""),
        })

        # Both CSV operations succeeded — safe to remove sidecar now.
        # If either operation above threw, we'd be in the except block and
        # the sidecar would remain, letting the user retry.
        sidecar_path = INCOMING_DIR / (filename + ".review.json")
        if sidecar_path.exists():
            sidecar_path.unlink()

        with _queue_lock:
            queue[filename]["status"] = "filed"

        log_activity(
            f"Filed: {data.get('vendor_name_normalized','?')} — "
            f"${data.get('amount_due','?')} → {dest_path.parent.name}", "success")

        # ── Google Drive sync (non-blocking background uploads) ──────────
        # dest_path = PROPERTY_DOCS_DIR / prop / category / account_code / filename
        _filing_folder = dest_path.parent          # …/account_code/
        _drive_category     = _filing_folder.parent.name   # category folder
        _drive_account_code = _filing_folder.name          # account_code folder
        _trigger_drive_sync(dest_path, data.get("property", ""),
                             category=_drive_category,
                             account_code=_drive_account_code)

        return jsonify({
            "ok": True,
            "filed_path":   str(dest_path),
            "new_filename": new_filename,
        })

    except Exception as e:
        with _queue_lock:
            if filename in queue:
                queue[filename]["status"] = "error"
        log_activity(f"Filing error for {filename}: {e}", "error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/reject/<filename>", methods=["POST"])
def reject_document(filename):
    with _queue_lock:
        item = queue.get(filename, {})
    result       = item.get("result") or {}
    working_path = WORKING_DIR / filename

    if working_path.exists():
        shutil.move(str(working_path), str(ERROR_DIR / filename))

    incoming_path = INCOMING_DIR / filename
    if incoming_path.exists():
        incoming_path.unlink()

    if result:
        vendor = (result.get("vendor_name_normalized", "") or
                  result.get("vendor_name_raw", ""))
        removed = delete_master_log_record({
            "vendor_name":    vendor,
            "account_number": result.get("account_number", ""),
            "document_date":  result.get("bill_date", ""),
            "amount_due":     result.get("amount_due", ""),
        })
        if removed:
            log_activity(
                f"Removed partial record for rejected doc: {filename}", "warning")

    with _queue_lock:
        queue.pop(filename, None)

    sidecar_path = INCOMING_DIR / (filename + ".review.json")
    if sidecar_path.exists():
        sidecar_path.unlink()

    log_activity(f"Rejected: {filename}", "warning")
    return jsonify({"ok": True})


# ================================================================
# ROUTES — Status & data
# ================================================================

@app.route("/api/activity", methods=["GET"])
def get_activity():
    return jsonify(activity_log[:20])


@app.route("/api/activity/log", methods=["POST"])
def post_activity_log():
    """Accept a client-side timing message and add it to the activity feed."""
    data  = request.get_json(silent=True) or {}
    msg   = (data.get("message") or "").strip()
    level = data.get("level", "info")
    if msg:
        log_activity(msg, level)
    return jsonify({"ok": True})


@app.route("/api/status", methods=["GET"])
def get_status():
    with _queue_lock:
        pending = sum(1 for v in queue.values() if v["status"] == "pending")
        review  = sum(1 for v in queue.values() if v["status"] == "review")
        errors  = sum(1 for v in queue.values() if v["status"] == "error")
    return jsonify({
        "incoming_folder": str(INCOMING_DIR),
        "pending":         pending,
        "review":          review,
        "errors":          errors,
        "api_key_loaded":  bool(os.getenv("ANTHROPIC_API_KEY")),
    })


def _enrich_with_overdue(rows: list[dict]) -> list[dict]:
    """Add is_overdue flag to each master log row."""
    today = datetime.now().strftime("%Y-%m-%d")
    for r in rows:
        status   = (r.get("payment_status") or "").lower()
        due_date = (r.get("due_date") or "").strip()
        r["is_overdue"] = bool(
            due_date and due_date < today and status in ("unpaid", "")
        )
    return rows


@app.route("/api/master-log", methods=["GET"])
def get_master_log():
    rows = get_all_master_records()
    # Sort by document_date descending (most-recent first); blank dates go to bottom
    rows.sort(key=lambda r: r.get("document_date", "") or "", reverse=True)
    return jsonify(_enrich_with_overdue(rows))


@app.route("/api/master-log/<int:index>", methods=["GET"])
def get_master_log_record(index):
    """Get a single master log record by its CSV row_index.
    Also injects billing_frequency from the matching reference table row."""
    record = get_record_by_index(index)
    if record is None:
        return jsonify({"error": "Record not found"}), 404
    record["_row_index"] = index
    # Enrich with billing_frequency from reference table
    acct = (record.get("account_number") or "").strip()
    if acct:
        ref_rows = read_csv_as_dicts(str(Path("data/reference_table.csv")))
        matched = next((r for r in ref_rows if (r.get("account_number") or "").strip() == acct), None)
        record["billing_frequency"] = (matched.get("billing_frequency") or "") if matched else ""
    else:
        record.setdefault("billing_frequency", "")
    return jsonify(record)


@app.route("/api/master-log/<int:index>", methods=["PUT"])
def update_master_log_record(index):
    """
    Update a single master log record by display index.
    Optional payload keys:
      move_file: bool        — also move the physical PDF to a new path
      new_filing_path: str   — new full path (required when move_file=true)
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # Pull out optional file-move instructions before writing to CSV
    move_file         = data.pop("move_file", False)
    new_filing_path   = data.pop("new_filing_path", "")
    billing_frequency = data.pop("billing_frequency", None)  # reference table field, not master log

    old_record = get_record_by_index(index)
    if old_record is None:
        return jsonify({"error": "Record not found"}), 404

    updated = update_record_by_index(index, data)
    if not updated:
        return jsonify({"error": "Record not found"}), 404

    file_moved = False
    file_error = ""

    if move_file and new_filing_path:
        old_path_str = old_record.get("final_storage_path", "")
        if old_path_str:
            try:
                old_p = Path(old_path_str)
                new_p = Path(new_filing_path)
                if old_p.exists():
                    new_p.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(old_p), str(new_p))
                    file_moved = True
                    # Update final_storage_path directly (bypasses protection)
                    rows2 = read_csv_as_dicts(MASTER_LOG_CSV)
                    if 0 <= index < len(rows2):
                        rows2[index]["final_storage_path"] = str(new_p)
                        from csv_manager import write_csv as _write_csv, MASTER_LOG_FIELDS as _ML_FIELDS
                        _write_csv(MASTER_LOG_CSV, _ML_FIELDS, rows2)
                else:
                    file_error = f"Source file not found: {old_path_str}"
            except Exception as e:
                file_error = str(e)

    # Update billing_frequency in reference table if provided
    if billing_frequency is not None:
        acct = old_record.get("account_number", "")
        if acct:
            upsert_reference_record({
                "vendor_name":       old_record.get("vendor_name", ""),
                "account_number":    acct,
                "vendor_category":   old_record.get("vendor_category", ""),
                "property":          old_record.get("property", ""),
                "unit":              old_record.get("unit", ""),
                "service_address":   old_record.get("service_address", ""),
                "billing_frequency": billing_frequency,
            })

    vendor = old_record.get("vendor_name", "?")
    date   = old_record.get("document_date", "?")
    log_activity(
        f"Record edited: {vendor} {date}"
        + (" + file moved" if file_moved else "")
        + (f" (move error: {file_error})" if file_error else ""),
        "info")

    return jsonify({
        "ok":         True,
        "file_moved": file_moved,
        "file_error": file_error,
    })


@app.route("/api/master-log/<int:index>", methods=["DELETE"])
def delete_master_log_record_route(index):
    """
    Delete a master log record and (optionally) its filed PDF.
    Body: { "delete_file": true }

    Contract:
      • The CSV record is deleted if found, regardless of file status.
      • Physical file missing → ok:true, file_deleted:false, file_error set.
      • Physical file present → deleted, file_deleted:true.
      • Only returns ok:false when the CSV delete itself fails.
      • On success, orphaned reference-table entries are also removed.
    """
    try:
        data        = request.get_json(silent=True) or {}
        delete_file = data.get("delete_file", True)

        # ── 1. Load record BEFORE deleting (need vendor/path info) ──
        print(f"[Delete] Attempting delete at index={index}")
        record = get_record_by_index(index)
        if record is None:
            print(f"[Delete] Record not found at index={index}")
            return jsonify({"ok": False, "error": f"Record not found at index {index}"}), 404

        vendor     = record.get("vendor_name", "?")
        account    = record.get("account_number", "")
        date       = record.get("document_date", "?")
        filed_path = (record.get("final_storage_path") or "").strip()
        print(f"[Delete] Found record: vendor={vendor!r} date={date!r} path={filed_path!r}")

        # ── 2. Delete the CSV record — this is the critical step ────
        success, _ = delete_record_by_index(index)
        if not success:
            msg = f"Index {index} out of range after re-read"
            print(f"[Delete] FAILED: {msg}")
            return jsonify({"ok": False, "error": msg}), 500

        # ── 3. Move physical file to D:/Scans/Deleted/ with timestamp ──
        file_moved  = False
        file_error  = ""
        moved_to    = ""
        if delete_file and filed_path:
            try:
                p = Path(filed_path)
                if p.exists():
                    DELETED_DIR.mkdir(parents=True, exist_ok=True)
                    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
                    new_name = f"{p.stem}_deleted_{ts}{p.suffix}"
                    dest     = DELETED_DIR / new_name
                    shutil.move(str(p), str(dest))
                    moved_to   = str(dest)
                    file_moved = True
                    print(f"[Delete] Moved file to: {dest}")
                    # Mirror deletion on Google Drive (move to PropertyDocs/Deleted/)
                    try:
                        drive_sync.queue_file_delete(filed_path)
                    except Exception as drive_exc:
                        print(f"[Delete] Drive delete queue failed: {drive_exc}")
                else:
                    file_error = f"File not on disk: {filed_path}"
                    print(f"[Delete] {file_error}")
            except Exception as exc:
                file_error = str(exc)

        # ── 4. Remove orphaned reference-table entry if applicable ──
        ref_removed = False
        try:
            ref_removed = delete_reference_if_orphaned(vendor, account)
        except Exception as exc:
            log_activity(f"Reference cleanup error for {vendor}: {exc}", "warning")

        log_activity(
            f"Deleted: {vendor} {date}"
            + (" + file moved" if file_moved else "")
            + (" + ref" if ref_removed else ""),
            "warning",
        )

        return jsonify({
            "ok":          True,
            "file_moved":  file_moved,
            "file_error":  file_error,
            "filed_path":  filed_path,
            "moved_to":    moved_to,
            "ref_removed": ref_removed,
        })

    except Exception as exc:
        tb = traceback.format_exc()
        print(f"[Delete] EXCEPTION at index={index}:\n{tb}")
        log_activity(f"Delete error (index {index}): {exc}", "error")
        return jsonify({"ok": False, "error": str(exc), "traceback": tb}), 500





# ================================================================
# ROUTES — Google Drive sync status
# ================================================================

@app.route("/api/sync-status", methods=["GET"])
def get_sync_status():
    return jsonify(drive_sync.get_status())


@app.route("/api/sync-retry", methods=["POST"])
def sync_retry():
    drive_sync.retry_failed()
    return jsonify({"ok": True, "message": "Re-queued failed uploads"})


# ================================================================
# ROUTES — Folder browser
# ================================================================

_DRIVE_LABEL_OVERRIDES: dict[str, str] = {
    "C": "Lenovo",
    "D": "Ext HDD",
}

@app.route("/api/list-drives", methods=["GET"])
def list_drives():
    """Return all accessible drive letters with their volume labels."""
    drives = []
    for letter in "CDEFGHIJKLMNOPQRSTUVWXYZ":
        path = f"{letter}:/"
        if not os.path.exists(path):
            continue
        label = ""
        try:
            vol_output = os.popen(f"vol {letter}:").read()
            # vol output: "Volume in drive D is Expansion\n Volume Serial Number is ..."
            for line in vol_output.splitlines():
                line = line.strip()
                if line.lower().startswith("volume in drive"):
                    parts = line.split(" is ", 1)
                    if len(parts) == 2:
                        label = parts[1].strip()
                    break
        except Exception:
            pass
        # Apply override if defined for this drive letter
        label = _DRIVE_LABEL_OVERRIDES.get(letter, label)
        drives.append({"letter": letter, "path": path, "label": label})
    return jsonify(drives)


@app.route("/api/browse-folder", methods=["GET"])
def browse_folder():
    import os as _os
    path_str = request.args.get("path", "C:/")
    # Normalise: replace all backslashes and strip surrounding quotes/whitespace
    path_str = path_str.strip().strip('"\'').replace("\\", "/")
    if not path_str:
        path_str = "C:/"
    # Use os.path for reliable Windows drive-letter path handling
    path_str = _os.path.normpath(path_str).replace("\\", "/")
    try:
        if not _os.path.exists(path_str):
            return jsonify({"error": f"Path not found: {path_str}", "path": path_str, "items": []}), 200
        if not _os.path.isdir(path_str):
            return jsonify({"error": f"Not a directory: {path_str}", "path": path_str, "items": []}), 200
        p = Path(path_str)
        items = []
        parent = _os.path.dirname(path_str)
        # Add ".." entry unless we're already at the drive root
        if parent and _os.path.normpath(parent) != _os.path.normpath(path_str):
            items.append({"name": "..", "type": "dir",
                          "path": parent.replace("\\", "/"), "ext": ""})
        for child in p.iterdir():
            if child.name.startswith("."):
                continue
            try:
                mtime = child.stat().st_mtime
            except OSError:
                mtime = 0
            items.append({
                "name":  child.name,
                "type":  "dir" if child.is_dir() else "file",
                "path":  str(child).replace("\\", "/"),
                "ext":   child.suffix.lower() if child.is_file() else "",
                "mtime": mtime,
            })
        # Dirs first (alphabetical), files second (newest first)
        items.sort(key=lambda x: (x["type"] == "file", -x["mtime"] if x["type"] == "file" else 0, x["name"].lower()))
        return jsonify({"path": path_str, "items": items})
    except PermissionError:
        return jsonify({"error": f"Permission denied: {path_str}", "path": path_str, "items": []}), 200
    except Exception as e:
        log_activity(f"browse-folder error ({path_str}): {e}", "warning")
        return jsonify({"error": str(e), "path": path_str, "items": []}), 200


# ================================================================
# ROUTES — PropertyMedia spaces
# ================================================================

_MEDIA_CATEGORIES = ["General", "Damage", "Before-After", "Ads", "Other"]
_MEDIA_ROOT = Path("D:/PropertyMedia")


def _scan_media_spaces() -> list[dict]:
    """Return current PropertyMedia structure as a list of property dicts."""
    result = []
    if not _MEDIA_ROOT.exists():
        return result
    for prop_dir in sorted(_MEDIA_ROOT.iterdir()):
        if not prop_dir.is_dir() or prop_dir.name.startswith("."):
            continue
        spaces = []
        for space_dir in sorted(prop_dir.iterdir()):
            if not space_dir.is_dir() or space_dir.name.startswith("."):
                continue
            categories = sorted(
                d.name for d in space_dir.iterdir()
                if d.is_dir() and not d.name.startswith(".")
            )
            spaces.append({"name": space_dir.name, "categories": categories})
        result.append({"name": prop_dir.name, "spaces": spaces})
    return result


@app.route("/api/property-media-spaces", methods=["GET"])
def get_property_media_spaces():
    return jsonify({"properties": _scan_media_spaces()})


@app.route("/api/property-media/add-space", methods=["POST"])
def add_property_media_space():
    data = request.get_json(force=True) or {}
    space_name = (data.get("space_name") or "").strip()
    properties = [p for p in (data.get("properties") or []) if isinstance(p, str)]

    if not space_name:
        return jsonify({"error": "space_name is required"}), 400
    if not properties:
        return jsonify({"error": "At least one property must be selected"}), 400
    # Prevent path traversal
    if any(c in space_name for c in ("/", "\\", "..", ":")):
        return jsonify({"error": "Invalid space name"}), 400

    created, errors = [], []
    for prop_name in properties:
        prop_dir = _MEDIA_ROOT / prop_name
        if not prop_dir.exists() or not prop_dir.is_dir():
            errors.append(f"Property not found: {prop_name}")
            continue
        space_dir = prop_dir / space_name
        for cat in _MEDIA_CATEGORIES:
            (space_dir / cat).mkdir(parents=True, exist_ok=True)
        created.append(prop_name)
        log_activity(f"PropertyMedia: created space '{space_name}' under '{prop_name}'")

    return jsonify({"ok": True, "created": created, "errors": errors,
                    "properties": _scan_media_spaces()})


# ================================================================
# ROUTES — Account history & summary stats
# ================================================================

@app.route("/api/account-history", methods=["GET"])
def get_account_history():
    account_number = request.args.get("account_number", "").strip()
    vendor         = request.args.get("vendor", "").strip().lower()
    if not account_number:
        return jsonify([])
    rows = get_all_master_records()
    matches = [r for r in rows if (r.get("account_number") or "").strip() == account_number]
    if vendor:
        matches = [r for r in matches
                   if (r.get("vendor_name") or "").lower() == vendor]
    matches.sort(key=lambda r: r.get("document_date", ""), reverse=True)
    return jsonify(matches[:10])


@app.route("/api/summary-stats", methods=["GET"])
def get_summary_stats():
    today = datetime.now().strftime("%Y-%m-%d")
    rows  = get_all_master_records()
    prop_filter = request.args.get("property", "").strip()
    if prop_filter:
        rows = [r for r in rows if (r.get("property") or "").strip() == prop_filter]
    unpaid_count  = 0
    unpaid_amount = 0.0
    overdue_count = 0
    for r in rows:
        status = (r.get("payment_status") or "unpaid").lower().strip()
        # Count unpaid and overdue as outstanding
        if status not in ("unpaid", "overdue"):
            continue
        unpaid_count += 1
        raw_amt = (r.get("amount_due") or "").replace("$", "").replace(",", "").strip()
        try:
            unpaid_amount += float(raw_amt)
        except ValueError:
            pass
        due_date = (r.get("due_date") or "").strip()
        # Only count as overdue when due_date is a parseable ISO date that has passed
        if due_date:
            try:
                import datetime as _dt
                _dt.date.fromisoformat(due_date)   # validates YYYY-MM-DD format
                if due_date < today:
                    overdue_count += 1
            except ValueError:
                pass   # "Due Upon Receipt" and other non-dates are ignored
    return jsonify({
        "total_filed":   len(rows),
        "unpaid_count":  unpaid_count,
        "unpaid_amount": round(unpaid_amount, 2),
        "overdue_count": overdue_count,
    })


# ================================================================
# ROUTES — Coverage matrix
# ================================================================

def _billing_expected_months(billing_freq: str, months: list, acct_docs: list) -> "set | None":
    """
    Return the set of year-month strings that are expected billing months for
    this account given its billing frequency.
    Returns None for 'random' — meaning no month is ever flagged as missing.
    """
    freq = (billing_freq or "").strip().lower()
    if freq in ("monthly", "weekly", ""):
        return set(months)
    if freq == "bi-monthly":
        return {m for m in months if int(m.split("-")[1]) in (1, 3, 5, 7, 9, 11)}
    if freq == "quarterly":
        return {m for m in months if int(m.split("-")[1]) in (1, 4, 7, 10)}
    if freq == "yearly":
        # Use the calendar month of the earliest ever-filed doc for this account,
        # defaulting to January if no history exists.
        filed_yms = sorted(
            d.get("year_month", "") for d in acct_docs if d.get("year_month")
        )
        first_mo = int(filed_yms[0].split("-")[1]) if filed_yms else 1
        return {m for m in months if int(m.split("-")[1]) == first_mo}
    if freq == "random":
        return None  # no month is ever "missing" — all blanks are optional
    return set(months)


@app.route("/api/coverage-matrix", methods=["GET"])
def get_coverage_matrix():
    today = datetime.now().date()
    try:
        year = int(request.args.get("year", today.year))
    except (ValueError, TypeError):
        year = today.year
    months = [f"{year}-{mo:02d}" for mo in range(1, 13)]

    # Load active reference rows
    ref_rows = [
        r for r in read_csv_as_dicts(str(Path("data/reference_table.csv")))
        if (r.get("active_status") or "active").strip().lower() == "active"
    ]

    # Load master log — index by (account_number, year_month) → list of records
    from collections import defaultdict
    log_index: dict = defaultdict(list)
    all_docs_by_acct: dict = defaultdict(list)
    for rec in get_all_master_records():
        acct = (rec.get("account_number") or "").strip()
        ym   = (rec.get("year_month")     or "").strip()
        if acct:
            all_docs_by_acct[acct].append(rec)
            if ym:
                log_index[(acct, ym)].append(rec)

    today_str = today.isoformat()

    def _doc_cell(acct: str, ym: str) -> dict:
        """Return cell dict for a month that HAS at least one document."""
        matches = log_index[(acct, ym)]
        rec = sorted(matches, key=lambda r: r.get("document_date", ""), reverse=True)[0]
        status = (rec.get("payment_status") or "unpaid").strip().lower()
        due    = (rec.get("due_date") or "").strip()
        is_overdue = bool(due and len(due) == 10 and due < today_str and status in ("unpaid", ""))
        cell_status = "filed" if status == "paid" else ("overdue" if is_overdue else "attention")
        return {
            "status": cell_status,
            "doc": {
                "row_index":          rec.get("row_index"),
                "vendor_name":        rec.get("vendor_name", ""),
                "amount_due":         rec.get("amount_due", ""),
                "document_date":      rec.get("document_date", ""),
                "due_date":           rec.get("due_date", ""),
                "payment_status":     rec.get("payment_status", ""),
                "final_storage_path": rec.get("final_storage_path", ""),
            },
        }

    property_order = ["1423 Central Ave", "3715 Lincoln Ave", "3047 Sea Marsh Rd", "Business"]
    rows_out = []

    for ref in ref_rows:
        acct          = (ref.get("account_number") or "").strip()
        property_     = (ref.get("property") or "").strip()
        billing_freq  = (ref.get("billing_frequency") or "").strip()
        acct_all_docs = all_docs_by_acct.get(acct, [])
        expected      = _billing_expected_months(billing_freq, months, acct_all_docs)

        cells = {}
        filed_count    = 0
        expected_count = 0
        missing_count  = 0

        for m in months:
            has_doc = bool(log_index.get((acct, m)))
            if has_doc:
                cells[m] = _doc_cell(acct, m)
                filed_count += 1
            else:
                # No document this month — determine display status
                if expected is None:
                    # Random frequency: show optional dash, never flagged
                    cells[m] = {"status": "optional", "doc": None}
                elif m in expected:
                    cells[m] = {"status": "missing", "doc": None}
                    expected_count += 1
                    missing_count  += 1
                else:
                    cells[m] = {"status": "not_expected", "doc": None}

            if expected is not None and m in expected and has_doc:
                expected_count += 1

        rows_out.append({
            "vendor_name":      ref.get("vendor_name", ""),
            "account_number":   acct,
            "vendor_category":  ref.get("vendor_category", ""),
            "billing_frequency": billing_freq,
            "property":         property_,
            "unit":             ref.get("unit", ""),
            "cells":            cells,
            "filed_count":      filed_count,
            "expected_count":   expected_count,
            "missing_count":    missing_count,
        })

    prop_rank = {p: i for i, p in enumerate(property_order)}
    rows_out.sort(key=lambda r: (prop_rank.get(r["property"], 99), r["vendor_name"].lower()))

    total_expected = sum(r["expected_count"] for r in rows_out)
    total_filed    = sum(r["filed_count"]    for r in rows_out)
    total_missing  = sum(r["missing_count"]  for r in rows_out)

    return jsonify({
        "year":   year,
        "months": months,
        "rows":   rows_out,
        "summary": {
            "total_expected": total_expected,
            "total_filed":    total_filed,
            "total_missing":  total_missing,
        },
    })


# ================================================================
# ROUTES — Filing path helper
# ================================================================

def _compute_path_dict(data: dict) -> dict:
    """
    Shared helper: build filing folder + filename from a data dict.
    Accepts both review-modal field names (bill_date, vendor_name_normalized)
    and CSV field names (document_date, vendor_name).
    Returns dict with folder, filename, full_path, exists.
    """
    adapted = {
        "property":               data.get("property", ""),
        "vendor_category":        data.get("vendor_category", "") or "other",
        "document_type":          data.get("document_type", ""),
        "bill_date":              data.get("bill_date", "") or data.get("document_date", ""),
        "vendor_name_normalized": (
            data.get("vendor_name_normalized", "")
            or data.get("vendor_name", "")
        ),
        "vendor_name_raw":        data.get("vendor_name_raw", ""),
        "vendor_name":            data.get("vendor_name", ""),
        "unit":                   data.get("unit", ""),
        "amount_due":             data.get("amount_due", ""),
        "account_number":         data.get("account_number", ""),
    }
    source_file = data.get("source_file", "document.pdf")
    folder  = build_filing_path(adapted)
    fname   = build_filename(adapted, source_file)
    full    = folder / fname
    return {
        "folder":    str(folder).replace("\\", "/"),
        "filename":  fname,
        "full_path": str(full).replace("\\", "/"),
        "exists":    full.exists(),
    }


@app.route("/api/compute-filing-path", methods=["POST"])
def compute_filing_path_endpoint():
    """Used by the edit modal (CSV field names) to show FROM/TO before moving a file."""
    data   = request.get_json() or {}
    result = _compute_path_dict(data)
    return jsonify(result)


@app.route("/api/preview-filing-path", methods=["POST"])
def preview_filing_path_endpoint():
    """Used by the review modal (Claude result field names) to show live filing path preview."""
    data   = request.get_json() or {}
    result = _compute_path_dict(data)
    return jsonify(result)


# ================================================================
# STARTUP
# ================================================================

def _start_file_watcher():
    """
    Background thread that watches D:/Scans/Incoming for new files.
    When a new file is detected it is added to the in-memory queue as
    "pending" so it appears on the dashboard.  No Claude processing is
    triggered automatically — the user must click Process (or Process All)
    on the card to start analysis.
    """
    try:
        from startup.folder_initializer import ensure_required_folders
        from startup.startup_diagnostics import run_startup_diagnostics
        from intake.scan_intake_watcher import ScanIntakeWatcher

        ensure_required_folders()
        run_startup_diagnostics()

        def _enqueue_pending(job):
            filename = job.filename
            path_str = str(job.source_path)
            with _queue_lock:
                if filename not in queue:
                    queue[filename] = {
                        "id":         filename,
                        "filename":   filename,
                        "path":       path_str,
                        "status":     "pending",
                        "result":     None,
                        "validation": None,
                        "error":      None,
                        "added":      datetime.now().strftime("%H:%M:%S"),
                    }
            log_activity(f"New file detected: {filename}", "info")
            print(f"[Watcher] New file queued as pending: {filename}")

        watcher = ScanIntakeWatcher()
        watcher.run_forever(_enqueue_pending)
    except Exception as e:
        print(f"[Watcher] Failed to start background watcher: {e}")


if __name__ == "__main__":
    initialize_csv_files()
    from startup.folder_initializer import ensure_required_folders
    ensure_required_folders()
    log_activity("Scanner MVP started", "info")

    # File-watcher is disabled — files are only queued when the user
    # explicitly browses to them and clicks "Add To Queue".
    # _watcher_thread = threading.Thread(target=_start_file_watcher, daemon=True)
    # _watcher_thread.start()
    # print("[Watcher] Background file watcher started (watching D:/Scans/Incoming)")

    # Google Drive: start background sync worker + startup sync
    drive_sync.start_worker()
    drive_sync.startup_sync()

    # Daily backup: run once per day on first startup of the day
    if should_run_daily_backup():
        _backup_thread = threading.Thread(
            target=do_daily_backup, args=(drive_sync,), daemon=True
        )
        _backup_thread.start()
        print("[Backup] Daily snapshot started")

    print("\nRegistered Flask routes:")
    for rule in sorted(app.url_map.iter_rules(), key=lambda r: r.rule):
        methods = ",".join(sorted(rule.methods - {"HEAD", "OPTIONS"}))
        print(f"  [{methods:6}] {rule.rule}")
    print()

    print("\n" + "=" * 50)
    print("  Scanner MVP is running!")
    print("  Open your browser to: http://localhost:5000")
    print("=" * 50 + "\n")
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)
