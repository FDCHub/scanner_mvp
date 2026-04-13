"""
app.py
------
Flask web server for scanner_mvp dashboard.
Run with:  python app.py
Open:      http://localhost:5000
"""

import os
import json
import shutil
import threading
import time
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

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
        return "handyman services"
    if doc_type == "receipt":
        return "supplier/store"
    if doc_type == "invoice":
        return "service provider"
    if any(kw in vendor for kw in utility_kw):
        return "utility"
    if doc_type == "bill":
        return "utility"
    return "other"

INCOMING_DIR      = Path("D:/Scans/Incoming")
WORKING_DIR       = Path("D:/Scans/Working")
FILED_DIR         = Path("D:/Scans/Filed")
ERROR_DIR         = Path("D:/Scans/Error")
DUP_DIR           = Path("D:/Scans/Duplicates")
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


def build_filing_path(result: dict) -> Path:
    prop     = (result.get("property") or "Unknown Property").strip()
    # Always use vendor_category as the subfolder — never fall back to document_type
    category = (result.get("vendor_category") or "other").strip()
    return PROPERTY_DOCS_DIR / prop / category


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
    incoming_files = (
        list(INCOMING_DIR.glob("*.pdf")) +
        list(INCOMING_DIR.glob("*.jpg")) +
        list(INCOMING_DIR.glob("*.jpeg")) +
        list(INCOMING_DIR.glob("*.png"))
    )

    for f in incoming_files:
        # Quick lock-free check — skip files the watcher already promoted to review.
        # We re-validate inside the lock before any mutation (see below).
        with _queue_lock:
            current_entry = queue.get(f.name)

        if current_entry and current_entry.get("status") == "review":
            continue  # already handled; nothing to do

        # Heavy I/O happens OUTSIDE the lock to avoid blocking other threads.
        sidecar_data, validation = _load_sidecar(f)

        with _queue_lock:
            entry = queue.get(f.name)
            if entry is None:
                # Brand-new file — either it came in as a pre-analyzed sidecar
                # pair or it's a plain pending document.
                queue[f.name] = {
                    "id":         f.name,
                    "filename":   f.name,
                    "path":       str(f),
                    "status":     "review"  if sidecar_data else "pending",
                    "result":     sidecar_data,
                    "validation": validation if sidecar_data else None,
                    "error":      None,
                    "added":      datetime.now().strftime("%H:%M:%S"),
                }
                if sidecar_data:
                    log_activity(f"Pre-analyzed document ready for review: {f.name}", "info")
            elif entry["status"] != "review" and sidecar_data is not None:
                # File was already in queue (pending / processing / error / filed)
                # but a sidecar appeared — promote it.  Covers:
                #   • pending:    watcher processed faster than SSE
                #   • processing: SSE was interrupted
                #   • error:      previous failure, watcher retry succeeded
                #   • filed:      document re-queued for re-review
                entry.update({
                    "status":     "review",
                    "result":     sidecar_data,
                    "validation": validation,
                    "error":      None,
                })
                log_activity(f"Sidecar detected — upgraded to review: {f.name}", "info")

    current_names = {f.name for f in incoming_files}
    with _queue_lock:
        stale = [
            k for k in list(queue.keys())
            if queue[k]["status"] in ("pending", "error")
            and k not in current_names
        ]
        for k in stale:
            del queue[k]
        result = list(queue.values())
    print(f"[Queue] get_queue returning {len(result)} item(s): {[e['filename'] + '/' + e['status'] for e in result]}")
    return jsonify(result)


@app.route("/api/queue/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400
    # Prevent re-processing a file that was already filed
    existing = get_all_master_records()
    if any(r.get("source_file") == f.filename for r in existing):
        log_activity(f"Skipped duplicate source: {f.filename}", "warning")
        return jsonify({"ok": False, "duplicate_source": True, "filename": f.filename}), 409
    f.save(str(INCOMING_DIR / f.filename))
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
    shutil.copy(str(p), str(dest))
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

@app.route("/api/process/<filename>", methods=["POST"])
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
            "vendor_name":     master_record["vendor_name"],
            "account_number":  master_record["account_number"],
            "vendor_category": master_record["vendor_category"],
            "property":        master_record["property"],
            "unit":            master_record["unit"],
            "service_address": master_record["service_address"],
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
    """Get a single master log record by its CSV row_index."""
    record = get_record_by_index(index)
    if record is None:
        return jsonify({"error": "Record not found"}), 404
    record["_row_index"] = index
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
    move_file       = data.pop("move_file", False)
    new_filing_path = data.pop("new_filing_path", "")

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
    Delete a master log record and its filed PDF.
    Body: { "delete_file": true }
    """
    data         = request.get_json() or {}
    delete_file  = data.get("delete_file", True)

    # Grab info before deleting
    record = get_record_by_index(index)
    if record is None:
        return jsonify({"error": "Record not found"}), 404

    vendor       = record.get("vendor_name", "?")
    date         = record.get("document_date", "?")
    filed_path   = record.get("final_storage_path", "")

    success, _ = delete_record_by_index(index)
    if not success:
        return jsonify({"error": "Could not delete record"}), 500

    file_deleted = False
    file_error   = ""
    if delete_file and filed_path:
        try:
            p = Path(filed_path)
            if p.exists():
                p.unlink()
                file_deleted = True
            else:
                file_error = "File not found at stored path"
        except Exception as e:
            file_error = str(e)

    log_activity(
        f"Deleted: {vendor} {date}"
        + (" + file" if file_deleted else ""), "warning")

    return jsonify({
        "ok":           True,
        "file_deleted": file_deleted,
        "file_error":   file_error,
        "filed_path":   filed_path,
    })





# ================================================================
# ROUTES — Folder browser
# ================================================================

@app.route("/api/browse-folder", methods=["GET"])
def browse_folder():
    import os as _os
    path_str = request.args.get("path", "D:/Scans/Incoming")
    # Normalise: replace all backslashes and strip surrounding quotes/whitespace
    path_str = path_str.strip().strip('"\'').replace("\\", "/")
    if not path_str:
        path_str = "D:/Scans/Incoming"
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
        for child in sorted(p.iterdir(), key=lambda x: (x.is_file(), x.name.lower())):
            if child.name.startswith("."):
                continue
            items.append({
                "name": child.name,
                "type": "dir" if child.is_dir() else "file",
                "path": str(child).replace("\\", "/"),
                "ext":  child.suffix.lower() if child.is_file() else "",
            })
        return jsonify({"path": path_str, "items": items})
    except PermissionError:
        return jsonify({"error": f"Permission denied: {path_str}", "path": path_str, "items": []}), 200
    except Exception as e:
        log_activity(f"browse-folder error ({path_str}): {e}", "warning")
        return jsonify({"error": str(e), "path": path_str, "items": []}), 200


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
        "property":              data.get("property", ""),
        "vendor_category":       data.get("vendor_category", "") or "other",
        "document_type":         data.get("document_type", ""),
        "bill_date":             data.get("bill_date", "") or data.get("document_date", ""),
        "vendor_name_normalized": (
            data.get("vendor_name_normalized", "")
            or data.get("vendor_name", "")
        ),
        "vendor_name_raw":       data.get("vendor_name_raw", ""),
        "unit":                  data.get("unit", ""),
        "amount_due":            data.get("amount_due", ""),
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
    Background thread that watches D:/Scans/Incoming for new PDFs and
    auto-processes them (OCR → Claude → sidecar → web review).
    Previously required running main.py separately; now embedded here.
    """
    try:
        from main import handle_scan_job
        from startup.folder_initializer import ensure_required_folders
        from startup.startup_diagnostics import run_startup_diagnostics
        from intake.scan_intake_watcher import ScanIntakeWatcher
        from modules.reference_matcher import load_reference_table as _load_ref

        ensure_required_folders()
        run_startup_diagnostics()

        watcher = ScanIntakeWatcher()
        watcher.run_forever(
            lambda job: handle_scan_job(
                job,
                lambda: _load_ref("data/reference_table.csv"),
                on_review=_notify_review,
            )
        )
    except Exception as e:
        print(f"[Watcher] Failed to start background watcher: {e}")


if __name__ == "__main__":
    initialize_csv_files()
    log_activity("Scanner MVP started", "info")

    # Start the file-watcher as a background daemon thread so it runs
    # alongside the Flask server — no need to run main.py separately.
    _watcher_thread = threading.Thread(target=_start_file_watcher, daemon=True)
    _watcher_thread.start()
    print("[Watcher] Background file watcher started (watching D:/Scans/Incoming)")

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
