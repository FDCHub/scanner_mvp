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
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

from claude_analyzer import analyze_document, get_overall_confidence
from vendor_normalizer import normalize_vendor_name
from vendor_profile_store import load_vendor_profiles
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

INCOMING_DIR = Path("D:/Scans/Incoming")
WORKING_DIR  = Path("D:/Scans/Working")
FILED_DIR    = Path("D:/Scans/Filed")
ERROR_DIR    = Path("D:/Scans/Error")
DUP_DIR      = Path("D:/Scans/Duplicates")
CONFIG_PATH  = Path(__file__).parent / "config.json"

for folder in [INCOMING_DIR, WORKING_DIR, FILED_DIR, ERROR_DIR, DUP_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

queue        = {}
activity_log = []


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
    prop     = result.get("property", "Unknown Property").strip() or "Unknown Property"
    doc_type = result.get("document_type", "other").strip() or "other"
    year     = (result.get("bill_date") or datetime.now().strftime("%Y-%m-%d"))[:4]
    return FILED_DIR / prop / doc_type / year


def build_filename(result: dict, original_name: str) -> str:
    date   = result.get("bill_date", "")[:10] or datetime.now().strftime("%Y-%m-%d")
    vendor = result.get("vendor_name_normalized", "") or result.get("vendor_name_raw", "unknown")
    vendor = "".join(c for c in vendor if c.isalnum() or c in " _-")[:30].strip().replace(" ", "_")
    amount = result.get("amount_due", "")
    amount_str = f"_${amount}" if amount else ""
    suffix = Path(original_name).suffix or ".pdf"
    return f"{date}_{vendor}{amount_str}{suffix}"


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
        if f.name not in queue:
            queue[f.name] = {
                "id": f.name, "filename": f.name, "path": str(f),
                "status": "pending", "result": None, "validation": None,
                "error": None, "added": datetime.now().strftime("%H:%M:%S"),
            }
    current_names = {f.name for f in incoming_files}
    for k in [k for k in queue if queue[k]["status"] == "pending"
              and k not in current_names]:
        del queue[k]
    return jsonify(list(queue.values()))


@app.route("/api/queue/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400
    f.save(str(INCOMING_DIR / f.filename))
    log_activity(f"Uploaded: {f.filename}", "info")
    return jsonify({"ok": True, "filename": f.filename})


@app.route("/api/queue/remove/<filename>", methods=["DELETE"])
def remove_from_queue(filename):
    path = INCOMING_DIR / filename
    if path.exists():
        path.unlink()
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
            queue[filename]["status"] = "processing"

            yield from emit("intake", 10, "Grabbing file from Incoming folder", "running")
            working_path = WORKING_DIR / filename
            shutil.copy(str(file_path), str(working_path))
            yield from emit("intake", 10, "File moved to Working folder", "success", filename)

            yield from emit("claude_send", 25, "Sending document to Claude API", "running",
                            "Uploading document image to Anthropic")

            yield from emit("claude_analyze", 50,
                            "Claude reading and extracting data...", "running",
                            "Analyzing document image")
            result     = analyze_document(working_path)
            vendor_out = result.get("vendor_name_normalized") or result.get("vendor_name_raw", "?")
            yield from emit("claude_analyze", 50, "Claude extraction complete", "success",
                            f"Vendor: {vendor_out} | Amount: ${result.get('amount_due','?')} | Date: {result.get('bill_date','?')}")

            yield from emit("claude_results", 60, "Normalizing extracted data", "running")
            if result.get("vendor_name_raw"):
                result["vendor_name_normalized"] = normalize_vendor_name(result["vendor_name_raw"])
            claude_confidence = get_overall_confidence(result)
            result["_confidence"]   = claude_confidence
            result["_filename"]     = filename
            result["_working_path"] = str(working_path)
            yield from emit("claude_results", 60, "Data normalized", "success",
                            f"Claude overall confidence: {claude_confidence.upper()}")

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

            queue[filename]["status"]     = "review"
            queue[filename]["result"]     = result
            queue[filename]["validation"] = validation
            overall = validation.get("overall", {})
            yield from emit("review", 100, "Ready for your review", "success",
                            overall.get("label", ""),
                            data={"result": result, "validation": validation,
                                  "confidence": claude_confidence, "filename": filename})

        except Exception as e:
            queue[filename]["status"] = "error"
            queue[filename]["error"]  = str(e)
            log_activity(f"Error: {filename} — {e}", "error")
            yield from emit("error", 100, "Processing failed", "error", str(e))

    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/process/all", methods=["POST"])
def process_all():
    pending = [v for v in queue.values() if v["status"] == "pending"]
    if not pending:
        return jsonify({"message": "No pending files"}), 200

    def run_all():
        for item in pending:
            try:
                working_path = WORKING_DIR / item["filename"]
                shutil.copy(item["path"], str(working_path))
                result = analyze_document(working_path)
                if result.get("vendor_name_raw"):
                    result["vendor_name_normalized"] = normalize_vendor_name(
                        result["vendor_name_raw"])
                confidence = get_overall_confidence(result)
                result["_confidence"]   = confidence
                result["_filename"]     = item["filename"]
                result["_working_path"] = str(working_path)
                vendor_profiles = load_vendor_profiles()
                validation = run_validation(result, vendor_profiles)
                queue[item["filename"]]["status"]     = "review"
                queue[item["filename"]]["result"]     = result
                queue[item["filename"]]["validation"] = validation
                log_activity(
                    f"Analyzed: {item['filename']} — "
                    f"{result.get('vendor_name_normalized','?')} ({confidence})", "info")
            except Exception as e:
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
    item = queue.get(filename)
    if not item:
        return jsonify({"error": "File not in queue"}), 404

    working_path = Path(item.get("result", {}).get("_working_path", ""))
    try:
        dest_folder = build_filing_path(data)
        dest_folder.mkdir(parents=True, exist_ok=True)
        new_filename = build_filename(data, filename)
        dest_path    = dest_folder / new_filename

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
        queue[filename]["status"] = "error"
        log_activity(f"Filing error for {filename}: {e}", "error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/reject/<filename>", methods=["POST"])
def reject_document(filename):
    item         = queue.get(filename, {})
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

    queue.pop(filename, None)
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


@app.route("/api/master-log", methods=["GET"])
def get_master_log():
    rows = get_all_master_records()
    rows.reverse()  # Most recent first
    return jsonify(rows)


@app.route("/api/master-log/<int:index>", methods=["GET"])
def get_master_log_record(index):
    """Get a single master log record by index."""
    # Since we reverse for display, translate display index to real index
    rows  = read_csv_as_dicts(MASTER_LOG_CSV)
    total = len(rows)
    real_index = total - 1 - index
    record = get_record_by_index(real_index)
    if record is None:
        return jsonify({"error": "Record not found"}), 404
    record["_display_index"] = index
    record["_real_index"]    = real_index
    return jsonify(record)


@app.route("/api/master-log/<int:index>", methods=["PUT"])
def update_master_log_record(index):
    """Update a single master log record by display index."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    rows       = read_csv_as_dicts(MASTER_LOG_CSV)
    total      = len(rows)
    real_index = total - 1 - index
    updated = update_record_by_index(real_index, data)
    if not updated:
        return jsonify({"error": "Record not found"}), 404
    record = rows[real_index]
    log_activity(
        f"Record updated: {record.get('vendor_name','?')} "
        f"{record.get('document_date','?')}", "info")
    return jsonify({"ok": True})


@app.route("/api/master-log/<int:index>", methods=["DELETE"])
def delete_master_log_record_route(index):
    """
    Delete a master log record and its filed PDF.
    Body: { "delete_file": true }
    """
    data         = request.get_json() or {}
    delete_file  = data.get("delete_file", True)
    rows         = read_csv_as_dicts(MASTER_LOG_CSV)
    total        = len(rows)
    real_index   = total - 1 - index

    # Grab info before deleting
    record = get_record_by_index(real_index)
    if record is None:
        return jsonify({"error": "Record not found"}), 404

    vendor       = record.get("vendor_name", "?")
    date         = record.get("document_date", "?")
    filed_path   = record.get("final_storage_path", "")

    success, _ = delete_record_by_index(real_index)
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
# STARTUP
# ================================================================

if __name__ == "__main__":
    initialize_csv_files()
    log_activity("Scanner MVP started", "info")
    print("\n" + "=" * 50)
    print("  Scanner MVP is running!")
    print("  Open your browser to: http://localhost:5000")
    print("=" * 50 + "\n")
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)
