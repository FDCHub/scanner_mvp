"""
csv_manager.py
--------------
Manages the flat file database (CSV) for scanner_mvp.

Master log schema is the agreed final version:
  - Redundant boolean flags removed (contains_prior_balance, etc.)
  - renamed_working_path removed
  - expense_amount_excluding_arrears removed
  - expense_amount_current_period renamed to current_period_amount
  - year_month added for easy pivot table slicing
  - payment_status added for outstanding balance filtering

Duplicate detection uses Claude's extracted content fields:
  vendor_name + account_number + document_date + amount_due
  (not filenames — filenames are unreliable)

Reject/delete: delete_master_log_record() removes a bad record
  written during a failed processing attempt so it doesn't block
  a corrected re-scan from being filed.
"""

import os
import csv
import shutil
from datetime import datetime

# === FILE PATHS ===
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
REFERENCE_CSV  = os.path.join(BASE_DIR, "data", "reference_table.csv")
MASTER_LOG_CSV = os.path.join(BASE_DIR, "data", "document_master_log.csv")
BACKUP_DIR     = os.path.join(BASE_DIR, "csv_backups")


# === SCHEMA ===

REFERENCE_FIELDS = [
    "vendor_name",
    "vendor_normalized_name",
    "account_number",
    "vendor_category",
    "document_type_default",
    "property",
    "unit",
    "service_address",
    "property_folder_name",
    "document_subfolder",
    "account_role",
    "utility_type",
    "billing_frequency",
    "active_status",
    "notes",
    "last_updated",
]

MASTER_LOG_FIELDS = [
    # ── When & what ──────────────────────────────────────────
    "timestamp",                  # When this record was created
    "document_type",              # bill / invoice / receipt
    "year_month",                 # e.g. 2026-03  (for pivot slicing)

    # ── Who ──────────────────────────────────────────────────
    "vendor_name",                # Normalized vendor name
    "vendor_category",            # utility / service provider / supplier etc.
    "account_number",             # Account number on document

    # ── Where ────────────────────────────────────────────────
    "property",                   # e.g. 1423 Central Ave
    "unit",                       # e.g. B, G, HSE
    "service_address",            # Address printed on document

    # ── When (document dates) ─────────────────────────────────
    "document_date",              # Date on the document (YYYY-MM-DD)
    "service_period_start",       # Billing period start
    "service_period_end",         # Billing period end
    "due_date",                   # Payment due date

    # ── Money ────────────────────────────────────────────────
    "previous_balance",           # Carried-over unpaid balance
    "payments_received",          # Payments applied this period
    "credits_adjustments",        # Credits or adjustments
    "late_fees",                  # Late fee charges
    "penalties",                  # Penalty charges
    "taxes_fees",                 # Taxes and fees
    "current_charges",            # Charges for current period only
    "current_period_amount",      # True expense this period (excl. prior balance)
    "amount_due",                 # Total amount due on document

    # ── Payment status ────────────────────────────────────────
    "payment_status",             # paid / unpaid / overdue

    # ── Accounting review ────────────────────────────────────
    "review_required_accounting", # yes / no flag for accountant
    "accounting_notes",           # Free-text notes

    # ── File & system ─────────────────────────────────────────
    "source_file",                # Original filename as received
    "final_storage_path",         # Full path where document was filed
    "confidence_score",           # high / medium / low (Claude overall)
    "claude_used",                # Always True in this version
    "duplicate_check_fields",     # Audit trail of fields used for dedup

    # ── Audit ─────────────────────────────────────────────────
    "manually_edited",            # "yes" if record was manually edited after filing
    "last_edited_timestamp",      # ISO timestamp of last manual edit
]


# === INITIALIZATION ===

def initialize_csv_files():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(REFERENCE_CSV), exist_ok=True)

    if not os.path.exists(REFERENCE_CSV):
        with open(REFERENCE_CSV, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=REFERENCE_FIELDS)
            writer.writeheader()
        print("  [CSV] Created reference_table.csv")

    if not os.path.exists(MASTER_LOG_CSV):
        with open(MASTER_LOG_CSV, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=MASTER_LOG_FIELDS)
            writer.writeheader()
        print("  [CSV] Created document_master_log.csv")


# === BACKUP ===

def backup_csv(file_path: str):
    if not os.path.exists(file_path):
        return
    timestamp       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename        = os.path.basename(file_path)
    backup_path     = os.path.join(BACKUP_DIR, f"{filename}.{timestamp}.bak")
    shutil.copy(file_path, backup_path)


# === READ / WRITE ===

def read_csv_as_dicts(file_path: str) -> list[dict]:
    if not os.path.exists(file_path):
        return []
    with open(file_path, mode="r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # DictReader stores extra columns (beyond the header) under a None key.
    # Strip it so json.dumps(sort_keys=True) doesn't crash on None < "string".
    for row in rows:
        row.pop(None, None)
    return rows


def write_csv(file_path: str, fieldnames: list, rows: list[dict]):
    backup_csv(file_path)
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def append_csv(file_path: str, fieldnames: list, row: dict):
    backup_csv(file_path)
    with open(file_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow(row)


# === NORMALIZATION ===

def normalize_value(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_key(value) -> str:
    return normalize_value(value).lower()


def normalize_amount(value) -> str:
    """
    Normalize a dollar amount for reliable comparison.
    '$1,076.80', '1076.8', '1076.80' all compare equal.
    """
    if not value:
        return ""
    cleaned = str(value).strip().replace("$", "").replace(",", "").strip()
    try:
        return str(round(float(cleaned), 2))
    except ValueError:
        return cleaned.lower()


def derive_year_month(date_str: str) -> str:
    """Extract YYYY-MM from a YYYY-MM-DD date string."""
    if date_str and len(date_str) >= 7:
        return date_str[:7]
    return datetime.now().strftime("%Y-%m")


# === DUPLICATE DETECTION ===

def is_master_log_duplicate(existing_row: dict, new_row: dict) -> tuple[bool, str]:
    """
    Returns (is_duplicate, reason_string).

    Duplicate = all four content fields match:
      vendor_name + account_number + document_date + amount_due

    All four must be non-empty in both records for the check to fire.
    Conservative by design: missing data = allow through rather than block.
    """
    vendor_new  = normalize_key(new_row.get("vendor_name", ""))
    account_new = normalize_key(new_row.get("account_number", ""))
    date_new    = normalize_key(new_row.get("document_date", ""))
    amount_new  = normalize_amount(new_row.get("amount_due", ""))

    if not all([vendor_new, account_new, date_new, amount_new]):
        return False, "insufficient_data_to_check"

    vendor_ex  = normalize_key(existing_row.get("vendor_name", ""))
    account_ex = normalize_key(existing_row.get("account_number", ""))
    date_ex    = normalize_key(existing_row.get("document_date", ""))
    amount_ex  = normalize_amount(existing_row.get("amount_due", ""))

    if (vendor_new == vendor_ex and account_new == account_ex and
            date_new == date_ex and amount_new == amount_ex):
        reason = (f"vendor='{vendor_new}' | account='{account_new}' | "
                  f"date='{date_new}' | amount='{amount_new}'")
        return True, reason

    return False, ""


def check_for_duplicate(record: dict) -> tuple[bool, str, dict]:
    """
    Check a new record against the full master log.
    Returns (is_duplicate, reason, matching_existing_row).
    Call this BEFORE filing so the dashboard can warn the user.
    """
    rows = read_csv_as_dicts(MASTER_LOG_CSV)
    for existing_row in rows:
        is_dup, reason = is_master_log_duplicate(existing_row, record)
        if is_dup:
            return True, reason, existing_row
    return False, "", {}


# === DELETE (used by reject flow) ===

def delete_master_log_record(record: dict) -> bool:
    """
    Remove a record from the master log that matches the given
    vendor_name + account_number + document_date + amount_due.

    Called when a user rejects a document after it was partially
    processed — ensures a bad record doesn't block a corrected re-scan.

    Returns True if a record was found and removed, False otherwise.
    """
    rows   = read_csv_as_dicts(MASTER_LOG_CSV)
    before = len(rows)
    kept   = []

    for row in rows:
        is_dup, _ = is_master_log_duplicate(row, record)
        if is_dup:
            print(f"  [CSV] Deleting bad record: {row.get('vendor_name')} | "
                  f"{row.get('document_date')} | ${row.get('amount_due')}")
        else:
            kept.append(row)

    if len(kept) < before:
        write_csv(MASTER_LOG_CSV, MASTER_LOG_FIELDS, kept)
        return True

    return False


# === REFERENCE TABLE ===

# Categories where account_number is absent — keyed by vendor+property+unit instead
_ACCOUNT_OPTIONAL_CATEGORIES: frozenset[str] = frozenset({
    "handyman services", "handyman", "insurance", "tax", "hoa",
})


def is_reference_duplicate(existing_row: dict, new_row: dict) -> bool:
    ex_cat  = normalize_key(existing_row.get("vendor_category", ""))
    new_cat = normalize_key(new_row.get("vendor_category", ""))

    # For no-account categories: key is vendor + property + unit
    if ex_cat in _ACCOUNT_OPTIONAL_CATEGORIES or new_cat in _ACCOUNT_OPTIONAL_CATEGORIES:
        return (
            normalize_key(existing_row.get("vendor_name", "")) == normalize_key(new_row.get("vendor_name", "")) and
            normalize_key(existing_row.get("property", "")) == normalize_key(new_row.get("property", "")) and
            normalize_key(existing_row.get("unit", "")) == normalize_key(new_row.get("unit", ""))
        )

    # Standard account-number based dedup
    ex_account  = normalize_key(existing_row.get("account_number", ""))
    new_account = normalize_key(new_row.get("account_number", ""))
    if not (ex_account and new_account):
        return False
    return (ex_account == new_account and
            normalize_key(existing_row.get("vendor_name", "")) == normalize_key(new_row.get("vendor_name", "")) and
            normalize_key(existing_row.get("property", "")) == normalize_key(new_row.get("property", "")) and
            normalize_key(existing_row.get("unit", "")) == normalize_key(new_row.get("unit", "")))


def build_reference_row(record: dict) -> dict:
    return {
        "vendor_name":            normalize_value(record.get("vendor_name")),
        "vendor_normalized_name": normalize_value(record.get("vendor_name")),
        "account_number":         normalize_value(record.get("account_number")),
        "vendor_category":        normalize_value(record.get("vendor_category")),
        "document_type_default":  "",
        "property":               normalize_value(record.get("property")),
        "unit":                   normalize_value(record.get("unit")),
        "service_address":        normalize_value(record.get("service_address")),
        "property_folder_name":   normalize_value(record.get("property")),
        "document_subfolder":     normalize_value(record.get("vendor_category")),
        "account_role":           "",
        "utility_type":           "",
        "billing_frequency":      "",
        "active_status":          "active",
        "notes":                  "",
        "last_updated":           datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def upsert_reference_record(record: dict):
    new_row = build_reference_row(record)
    rows    = read_csv_as_dicts(REFERENCE_CSV)
    updated = False
    for idx, existing_row in enumerate(rows):
        if is_reference_duplicate(existing_row, new_row):
            rows[idx] = {**existing_row, **new_row}
            updated   = True
            break
    if not updated:
        rows.append(new_row)
    write_csv(REFERENCE_CSV, REFERENCE_FIELDS, rows)


# === MASTER LOG — APPEND ===

def build_master_log_row(record: dict) -> dict:
    doc_date = normalize_value(record.get("document_date"))
    return {
        "timestamp":                  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "document_type":              normalize_value(record.get("document_type")),
        "year_month":                 derive_year_month(doc_date),
        "vendor_name":                normalize_value(record.get("vendor_name")),
        "vendor_category":            normalize_value(record.get("vendor_category")),
        "account_number":             normalize_value(record.get("account_number")),
        "property":                   normalize_value(record.get("property")),
        "unit":                       normalize_value(record.get("unit")),
        "service_address":            normalize_value(record.get("service_address")),
        "document_date":              doc_date,
        "service_period_start":       normalize_value(record.get("service_period_start", "")),
        "service_period_end":         normalize_value(record.get("service_period_end", "")),
        "due_date":                   normalize_value(record.get("due_date")),
        "previous_balance":           normalize_value(record.get("previous_balance", "")),
        "payments_received":          normalize_value(record.get("payments_received", "")),
        "credits_adjustments":        normalize_value(record.get("credits_adjustments", "")),
        "late_fees":                  normalize_value(record.get("late_fees", "")),
        "penalties":                  normalize_value(record.get("penalties", "")),
        "taxes_fees":                 normalize_value(record.get("taxes_fees", "")),
        "current_charges":            normalize_value(record.get("current_charges", "")),
        "current_period_amount":      normalize_value(record.get("current_period_amount", "")),
        "amount_due":                 normalize_value(record.get("amount_due")),
        "payment_status":             normalize_value(record.get("payment_status", "unpaid")),
        "review_required_accounting": normalize_value(record.get("review_required_accounting", "")),
        "accounting_notes":           normalize_value(record.get("accounting_notes", "")),
        "source_file":                normalize_value(record.get("source_file")),
        "final_storage_path":         normalize_value(record.get("final_storage_path")),
        "confidence_score":           normalize_value(record.get("confidence_score")),
        "claude_used":                "True",
        "duplicate_check_fields":     "vendor_name|account_number|document_date|amount_due",
        "manually_edited":            "",
        "last_edited_timestamp":      "",
    }


def get_all_master_records() -> list[dict]:
    """Return all master log records with a row_index field added."""
    rows = read_csv_as_dicts(MASTER_LOG_CSV)
    for i, row in enumerate(rows):
        row["row_index"] = i
    return rows


def get_record_by_index(index: int) -> dict | None:
    """Return a single master log record by its row index."""
    rows = read_csv_as_dicts(MASTER_LOG_CSV)
    if 0 <= index < len(rows):
        row = rows[index].copy()
        row["row_index"] = index
        return row
    return None


def update_record_by_index(index: int, updates: dict, skip_audit: bool = False) -> bool:
    """
    Update a single master log record by row index.
    Only updatable fields are written — system fields are protected.
    Returns True if updated, False if index out of range.
    """
    PROTECTED_FIELDS = {
        "timestamp", "source_file",
        "confidence_score", "claude_used", "duplicate_check_fields",
    }
    rows = read_csv_as_dicts(MASTER_LOG_CSV)
    if not (0 <= index < len(rows)):
        return False

    for key, value in updates.items():
        if key not in PROTECTED_FIELDS and key in MASTER_LOG_FIELDS:
            rows[index][key] = normalize_value(value)

    # Recalculate year_month if document_date changed
    if "document_date" in updates:
        rows[index]["year_month"] = derive_year_month(rows[index]["document_date"])

    # Audit trail — always stamp manual edits unless called internally
    if not skip_audit:
        rows[index]["manually_edited"] = "yes"
        rows[index]["last_edited_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    write_csv(MASTER_LOG_CSV, MASTER_LOG_FIELDS, rows)
    print(f"  [CSV] Record updated at index {index}: "
          f"{rows[index].get('vendor_name')} | {rows[index].get('document_date')}")
    return True


def delete_record_by_index(index: int) -> tuple[bool, str]:
    """
    Delete a single master log record by row index.
    Returns (success, final_storage_path) so caller can also delete the file.
    """
    rows = read_csv_as_dicts(MASTER_LOG_CSV)
    if not (0 <= index < len(rows)):
        return False, ""
    deleted_path = rows[index].get("final_storage_path", "")
    vendor       = rows[index].get("vendor_name", "?")
    date         = rows[index].get("document_date", "?")
    del rows[index]
    write_csv(MASTER_LOG_CSV, MASTER_LOG_FIELDS, rows)
    print(f"  [CSV] Record deleted at index {index}: {vendor} | {date}")
    return True, deleted_path


def append_document_master_record(record: dict) -> tuple[bool, str, dict]:
    """
    Attempt to append a new record to the master log.

    Returns:
        (True,  "",       {})         — written successfully
        (False, reason,   match_row)  — duplicate found, NOT written

    The caller (app.py confirm route) surfaces the reason to the dashboard.
    """
    new_row = build_master_log_row(record)
    is_dup, reason, match_row = check_for_duplicate(new_row)

    if is_dup:
        print(f"  [CSV] Duplicate blocked: {reason}")
        return False, reason, match_row

    append_csv(MASTER_LOG_CSV, MASTER_LOG_FIELDS, new_row)
    print(f"  [CSV] Record written: {new_row['vendor_name']} | "
          f"{new_row['document_date']} | ${new_row['amount_due']}")
    return True, "", {}
