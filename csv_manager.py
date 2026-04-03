import os
import csv
import shutil
from datetime import datetime

# === FILE PATHS ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

REFERENCE_CSV = os.path.join(BASE_DIR, "reference_table.csv")
MASTER_LOG_CSV = os.path.join(BASE_DIR, "document_master_log.csv")
BACKUP_DIR = os.path.join(BASE_DIR, "csv_backups")

# === SCHEMA DEFINITIONS ===
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
    "timestamp",
    "document_type",
    "vendor_name",
    "vendor_category",
    "account_number",
    "property",
    "unit",
    "service_address",
    "document_date",
    "service_period_start",
    "service_period_end",
    "due_date",
    "previous_balance",
    "payments_received",
    "credits_adjustments",
    "late_fees",
    "penalties",
    "taxes_fees",
    "current_charges",
    "amount_due",
    "expense_amount_current_period",
    "expense_amount_excluding_arrears",
    "contains_prior_balance",
    "contains_late_fee",
    "contains_penalty",
    "review_required_accounting",
    "accounting_notes",
    "source_file",
    "renamed_working_path",
    "final_storage_path",
    "confidence_score",
    "chatgpt_used",
]

# === INITIALIZATION ===
def initialize_csv_files():
    os.makedirs(BACKUP_DIR, exist_ok=True)

    if not os.path.exists(REFERENCE_CSV):
        with open(REFERENCE_CSV, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=REFERENCE_FIELDS)
            writer.writeheader()

    if not os.path.exists(MASTER_LOG_CSV):
        with open(MASTER_LOG_CSV, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=MASTER_LOG_FIELDS)
            writer.writeheader()

# === BACKUP SYSTEM ===
def backup_csv(file_path):
    if not os.path.exists(file_path):
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.basename(file_path)
    backup_filename = f"{filename}.{timestamp}.bak"
    backup_path = os.path.join(BACKUP_DIR, backup_filename)

    shutil.copy(file_path, backup_path)

# === READ HELPERS ===
def read_csv_as_dicts(file_path):
    if not os.path.exists(file_path):
        return []

    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

# === WRITE HELPERS ===
def write_csv(file_path, fieldnames, rows):
    backup_csv(file_path)

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def append_csv(file_path, fieldnames, row):
    backup_csv(file_path)

    with open(file_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow(row)

# === NORMALIZATION HELPERS ===
def normalize_value(value):
    if value is None:
        return ""
    return str(value).strip()

def normalize_key(value):
    return normalize_value(value).lower()

# === REFERENCE TABLE LOGIC ===
def is_reference_duplicate(existing_row, new_row):
    existing_account = normalize_key(existing_row.get("account_number"))
    new_account = normalize_key(new_row.get("account_number"))

    existing_vendor = normalize_key(existing_row.get("vendor_name"))
    new_vendor = normalize_key(new_row.get("vendor_name"))

    existing_property = normalize_key(existing_row.get("property"))
    new_property = normalize_key(new_row.get("property"))

    existing_unit = normalize_key(existing_row.get("unit"))
    new_unit = normalize_key(new_row.get("unit"))

    if existing_account and new_account:
        return (
            existing_account == new_account
            and existing_vendor == new_vendor
            and existing_property == new_property
            and existing_unit == new_unit
        )

    return False

def build_reference_row(record):
    return {
        "vendor_name": normalize_value(record.get("vendor_name")),
        "vendor_normalized_name": normalize_value(record.get("vendor_name")),
        "account_number": normalize_value(record.get("account_number")),
        "vendor_category": normalize_value(record.get("vendor_category")),
        "document_type_default": "",
        "property": normalize_value(record.get("property")),
        "unit": normalize_value(record.get("unit")),
        "service_address": normalize_value(record.get("service_address")),
        "property_folder_name": normalize_value(record.get("property")),
        "document_subfolder": normalize_value(record.get("vendor_category")),
        "account_role": "",
        "utility_type": "",
        "billing_frequency": "",
        "active_status": "active",
        "notes": "",
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

def upsert_reference_record(record):
    new_row = build_reference_row(record)
    rows = read_csv_as_dicts(REFERENCE_CSV)

    updated = False

    for idx, existing_row in enumerate(rows):
        if is_reference_duplicate(existing_row, new_row):
            rows[idx] = {
                **existing_row,
                **new_row,
            }
            updated = True
            break

    if not updated:
        rows.append(new_row)

    write_csv(REFERENCE_CSV, REFERENCE_FIELDS, rows)

# === MASTER LOGIC ===
def build_master_log_row(record):
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "document_type": normalize_value(record.get("document_type")),
        "vendor_name": normalize_value(record.get("vendor_name")),
        "vendor_category": normalize_value(record.get("vendor_category")),
        "account_number": normalize_value(record.get("account_number")),
        "property": normalize_value(record.get("property")),
        "unit": normalize_value(record.get("unit")),
        "service_address": normalize_value(record.get("service_address")),
        "document_date": normalize_value(record.get("document_date")),
        "service_period_start": "",
        "service_period_end": "",
        "due_date": normalize_value(record.get("due_date")),
        "previous_balance": "",
        "payments_received": "",
        "credits_adjustments": "",
        "late_fees": "",
        "penalties": "",
        "taxes_fees": "",
        "current_charges": "",
        "amount_due": normalize_value(record.get("amount_due")),
        "expense_amount_current_period": "",
        "expense_amount_excluding_arrears": "",
        "contains_prior_balance": "",
        "contains_late_fee": "",
        "contains_penalty": "",
        "review_required_accounting": "",
        "accounting_notes": "",
        "source_file": normalize_value(record.get("source_file")),
        "renamed_working_path": normalize_value(record.get("output_file")),
        "final_storage_path": normalize_value(record.get("final_storage_path")),
        "confidence_score": normalize_value(record.get("confidence_score")),
        "chatgpt_used": normalize_value(record.get("chatgpt_used")),
    }

def is_master_log_duplicate(existing_row, new_row):
    existing_output = normalize_key(existing_row.get("output_file"))
    new_output = normalize_key(new_row.get("output_file"))

    if existing_output and new_output and existing_output == new_output:
        return True

    existing_source = normalize_key(existing_row.get("source_file"))
    new_source = normalize_key(new_row.get("source_file"))

    existing_doc_date = normalize_key(existing_row.get("document_date"))
    new_doc_date = normalize_key(new_row.get("document_date"))

    existing_vendor = normalize_key(existing_row.get("vendor_name"))
    new_vendor = normalize_key(new_row.get("vendor_name"))

    existing_account = normalize_key(existing_row.get("account_number"))
    new_account = normalize_key(new_row.get("account_number"))

    if (
        existing_source and new_source
        and existing_doc_date and new_doc_date
        and existing_vendor and new_vendor
        and existing_account and new_account
    ):
        return (
            existing_source == new_source
            and existing_doc_date == new_doc_date
            and existing_vendor == new_vendor
            and existing_account == new_account
        )

    return False

def append_document_master_record(record):
    new_row = build_master_log_row(record)
    rows = read_csv_as_dicts(MASTER_LOG_CSV)

    for existing_row in rows:
        if is_master_log_duplicate(existing_row, new_row):
            return False

    append_csv(MASTER_LOG_CSV, MASTER_LOG_FIELDS, new_row)
    return True
