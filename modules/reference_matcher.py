import csv
import re


def normalize_account_number(value):
    if not value:
        return ""
    return re.sub(r"[^0-9]", "", str(value))


def load_reference_table(path):
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def reference_check(extracted, reference_table):
    vendor = extracted.get("vendor_name")
    account = extracted.get("account_number")
    address = extracted.get("service_address")
    property_name = extracted.get("property_name")
    unit = extracted.get("unit")

    normalized_account = normalize_account_number(account)

    # 1. Account match
    if normalized_account:
        for ref in reference_table:
            ref_account = normalize_account_number(ref.get("account_number"))
            if ref_account and ref_account == normalized_account:
                return {
                    "match_status": "matched",
                    "reference_id": ref["reference_id"],
                    "property_name": ref["property_name"],
                    "unit": ref["unit"],
                    "category": ref["category"],
                    "needs_user_confirmation": False,
                    "routing_basis": "account"
                }

    # 2. Address match
    if address:
        for ref in reference_table:
            if ref.get("service_address") == address:
                return {
                    "match_status": "matched",
                    "reference_id": ref["reference_id"],
                    "property_name": ref["property_name"],
                    "unit": ref["unit"],
                    "category": ref["category"],
                    "needs_user_confirmation": False,
                    "routing_basis": "address"
                }

    # 3. Vendor match (single_property only)
    if vendor:
        matches = [r for r in reference_table if r.get("vendor_name") == vendor]

        if len(matches) == 1 and matches[0].get("property_scope") == "single_property":
            ref = matches[0]
            return {
                "match_status": "matched",
                "reference_id": ref["reference_id"],
                "property_name": ref["property_name"],
                "unit": ref["unit"],
                "category": ref["category"],
                "needs_user_confirmation": False,
                "routing_basis": "vendor"
            }

    # 4. No match → proposed
    return {
        "match_status": "proposed",
        "reference_id": None,
        "property_name": property_name,
        "unit": unit,
        "category": extracted.get("category_guess"),
        "needs_user_confirmation": True,
        "routing_basis": "unknown"
    }