import csv
import json
from vendor_profile_store import save_vendor_profiles

CSV_PATH = "vendor_seed_table.csv"
OUTPUT_PATH = "vendor_profiles.json"


def load_seed_file():
    profiles = {}

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if not row or not row.get("vendor_name_normalized"):
                continue

            vendor_name = (row.get("vendor_name_normalized") or "").strip()
            if not vendor_name:
                continue

            vendor_category = (row.get("vendor_category") or "").strip()
            multi_property = (row.get("multi_property") or "").strip().lower() == "true"
            document_type = (row.get("document_type_expected") or "").strip()
            account = (row.get("account_number_hint") or "").strip()
            property_name = (row.get("property") or "").strip()
            unit = (row.get("unit_hint") or "").strip()
            primary_match_method = (row.get("primary_match_method") or "").strip()
            fallback_match_method = (row.get("fallback_match_method") or "").strip()
            requires_unit_selection = (
                (row.get("requires_unit_selection") or "").strip().lower() == "true"
            )

            if vendor_name not in profiles:
                profiles[vendor_name] = {
                    "vendor_name_normalized": vendor_name,
                    "vendor_category": vendor_category,
                    "multi_property": multi_property,
                    "document_types_seen": [],
                    "primary_match_method": primary_match_method,
                    "fallback_match_method": fallback_match_method,
                    "requires_unit_selection": requires_unit_selection,
                    "known_accounts": {},
                    "known_properties_used": [],
                    "status": "seeded",
                }

            profile = profiles[vendor_name]

            # Keep multi_property once true
            if multi_property:
                profile["multi_property"] = True

            # Keep document types unique
            if document_type and document_type not in profile["document_types_seen"]:
                profile["document_types_seen"].append(document_type)

            # Add account mapping
            if account:
                profile["known_accounts"][account] = {
                    "property": property_name,
                    "unit": unit,
                }

            # Add property if not ALL and not already present
            if property_name and property_name != "ALL":
                if property_name not in profile["known_properties_used"]:
                    profile["known_properties_used"].append(property_name)

    print("\nDEBUG — Alameda Municipal Power accounts:")
    print(profiles.get("Alameda Municipal Power", {}).get("known_accounts", {}))

    print("\nDEBUG — PG&E accounts:")
    print(profiles.get("PG&E", {}).get("known_accounts", {}))

    save_vendor_profiles(profiles)
    print("Seed data loaded into vendor_profiles.json")


if __name__ == "__main__":
    load_seed_file()
