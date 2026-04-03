from chatgpt_analyzer import analyze_document
from new_doc_detector import is_new_document
from confidence_scorer import score_document
from user_review import review_extracted_data
from vendor_profile_store import upsert_vendor_profile
import json

test_text = """
Florida Public Utilities
Account Number: 200000352910
Service Address: 3047 Sea Marsh Rd
Bill Date: 03/11/2026
Due Date: 03/31/2026
Amount Due: $253.92
"""

# Step 1: Extract with ChatGPT
extracted = analyze_document(test_text)
print("\n--- EXTRACTED DATA ---")
print(json.dumps(extracted, indent=2))

# Step 2: Simulate local matching results
vendor_name = extracted.get("vendor_name_normalized", "")
account_number = extracted.get("account_number", "")

reference_match_found = False   # force "new doc" path for this test

# Step 3: New doc detection
new_doc = is_new_document(vendor_name, account_number, reference_match_found)
print(f"\nIs new document? {new_doc}")

# Step 4: Confidence score
confidence = score_document(
    account_match=False,
    address_match=False,
    vendor_match=False,
    unit_match=False,
    is_new_vendor=True,
    is_new_account=True,
    is_handyman=False,
    missing_property=(extracted.get("property", "") == ""),
    missing_unit=False
)
print("\n--- CONFIDENCE ---")
print(confidence)

# Step 5: User review if needed
if new_doc or confidence["decision"] == "review":
    reviewed = review_extracted_data(extracted)
    print("\n--- REVIEWED DATA ---")
    print(json.dumps(reviewed, indent=2))

    if reviewed.get("user_confirmed") is True:
        vendor_name = reviewed.get("vendor_name_normalized", "")
        account_number = reviewed.get("account_number", "")
        property_name = reviewed.get("property", "")
        unit = reviewed.get("unit", "")

        profile = {
            "vendor_name_normalized": vendor_name,
            "vendor_category": "utility",
            "document_types_seen": [reviewed.get("document_type", "")],
            "primary_match_method": "account_number",
            "fallback_match_method": "address",
            "requires_unit_selection": False,
            "known_accounts": {
                account_number: {
                    "property": property_name,
                    "unit": unit
                }
            },
            "known_properties_used": [property_name],
            "status": "provisional"
        }

        upsert_vendor_profile(vendor_name, profile)
        print(f"\nSaved vendor profile for: {vendor_name}")
else:
    print("\nNo review needed.")