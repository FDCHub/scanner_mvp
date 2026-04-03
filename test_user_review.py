from user_review import review_extracted_data

sample = {
    "vendor_name_normalized": "Florida Public Utilities",
    "document_type": "bill",
    "account_number": "200000352910",
    "property": "3047 Sea Marsh Rd",
    "unit": "",
    "bill_date": "2026-03-11",
    "due_date": "2026-03-31",
    "amount_due": 253.92
}

result = review_extracted_data(sample)
print(result)