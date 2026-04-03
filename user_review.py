def _prompt_edit(field_label: str, current_value, allowed_values=None):
    current_text = "" if current_value is None else str(current_value)

    print(f"\n{field_label}")
    print(f"Current value: {current_text}")

    if allowed_values:
        print("Allowed values:")
        for value in allowed_values:
            print(f" - {value}")

    new_value = input("Enter new value or press Enter to keep current: ").strip()

    if new_value == "":
        return current_value

    if allowed_values and new_value not in allowed_values:
        print("Invalid value. Keeping current value.")
        return current_value

    return new_value


def review_extracted_data(extracted_data: dict) -> dict:
    print("\n--- USER REVIEW ---")

    print(f"Vendor Status: {extracted_data.get('vendor_status', '')}")
    print(f"Document Type: {extracted_data.get('document_type', '')}")
    print(f"Vendor Category: {extracted_data.get('vendor_category', '')}")

    print(f"\nVendor: {extracted_data.get('vendor_name_normalized', '')}")
    print(f"Account Number: {extracted_data.get('account_number', '')}")
    print(f"Property: {extracted_data.get('property', '')}")
    print(f"Unit: {extracted_data.get('unit', '')}")
    print(f"Bill Date: {extracted_data.get('bill_date', '')}")
    print(f"Due Date: {extracted_data.get('due_date', '')}")
    print(f"Amount Due: {extracted_data.get('amount_due', '')}")

    if extracted_data.get("vendor_category") == "handyman services":
        print("\nHandyman detected — property/unit selection is required.")

    user_input = input("\nConfirm data as-is? (y/n): ").strip().lower()

    if user_input == "y":
        extracted_data["user_confirmed"] = True
        return extracted_data

    print("\n--- EDIT MODE ---")
    print("Press Enter to keep the current value.")

    document_type_options = ["bill", "invoice", "receipt", ""]
    vendor_category_options = [
        "utility",
        "government",
        "insurance",
        "service",
        "handyman services",
        "supplies",
        "supplier/store",
        "other",
        "",
    ]
    property_options = [
        "1423 Central Ave",
        "3715 Lincoln Ave",
        "3047 Sea Marsh Rd",
        "",
    ]
    unit_options = [
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
        "House",
        "",
    ]

    extracted_data["vendor_name_normalized"] = _prompt_edit(
        "Vendor Name",
        extracted_data.get("vendor_name_normalized", "")
    )

    extracted_data["account_number"] = _prompt_edit(
        "Account Number",
        extracted_data.get("account_number", "")
    )

    extracted_data["property"] = _prompt_edit(
        "Property",
        extracted_data.get("property", ""),
        property_options
    )

    extracted_data["unit"] = _prompt_edit(
        "Unit",
        extracted_data.get("unit", ""),
        unit_options
    )

    extracted_data["document_type"] = _prompt_edit(
        "Document Type",
        extracted_data.get("document_type", ""),
        document_type_options
    )

    extracted_data["vendor_category"] = _prompt_edit(
        "Vendor Category",
        extracted_data.get("vendor_category", ""),
        vendor_category_options
    )

    extracted_data["bill_date"] = _prompt_edit(
        "Bill Date",
        extracted_data.get("bill_date", "")
    )

    extracted_data["due_date"] = _prompt_edit(
        "Due Date",
        extracted_data.get("due_date", "")
    )

    extracted_data["amount_due"] = _prompt_edit(
        "Amount Due",
        extracted_data.get("amount_due", "")
    )

    print("\n--- REVIEWED / CORRECTED DATA ---")
    print(f"Vendor: {extracted_data.get('vendor_name_normalized', '')}")
    print(f"Account Number: {extracted_data.get('account_number', '')}")
    print(f"Property: {extracted_data.get('property', '')}")
    print(f"Unit: {extracted_data.get('unit', '')}")
    print(f"Document Type: {extracted_data.get('document_type', '')}")
    print(f"Vendor Category: {extracted_data.get('vendor_category', '')}")
    print(f"Bill Date: {extracted_data.get('bill_date', '')}")
    print(f"Due Date: {extracted_data.get('due_date', '')}")
    print(f"Amount Due: {extracted_data.get('amount_due', '')}")

    final_confirm = input("\nConfirm corrected data? (y/n): ").strip().lower()

    extracted_data["user_confirmed"] = final_confirm == "y"
    return extracted_data