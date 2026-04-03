def is_new_document(vendor_name: str, account_number: str, reference_match_found: bool) -> bool:
    if not vendor_name:
        return True

    if not reference_match_found:
        return True

    if not account_number:
        return True

    return False