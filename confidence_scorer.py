def score_document(
    account_match: bool,
    address_match: bool,
    vendor_match: bool,
    unit_match: bool,
    is_new_vendor: bool,
    is_new_account: bool,
    is_handyman: bool,
    missing_property: bool,
    missing_unit: bool
) -> dict:
    score = 0

    if account_match:
        score += 50
    if address_match:
        score += 30
    if vendor_match:
        score += 10
    if unit_match:
        score += 10

    review_required = False

    if is_new_vendor:
        review_required = True
    if is_new_account:
        review_required = True
    if is_handyman:
        review_required = True
    if missing_property:
        review_required = True
    if missing_unit:
        review_required = True

    if review_required:
        decision = "review"
    elif score >= 90:
        decision = "auto"
    elif score >= 70:
        decision = "flag"
    else:
        decision = "review"

    return {
        "score": score,
        "decision": decision,
        "review_required": review_required
    }