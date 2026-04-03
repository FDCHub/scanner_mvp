import re


def clean_account_number(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", str(value).strip())


def calculate_similarity(a: str, b: str) -> float:
    """
    Simple similarity score based on matching characters in same position.
    Returns value between 0 and 1.
    """
    a = clean_account_number(a)
    b = clean_account_number(b)

    if not a or not b:
        return 0.0

    max_len = max(len(a), len(b))
    matches = sum(1 for i in range(min(len(a), len(b))) if a[i] == b[i])

    return matches / max_len


def find_similar_account(input_account: str, known_accounts: dict, threshold: float = 0.85):
    """
    Looks for similar account numbers in known accounts.
    Returns best match if above threshold.
    """
    input_account = clean_account_number(input_account)

    if not input_account or not known_accounts:
        return None, 0.0

    best_match = None
    best_score = 0.0

    for acct in known_accounts.keys():
        cleaned_acct = clean_account_number(acct)
        score = calculate_similarity(input_account, cleaned_acct)

        if score > best_score:
            best_score = score
            best_match = acct

    if best_score >= threshold:
        return best_match, best_score

    return None, best_score