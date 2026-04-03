from __future__ import annotations
import re
from typing import Dict, Any

_AMOUNT_RE = re.compile(r'(?<!\d)(?:\$\s*)?(\d{1,3}(?:,\d{3})*(?:\.\d{2})|\d+\.\d{2})(?!\d)')
_DATE_RE = re.compile(r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[A-Z][a-z]+\s+\d{1,2},\s*\d{4})\b')
_ACCOUNT_RE = re.compile(r'(?:account|acct)(?:\s*(?:number|no\.?))?[:#\s]*([xX*]*\d{4,})', re.I)

KNOWN_VENDORS = [
    "PG&E", "EBMUD", "AT&T", "Comcast", "Verizon", "Florida Power & Light", "FPL", "Brother"
]

def extract_fields(text: str) -> Dict[str, Any]:
    amount_match = _AMOUNT_RE.search(text)
    date_match = _DATE_RE.search(text)
    account_match = _ACCOUNT_RE.search(text)

    vendor = None
    for name in KNOWN_VENDORS:
        if name.lower() in text.lower():
            vendor = name
            break

    property_hint = None
    lowered = text.lower()
    if "central ave" in lowered:
        property_hint = "1423 Central Ave"
    elif "lincoln ave" in lowered:
        property_hint = "3715 Lincoln Ave"
    elif "sea marsh" in lowered or "fernandina" in lowered:
        property_hint = "3047 Sea Marsh"

    amount = None
    if amount_match:
        amount = float(amount_match.group(1).replace(",", ""))

    return {
        "vendor": vendor,
        "amount": amount,
        "document_date": date_match.group(1) if date_match else None,
        "account_last4": account_match.group(1)[-4:] if account_match else None,
        "property_hint": property_hint,
    }
