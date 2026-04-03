from __future__ import annotations
from typing import Dict

def classify_document(text: str) -> Dict[str, object]:
    t = text.lower()
    if any(k in t for k in ["receipt", "visa", "mastercard", "subtotal", "tax", "change due"]):
        return {"document_type": "receipt", "confidence": 0.78}
    if any(k in t for k in ["statement date", "account number", "amount due", "payment due"]):
        return {"document_type": "bill", "confidence": 0.82}
    if any(k in t for k in ["policy number", "insured", "coverage"]):
        return {"document_type": "insurance", "confidence": 0.8}
    if any(k in t for k in ["permit", "license", "city of", "county of"]):
        return {"document_type": "permit_or_license", "confidence": 0.72}
    return {"document_type": "unknown", "confidence": 0.35}
