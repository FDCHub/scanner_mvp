from __future__ import annotations
from models.shared_models import ExtractionResult

def generate_summary(extraction: ExtractionResult) -> str:
    parts = []
    if extraction.vendor:
        parts.append(extraction.vendor)
    if extraction.document_type and extraction.document_type != "unknown":
        parts.append(extraction.document_type.replace("_", " "))
    if extraction.amount is not None:
        parts.append(f"${extraction.amount:,.2f}")
    if extraction.document_date:
        parts.append(extraction.document_date)
    if extraction.property_hint:
        parts.append(f"for {extraction.property_hint}")
    return ", ".join(parts) if parts else "Document ready for review."
