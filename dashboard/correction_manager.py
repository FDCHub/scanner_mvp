from __future__ import annotations
from typing import Any, Dict
from models.shared_models import ExtractionResult

def apply_corrections(extraction: ExtractionResult, edits: Dict[str, Any]) -> ExtractionResult:
    for key, value in edits.items():
        if hasattr(extraction, key):
            setattr(extraction, key, value)
        else:
            extraction.fields[key] = value
    return extraction
