from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any
from PyPDF2 import PdfReader

def inspect_pdf(pdf_path: str | Path) -> Dict[str, Any]:
    pdf_path = Path(pdf_path)
    reader = PdfReader(str(pdf_path))
    page_count = len(reader.pages)
    text_preview = ""
    if page_count:
        try:
            text_preview = (reader.pages[0].extract_text() or "")[:1000]
        except Exception:
            text_preview = ""
    return {
        "pdf_path": str(pdf_path),
        "page_count": page_count,
        "text_preview": text_preview,
        "preview_images": [],
    }
