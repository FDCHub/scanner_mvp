from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
from PyPDF2 import PdfReader

def extract_text_from_pdf(pdf_path: str | Path) -> Dict[str, Any]:
    pdf_path = Path(pdf_path)
    reader = PdfReader(str(pdf_path))
    pages = []
    for i, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        pages.append({"page_number": i, "text": text})
    combined_text = "\n\n".join(item["text"] for item in pages if item["text"])
    warnings = []
    if not combined_text.strip():
        warnings.append("No embedded PDF text found. Install/configure Tesseract for image-only scan OCR.")
    return {
        "pages": pages,
        "combined_text": combined_text,
        "warnings": warnings,
    }
