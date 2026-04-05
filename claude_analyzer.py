"""
claude_analyzer.py
------------------
Replaces chatgpt_analyzer.py.

Sends a document image (converted from PDF or passed directly) to Claude's
vision API and returns a structured dictionary of extracted fields.

Key differences from the old ChatGPT version:
  - Accepts a file PATH (PDF or image), not pre-extracted OCR text
  - Claude reads the document image directly — no Tesseract OCR needed
  - Claude also returns a per-field confidence breakdown, not just one score
  - Works with pdf2image to convert PDFs to images before sending
"""

import anthropic
import base64
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from pdf2image import convert_from_path

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

MODEL = "claude-haiku-4-5-20251001"   # Fast and cheap for document extraction.
                                       # Swap to "claude-sonnet-4-6" for harder docs.

SYSTEM_PROMPT = """
You are a document data extraction engine specializing in bills, invoices, and receipts.

Your job is to read the document image carefully and extract structured data.

Rules:
- Return ONLY valid JSON matching the schema provided. No explanations, no markdown.
- Do not guess — use "" for any field you cannot clearly read.
- Normalize all dates to YYYY-MM-DD format.
- Amounts must be numeric strings without $ signs (e.g. "125.47").
- For document_type use only: "bill", "invoice", or "receipt".
- For confidence scores use only: "high", "medium", or "low".
  - high   = clearly visible and unambiguous on the document
  - medium = partially visible, inferred, or slightly unclear
  - low    = not found, illegible, or uncertain
- In extraction_notes, explain anything unusual, ambiguous, or worth human review.
- If the document appears to span multiple pages but only one image was provided,
  note this in extraction_notes.
"""

KNOWN_PROPERTIES = """
KNOWN PROPERTIES AND UNITS (use these to identify property and unit fields):
  - 1423 Central Ave      (Units: B, G, I, HSE)
  - 3715 Lincoln Ave      (Units: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  - 3047 Sea Marsh Rd     (Single house — no unit)
"""

SCHEMA = {
    "document_type":           "",
    "vendor_name_raw":         "",
    "vendor_name_normalized":  "",
    "property":                "",
    "unit":                    "",
    "service_address":         "",
    "account_number":          "",
    "customer_name":           "",
    "bill_date":               "",
    "service_period_start":    "",
    "service_period_end":      "",
    "due_date":                "",
    "amount_due":              "",
    "previous_balance":        "",
    "payments_received":       "",
    "adjustments_or_credits":  "",
    "late_fees":               "",
    "total_balance":           "",
    "payment_status":          "",
    "payment_date":            "",
    "invoice_number":          "",
    "description_of_charges":  "",
    "document_image_quality":  "",   # "clear", "degraded", or "unreadable"
    "extraction_notes":        "",
    "field_confidence": {            # Per-field confidence breakdown
        "vendor":              "",
        "property":            "",
        "account_number":      "",
        "date":                "",
        "amount_due":          "",
        "due_date":            "",
    }
}


def _pdf_to_base64_image(pdf_path: Path) -> tuple[str, str]:
    """
    Convert the first page of a PDF to a base64-encoded JPEG.
    Returns (base64_string, media_type).
    """
    images = convert_from_path(str(pdf_path), dpi=200, first_page=1, last_page=1)
    if not images:
        raise ValueError(f"Could not convert PDF to image: {pdf_path}")

    import io
    buffer = io.BytesIO()
    images[0].save(buffer, format="JPEG", quality=90)
    buffer.seek(0)
    b64 = base64.standard_b64encode(buffer.read()).decode("utf-8")
    return b64, "image/jpeg"


def _image_to_base64(image_path: Path) -> tuple[str, str]:
    """
    Read an image file (JPEG, PNG, etc.) and return base64 + media type.
    """
    suffix = image_path.suffix.lower()
    media_type_map = {
        ".jpg":  "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png":  "image/png",
        ".gif":  "image/gif",
        ".webp": "image/webp",
    }
    media_type = media_type_map.get(suffix, "image/jpeg")
    with open(image_path, "rb") as f:
        b64 = base64.standard_b64encode(f.read()).decode("utf-8")
    return b64, media_type


def _prepare_image(file_path: Path) -> tuple[str, str]:
    """
    Route to the right conversion based on file type.
    Returns (base64_string, media_type).
    """
    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return _pdf_to_base64_image(file_path)
    else:
        return _image_to_base64(file_path)


def analyze_document(file_path: str | Path) -> dict:
    """
    Main entry point. Pass a path to a PDF or image file.
    Returns a fully populated extraction dictionary.

    Example:
        result = analyze_document("D:/Scans/Working/my_bill.pdf")
        print(result["vendor_name_normalized"])
        print(result["amount_due"])
        print(result["field_confidence"]["amount_due"])  # "high" / "medium" / "low"
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Document not found: {file_path}")

    print(f"  [Claude] Preparing image from: {file_path.name}")
    image_data, media_type = _prepare_image(file_path)

    user_prompt = f"""
Please extract all data from this document image.

{KNOWN_PROPERTIES}

Return ONLY a JSON object matching this exact schema (no extra fields, no markdown):

{json.dumps(SCHEMA, indent=2)}
"""

    print(f"  [Claude] Sending to API ({MODEL})...")
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": image_data,
                        },
                    },
                    {
                        "type": "text",
                        "text": user_prompt,
                    },
                ],
            }
        ],
    )

    raw_text = response.content[0].text.strip()

    # Strip markdown code fences if Claude wraps the JSON anyway
    if raw_text.startswith("```"):
        lines = raw_text.splitlines()
        raw_text = "\n".join(
            line for line in lines
            if not line.strip().startswith("```")
        ).strip()

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Claude returned invalid JSON: {e}\n\nRaw response:\n{raw_text}"
        )

    # Enforce exact schema shape — no missing fields, no extra fields
    clean = {}
    for field, default in SCHEMA.items():
        if field == "field_confidence":
            conf_raw = data.get("field_confidence", {})
            clean["field_confidence"] = {
                k: conf_raw.get(k, "") for k in SCHEMA["field_confidence"]
            }
        else:
            clean[field] = data.get(field, default)

    print(f"  [Claude] Extraction complete. "
          f"Vendor: '{clean.get('vendor_name_normalized') or clean.get('vendor_name_raw')}' | "
          f"Amount: '{clean.get('amount_due')}' | "
          f"Date: '{clean.get('bill_date')}'")

    return clean


def get_overall_confidence(result: dict) -> str:
    """
    Derive a single overall confidence label from the per-field breakdown.
    Used by the validation layer and review popup to decide how much to highlight.

    Returns: "high", "medium", or "low"
    """
    conf = result.get("field_confidence", {})
    scores = list(conf.values())

    if not scores:
        return "low"

    score_map = {"high": 2, "medium": 1, "low": 0, "": 0}
    numeric = [score_map.get(s, 0) for s in scores]
    avg = sum(numeric) / len(numeric)

    if avg >= 1.7:
        return "high"
    elif avg >= 0.9:
        return "medium"
    else:
        return "low"
