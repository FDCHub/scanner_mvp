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

Your job is to read the document image carefully and extract every visible data field.

Rules:
- Return ONLY valid JSON matching the schema provided. No explanations, no markdown.
- Only leave a field as "" if the information is genuinely absent from the document.
- Normalize all dates to YYYY-MM-DD format.
- Amounts must be numeric strings without $ signs (e.g. "125.47"). Strip commas.
- For document_type use only: "bill", "invoice", or "receipt".
- For confidence scores use only: "high", "medium", or "low".
  - high   = clearly visible and unambiguous on the document
  - medium = partially visible, inferred, or slightly unclear
  - low    = not found, illegible, or uncertain
- In extraction_notes, explain anything unusual, ambiguous, or worth human review.
- If the document appears to span multiple pages but only one image was provided,
  note this in extraction_notes.

Financial field extraction guide — look for these labels (exact wording varies by vendor):
- previous_balance    : "Previous Balance", "Prior Balance", "Balance Forward", "Past Due"
- payments_received   : "Payments Received", "Payment Applied", "Credits Applied", "Payment - Thank You"
- adjustments_or_credits : "Adjustments", "Credits", "Promotional Credit", "Discount"
- current_charges     : "Current Charges", "New Charges", "Current Bill", "Charges This Period"
- late_fees           : "Late Fee", "Late Charge", "Past Due Fee", "Penalty"
- amount_due          : "Amount Due", "Total Due", "Balance Due", "Total Amount Due", "Please Pay"
- due_date            : "Due Date", "Payment Due", "Pay By", "Due By"

Extract every financial field you can read — these are the most important fields for this system.
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
    "current_charges":         "",
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
        max_tokens=2048,
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


# ── Dynamic-only extraction ───────────────────────────────────────────────────
# Used when static fields (vendor, account, property, unit, address) are already
# confirmed via a reference-table match.  Only financial / billing fields are
# requested, reducing prompt size and Claude latency.

DYNAMIC_SCHEMA = {
    "document_type":          "",
    "bill_date":              "",
    "service_period_start":   "",
    "service_period_end":     "",
    "due_date":               "",
    "amount_due":             "",
    "previous_balance":       "",
    "payments_received":      "",
    "adjustments_or_credits": "",
    "current_charges":        "",
    "late_fees":              "",
    "total_balance":          "",
    "payment_status":         "",
    "payment_date":           "",
    "invoice_number":         "",
    "description_of_charges": "",
    "document_image_quality": "",
    "extraction_notes":       "",
    "field_confidence": {
        "amount_due": "",
        "due_date":   "",
        "date":       "",
    },
}

_DYNAMIC_SYSTEM = """
You are a document data extraction engine specialising in bills, invoices, and receipts.

The vendor, account number, and property details for this document are ALREADY CONFIRMED.
Your ONLY task is to extract the financial and billing data fields listed in the schema.

Rules:
- Return ONLY valid JSON matching the schema exactly. No explanations, no markdown fences.
- Leave a field as "" only if the information is genuinely absent from the document.
- Normalize all dates to YYYY-MM-DD format.
- Amounts must be numeric strings without $ signs (e.g. "125.47"). Strip commas.
- For document_type use only: "bill", "invoice", or "receipt".
- For confidence scores use only: "high", "medium", or "low".
- If "PAID" stamp or text is visible on the document, set payment_status to "paid".

Financial field extraction guide — look for these labels (exact wording varies by vendor):
- previous_balance    : "Previous Balance", "Prior Balance", "Balance Forward", "Past Due"
- payments_received   : "Payments Received", "Payment Applied", "Credits Applied", "Payment - Thank You"
- adjustments_or_credits : "Adjustments", "Credits", "Promotional Credit", "Discount"
- current_charges     : "Current Charges", "New Charges", "Current Bill", "Charges This Period"
- late_fees           : "Late Fee", "Late Charge", "Past Due Fee", "Penalty"
- amount_due          : "Amount Due", "Total Due", "Balance Due", "Total Amount Due", "Please Pay"
- due_date            : "Due Date", "Payment Due", "Pay By", "Due By"

Extract EVERY financial field you can read — these are the most important fields.
"""


def analyze_document_dynamic(
    file_path: str | Path,
    static_context: dict,
) -> dict:
    """
    Extract only dynamic financial fields from a document.

    *static_context* contains the already-known canonical values pulled from the
    reference table (vendor_name, account_number, property, unit, service_address,
    vendor_category).  Claude is told these are confirmed so it can focus entirely
    on the billing / financial data.

    Returns a dict that merges the dynamic extraction with the provided static
    context, ready to be used as a drop-in replacement for analyze_document().
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Document not found: {file_path}")

    print(f"  [Claude/dynamic] Preparing image from: {file_path.name}")
    image_data, media_type = _prepare_image(file_path)

    ctx_lines = []
    for key, label in [
        ("vendor_name",    "Vendor"),
        ("account_number", "Account"),
        ("property",       "Property"),
        ("unit",           "Unit"),
        ("service_address","Service Address"),
    ]:
        val = static_context.get(key, "")
        if val:
            ctx_lines.append(f"  {label}: {val}")

    context_block = "\n".join(ctx_lines) if ctx_lines else "  (not provided)"

    user_prompt = f"""
Known account details (already confirmed — do NOT re-extract these fields):
{context_block}

Extract ONLY the financial / billing data from this document.
Return ONLY a JSON object matching this exact schema (no extra fields, no markdown):

{json.dumps(DYNAMIC_SCHEMA, indent=2)}
"""

    print(f"  [Claude/dynamic] Sending to API ({MODEL}) — financial fields only...")
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=_DYNAMIC_SYSTEM,
        messages=[{
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
                {"type": "text", "text": user_prompt},
            ],
        }],
    )

    raw_text = response.content[0].text.strip()
    if raw_text.startswith("```"):
        raw_text = "\n".join(
            l for l in raw_text.splitlines() if not l.strip().startswith("```")
        ).strip()

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Claude (dynamic) returned invalid JSON: {e}\n\nRaw:\n{raw_text}"
        )

    # Build clean dynamic result from schema
    clean: dict = {}
    for field, default in DYNAMIC_SCHEMA.items():
        if field == "field_confidence":
            conf_raw = data.get("field_confidence", {})
            clean["field_confidence"] = {
                k: conf_raw.get(k, "") for k in DYNAMIC_SCHEMA["field_confidence"]
            }
        else:
            clean[field] = data.get(field, default)

    # Merge in the confirmed static fields
    clean["vendor_name_raw"]        = static_context.get("vendor_name", "")
    clean["vendor_name_normalized"] = static_context.get("vendor_name", "")
    clean["vendor_category"]        = static_context.get("vendor_category", "")
    clean["account_number"]         = static_context.get("account_number", "")
    clean["property"]               = static_context.get("property", "")
    clean["unit"]                   = static_context.get("unit", "")
    clean["service_address"]        = static_context.get("service_address", "")

    print(
        f"  [Claude/dynamic] Done. "
        f"Amount: '{clean.get('amount_due')}' | "
        f"Due: '{clean.get('due_date')}' | "
        f"Date: '{clean.get('bill_date')}'"
    )
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
