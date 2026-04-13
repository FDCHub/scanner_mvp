import anthropic
import json
import os
from dotenv import load_dotenv

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """
You are a document data extraction engine.

Extract structured data from bills, invoices, and receipts.

Return ONLY valid JSON. No explanations.

Rules:
- Use the exact schema provided
- Do not guess — leave fields as "" if not found
- Normalize all dates to YYYY-MM-DD
- Amounts should be numbers without $ signs
- Identify property and unit using the provided known properties and units
- Be precise and conservative
"""

USER_PROMPT_TEMPLATE = """
DOCUMENT OCR TEXT:
{ocr_text}

KNOWN PROPERTIES:
- 1423 Central Ave (Units: B, G, I, HSE)
- 3715 Lincoln Ave (Units: 1-10)
- 3047 Sea Marsh Rd (House)

EXTRACTION SCHEMA:
{schema}
"""


SCHEMA = {
    "document_type": "",
    "vendor_name_raw": "",
    "vendor_name_normalized": "",
    "property": "",
    "unit": "",
    "service_address": "",
    "account_number": "",
    "customer_name": "",
    "bill_date": "",
    "service_period_start": "",
    "service_period_end": "",
    "due_date": "",
    "amount_due": "",
    "previous_balance": "",
    "payments_received": "",
    "adjustments_or_credits": "",
    "late_fees": "",
    "total_balance": "",
    "payment_status": "",
    "payment_date": "",
    "invoice_number": "",
    "description_of_charges": "",
    "confidence_score": "",
    "needs_review": "",
    "source_pages": "",
    "extraction_notes": ""
}


def analyze_document(ocr_text: str) -> dict:
    user_prompt = USER_PROMPT_TEMPLATE.format(
        ocr_text=ocr_text,
        schema=json.dumps(SCHEMA, indent=2)
    )

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[
            {"role": "user", "content": user_prompt}
        ]
    )

    output_text = response.content[0].text.strip()

    # Strip markdown code fences if Claude wraps the JSON
    if output_text.startswith("```"):
        lines = output_text.splitlines()
        output_text = "\n".join(
            line for line in lines
            if not line.strip().startswith("```")
        ).strip()

    try:
        data = json.loads(output_text)
    except Exception:
        raise ValueError("Invalid JSON returned from Claude")

    # Force exact schema shape (no missing fields)
    clean_data = {}
    for field, default_value in SCHEMA.items():
        clean_data[field] = data.get(field, default_value)

    return clean_data