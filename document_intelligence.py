import re
import shutil
from pathlib import Path
from datetime import datetime


def normalize_text(text: str) -> str:
    """
    Clean OCR text for easier searching
    """
    text = text.lower()
    text = text.replace("\r", "\n")
    return text


def detect_vendor(text: str) -> str | None:
    """
    Detect the vendor / sender from OCR text
    """
    t = normalize_text(text)

    if "alameda municipal power" in t or "municipal power" in t:
        return "AMP"

    if "east bay municipal utility district" in t or "ebmud" in t:
        return "EBMUD"

    if "pacific gas and electric" in t or "pg&e" in t or "pge" in t:
        return "PGE"

    if "comcast" in t or "xfinity" in t:
        return "COMCAST"

    return None


def classify_document(text: str) -> str:
    """
    Identify the document type
    """
    vendor = detect_vendor(text)

    if vendor in ["AMP", "EBMUD", "PGE"]:
        return "Utility_Bill"

    if vendor == "COMCAST":
        return "Internet_Bill"

    t = normalize_text(text)

    if "oakland housing authority" in t or "inspection" in t:
        return "OHA_Inspection"

    return "Unknown"


def detect_property(text: str) -> str | None:
    """
    Detect the property address
    """
    t = normalize_text(text)

    if "1423 central ave" in t:
        return "1423 Central Ave"

    return None


def extract_service_address(text: str) -> str | None:
    """
    Extract service address line (generalized)
    """
    import re

    t = normalize_text(text)

    # 1. Look for explicit service address labels first
    label_patterns = [
        r"service address:\s*(.+)",
        r"service location:\s*(.+)",
        r"premise address:\s*(.+)",
        r"for service at:\s*(.+)",
    ]

    for pattern in label_patterns:
        match = re.search(pattern, t, re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()

            # Reject mailing-style addresses
            if "pmb" in candidate.lower() or "po box" in candidate.lower():
                continue

            return clean_address(candidate)

    # 2. Fallback: find address-like lines
    lines = t.split("\n")

    for line in lines:
        line = line.strip()

        # Basic address pattern: number + street
        if re.search(r"\d{3,5}\s+[a-z]+\s+(ave|street|st|road|rd|blvd|lane|ln)", line, re.IGNORECASE):

            # Reject mailing / business mailbox
            if "pmb" in line.lower() or "po box" in line.lower():
                continue

            return clean_address(line)

    return None

def clean_address(addr: str) -> str:
    return " ".join(addr.upper().replace("|", "").split())

def extract_unit(text: str) -> str | None:
    """
    Extract unit letter (B, G, I, HSE)
    """
    t = normalize_text(text)

    patterns = [
        r"service address:\s*1423\s+central\s+ave\s+(b|g|i|hse)\b",
        r"1423\s+central\s+ave\s+(b|g|i|hse)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, t, re.IGNORECASE)
        if match:
            return match.group(1).upper()

    return None


def extract_date(text: str) -> str | None:
    """
    Extract billing or document date
    """
    t = normalize_text(text)

    patterns = [
        r"billing date:\s*(\d{2}/\d{2}/\d{4})",
        r"bill(?:ing)?\s+date\s+([a-z]{3}\s+\d{1,2},\s+\d{4})",
        r"date:\s*(\d{2}/\d{2}/\d{4})",
    ]

    for pattern in patterns:
        match = re.search(pattern, t, re.IGNORECASE)
        if match:
            raw = match.group(1).strip()

            for fmt in ("%m/%d/%Y", "%b %d, %Y"):
                try:
                    dt = datetime.strptime(raw, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

    return None


def extract_account_number(text: str) -> str | None:
    """
    Extract utility / service account number
    Handles cases where number is on next line
    """
    t = normalize_text(text)

    # Case 1: standard inline patterns
    patterns = [
        r"account number:\s*([0-9\- ]+)",
        r"account\s*#:\s*([0-9\- ]+)",
        r"acct(?:ount)?\s*(?:number|#)?\s*:\s*([0-9\- ]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, t, re.IGNORECASE)
        if match:
            value = re.sub(r"\s+", "", match.group(1))
            return value

    # Case 2: account number on next line
    lines = t.split("\n")

    for i, line in enumerate(lines):
        if "account number" in line:
            # look at next 2 lines for digits
            for j in range(i + 1, min(i + 3, len(lines))):
                candidate = re.sub(r"[^0-9]", "", lines[j])
                if len(candidate) >= 8:
                    return candidate

    return None

def rename_processed_file(txt_path: Path, data: dict) -> Path | None:
    """
    Rename the original PDF based on extracted metadata
    """
    pdf_path = txt_path.with_suffix(".pdf")

    if not pdf_path.exists():
        print("⚠️ PDF not found for renaming")
        return None

    vendor = data.get("vendor") or "UNKNOWN"
    property_ = (data.get("property") or "UNKNOWN").replace(" ", "")
    unit = data.get("unit") or "X"
    date = data.get("date") or "0000-00-00"
    account = data.get("account_number") or "NA"

    new_name = f"{vendor}_{property_}_{unit}_{date}_{account}.pdf"
    new_path = pdf_path.parent / new_name

    counter = 1
    original_new_path = new_path

    while new_path.exists():
        new_path = original_new_path.with_name(
            f"{original_new_path.stem}_{counter}{original_new_path.suffix}"
        )
        counter += 1

    pdf_path.rename(new_path)

    print(f"✅ Renamed file to: {new_path.name}")
    return new_path


def route_processed_file(pdf_path: Path, data: dict) -> None:
    """
    Copy renamed PDF into the correct property/category folder.
    Falls back to NeedsReview if required routing data is missing.
    """
    if not pdf_path.exists():
        print("PDF not found for routing")
        return

    property_name = (data.get("property") or "").strip()
    vendor = (data.get("vendor") or data.get("vendor_name_normalized") or "").strip()
    document_type = (data.get("document_type") or "").strip().lower()
    category = (data.get("category") or "").strip()

    # Normalize category if missing
    if not category:
        if document_type == "bill":
            category = "Utilities"
        elif document_type == "invoice":
            category = "Invoices"
        elif document_type == "receipt":
            category = "Receipts"

    # Final routing decision
    if property_name and category:
        target_folder = Path("D:/PropertyDocs") / property_name / category
    else:
        target_folder = Path("D:/PropertyDocs") / "NeedsReview"

    target_folder.mkdir(parents=True, exist_ok=True)

    target_path = target_folder / pdf_path.name
    shutil.copy2(pdf_path, target_path)

    print(f"📁 Routed to: {target_path}")

    return target_path

def extract_pge_account_number(text: str) -> str:
    """
    Extract PG&E account number formats like:
    2309403560-9
    Account No: 2309403560-9
    """
    if not text:
        return ""

    patterns = [
        r"Account\s*No\.?\s*[:#]?\s*([0-9]{6,}-[0-9])",
        r"Account\s*Number\s*[:#]?\s*([0-9]{6,}-[0-9])",
        r"\b([0-9]{6,}-[0-9])\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()

    return ""

def normalize_ocr_account_candidate(value: str) -> str:
    """
    Normalize common OCR substitutions in account-like strings.
    Keeps the overall structure intact for downstream matching.
    """
    if not value:
        return ""

    value = str(value).strip()

    replacements = {
        "O": "0",
        "o": "0",
        "I": "1",
        "l": "1",
        "!": "1",
        "S": "5",
        "s": "5",
        "B": "8",
    }

    normalized = "".join(replacements.get(ch, ch) for ch in value)
    return normalized

def extract_loose_account_candidate(text: str) -> str:
    """
    Extract an account-like token even if OCR made a character mistake.
    Intended to feed fuzzy matching, not to guarantee a perfect account number.
    Examples:
        2309403560-9
        2309403560-!
        2309403560-I
    """
    if not text:
        return ""

    patterns = [
        r"Account\s*No\.?\s*[:#]?\s*([A-Za-z0-9!]{6,}-[A-Za-z0-9!])",
        r"Account\s*Number\s*[:#]?\s*([A-Za-z0-9!]{6,}-[A-Za-z0-9!])",
        r"\b([A-Za-z0-9!]{6,}-[A-Za-z0-9!])\b",
        r"\b(\d{9,})\b",  # fallback for long numeric accounts
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return normalize_ocr_account_candidate(match.group(1))
            
            compact = candidate.replace("-", "").replace("/", "")

            # block likely MMDDYYYY-style dates if they ever slip through
            if compact.isdigit() and len(compact) == 8:
                continue

            return candidate
    return ""

def extract_service_address_fallback(text: str) -> str:
    """
    Try to capture a street line followed by city/state/ZIP from OCR text.
    Example:
        3715 LINCOLN AVE
        OAKLAND, CA 94602
    Returns:
        3715 LINCOLN AVE, OAKLAND, CA 94602
    """
    if not text:
        return ""

    lines = [line.strip() for line in text.splitlines() if line.strip()]

    street_pattern = re.compile(
        r"^\d{1,6}\s+[A-Z0-9.\- ]+\b(?:AVE|AVENUE|ST|STREET|RD|ROAD|DR|DRIVE|LN|LANE|BLVD|BOULEVARD|CT|COURT|WAY|PL|PLACE)\b",
        flags=re.IGNORECASE,
    )

    city_state_zip_pattern = re.compile(
        r"^[A-Z .'-]+,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?$",
        flags=re.IGNORECASE,
    )

    for i in range(len(lines) - 1):
        street_line = lines[i]
        city_line = lines[i + 1]

        if street_pattern.search(street_line) and city_state_zip_pattern.search(city_line):
            return f"{street_line}, {city_line}"

    return ""


def infer_property_from_service_address(service_address: str) -> str:
    """
    Map a captured service address to a known property name.
    """
    if not service_address:
        return ""

    address_upper = service_address.upper()

    if "1423 CENTRAL AVE" in address_upper:
        return "1423 Central Ave"

    if "3715 LINCOLN AVE" in address_upper:
        return "3715 Lincoln Ave"

    if "3047 SEA MARSH" in address_upper:
        return "3047 Sea Marsh Rd"

    return ""

def process_ocr_text(txt_path: str | Path) -> dict:
    """
    Analyze OCR text file and return extracted metadata.
    Does NOT rename or route yet.
    """
    txt_path = Path(txt_path)

    text = txt_path.read_text(encoding="utf-8", errors="ignore")

    vendor = detect_vendor(text)
    account_number = extract_account_number(text)

    if not account_number:
        account_number = extract_loose_account_candidate(text)

    service_address = extract_service_address(text)
    if not service_address:
        service_address = extract_service_address_fallback(text)

    property_name = detect_property(text)
    if not property_name and service_address:
        property_name = infer_property_from_service_address(service_address)

    result = {
        "vendor": vendor,
        "document_type": classify_document(text),
        "property": property_name,
        "unit": extract_unit(text),
        "date": extract_date(text),
        "account_number": account_number,
        "service_address": service_address,
    }

    print("\n=== DOCUMENT INTELLIGENCE PREVIEW ===")
    print(f"TXT File:       {txt_path}")
    print(f"Vendor:         {result['vendor'] or 'UNKNOWN'}")
    print(f"Document Type:  {result['document_type']}")
    print(f"Property:       {result['property'] or 'UNKNOWN'}")
    print(f"Unit:           {result['unit'] or 'UNKNOWN'}")
    print(f"Date:           {result['date'] or 'UNKNOWN'}")
    print(f"Account Number: {result['account_number'] or 'UNKNOWN'}")
    print(f"Service Addr:   {result['service_address'] or 'UNKNOWN'}")
    print("=====================================\n")

    return result
