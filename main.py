# Internal modules
from account_matcher import find_similar_account
from processing_logger import log_processing_event
from vendor_normalizer import normalize_vendor_name

# Project modules
from startup.folder_initializer import ensure_required_folders
from startup.startup_diagnostics import run_startup_diagnostics
from intake.scan_intake_watcher import ScanIntakeWatcher

# Standard / external
from pathlib import Path
import shutil
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract

from document_intelligence import (
    process_ocr_text,
    rename_processed_file,
    route_processed_file,
)
from modules.reference_matcher import load_reference_table, reference_check

from chatgpt_analyzer import analyze_document
from new_doc_detector import is_new_document
from confidence_scorer import score_document
from user_review import review_extracted_data
from vendor_profile_store import load_vendor_profiles, upsert_vendor_profile
from csv_manager import (
    initialize_csv_files,
    upsert_reference_record,
    append_document_master_record,
)

def normalize_document_type(doc_type: str) -> str:
    if not doc_type:
        return ""

    doc_type_clean = str(doc_type).strip().lower()

    if doc_type_clean in {"utility_bill", "utility bill", "bill"}:
        return "bill"
    if doc_type_clean in {"invoice"}:
        return "invoice"
    if doc_type_clean in {"receipt"}:
        return "receipt"

    return doc_type_clean


def infer_vendor_category(result: dict) -> str:
    vendor_name = str(result.get("vendor") or result.get("vendor_name_normalized") or "").lower()
    doc_type = normalize_document_type(result.get("document_type", ""))

    handyman_keywords = [
        "handyman",
        "repair",
        "plumbing",
        "electric",
        "hvac",
        "maintenance",
        "contractor",
    ]

    utility_keywords = [
        "utility",
        "power",
        "water",
        "gas",
        "electricity",
        "comcast",
        "xfinity",
        "internet",
        "public utilities",
    ]

    if any(keyword in vendor_name for keyword in handyman_keywords):
        return "handyman services"

    if doc_type == "receipt":
        return "supplier/store"

    if doc_type == "invoice":
        return "service provider"

    if any(keyword in vendor_name for keyword in utility_keywords):
        return "utility"

    if doc_type == "bill":
        return "utility"

    return "other"


def build_vendor_profile(reviewed_data: dict) -> dict:
    vendor_name = reviewed_data.get("vendor_name_normalized", "") or reviewed_data.get("vendor", "")
    account_number = reviewed_data.get("account_number", "")
    property_name = reviewed_data.get("property", "")
    unit = reviewed_data.get("unit", "")
    vendor_category = infer_vendor_category(reviewed_data)
    doc_type = normalize_document_type(reviewed_data.get("document_type", ""))

    is_handyman = vendor_category == "handyman services"

    primary_match_method = "manual" if is_handyman else "account_number"
    fallback_match_method = "address" if not is_handyman else "vendor"
    requires_unit_selection = is_handyman

    known_accounts = {}
    if account_number:
        known_accounts[account_number] = {
            "property": property_name,
            "unit": unit,
            "category": vendor_category,
        }

    known_properties_used = [property_name] if property_name else []

    return {
        "vendor_name_normalized": vendor_name,
        "vendor_category": vendor_category,
        "document_types_seen": [doc_type] if doc_type else [],
        "primary_match_method": primary_match_method,
        "fallback_match_method": fallback_match_method,
        "requires_unit_selection": requires_unit_selection,
        "known_accounts": known_accounts,
        "known_properties_used": known_properties_used,
        "status": "provisional",
    }


def apply_ai_result_to_local_result(result: dict, ai_result: dict) -> dict:
    merged = result.copy()

    if ai_result.get("vendor_name_normalized"):
        merged["vendor"] = ai_result["vendor_name_normalized"]
    elif ai_result.get("vendor"):
        merged["vendor"] = ai_result["vendor"]

    if ai_result.get("account_number"):
        merged["account_number"] = ai_result["account_number"]

    if ai_result.get("bill_date"):
        merged["date"] = ai_result["bill_date"]
    elif ai_result.get("date"):
        merged["date"] = ai_result["date"]

    if ai_result.get("service_address"):
        merged["service_address"] = ai_result["service_address"]

    if ai_result.get("property"):
        merged["property"] = ai_result["property"]

    if "unit" in ai_result:
        merged["unit"] = ai_result.get("unit", "")

    if ai_result.get("document_type"):
        merged["document_type"] = normalize_document_type(ai_result["document_type"])

    if ai_result.get("description_of_charges"):
        merged["description_of_charges"] = ai_result["description_of_charges"]

    merged["category"] = infer_vendor_category(merged)

    return merged

def apply_vendor_profile_match(result: dict, vendor_profiles: dict) -> dict:
    vendor_name = str(result.get("vendor") or "").strip()
    account_number = str(result.get("account_number") or "").strip()

    if not vendor_name:
        return {
            "vendor_profile_found": False,
            "account_match_found": False,
            "property": "",
            "unit": "",
            "category": "",
            "vendor_profile": {},
        }

    profile = vendor_profiles.get(vendor_name, {})
    if not profile:
        return {
            "vendor_profile_found": False,
            "account_match_found": False,
            "property": "",
            "unit": "",
            "category": "",
            "vendor_profile": {},
        }

    account_match = profile.get("known_accounts", {}).get(account_number, {})

    return {
        "vendor_profile_found": True,
        "account_match_found": bool(account_match),
        "property": account_match.get("property", ""),
        "unit": account_match.get("unit", ""),
        "category": profile.get("vendor_category", ""),
        "vendor_profile": profile,
    }

def print_final_document_summary(
    result: dict,
    confidence: dict,
    new_doc: bool,
    chatgpt_used: bool,
    renamed_pdf_path,
) -> None:
    print("\n=== FINAL DOCUMENT SUMMARY ===")
    print(f"Vendor:         {result.get('vendor') or 'UNKNOWN'}")
    print(f"Document Type:  {result.get('document_type') or 'UNKNOWN'}")
    print(f"Property:       {result.get('property') or 'UNKNOWN'}")
    print(f"Unit:           {result.get('unit') or 'UNKNOWN'}")
    print(f"Date:           {result.get('date') or 'UNKNOWN'}")
    print(f"Account Number: {result.get('account_number') or 'UNKNOWN'}")
    print(f"Service Addr:   {result.get('service_address') or 'UNKNOWN'}")
    print(f"Category:       {result.get('category') or 'UNKNOWN'}")
    print(f"Confidence:     {confidence.get('score', 'UNKNOWN')}")
    print(f"Decision:       {confidence.get('decision', 'UNKNOWN')}")
    print(f"New Document:   {'yes' if new_doc else 'no'}")
    print(f"ChatGPT Used:   {'yes' if chatgpt_used else 'no'}")
    print(f"Final File:     {str(renamed_pdf_path) if renamed_pdf_path else 'NOT RENAMED'}")
    print("================================\n")

def handle_scan_job(scan_job, get_reference_table) -> None:
    source = scan_job.source_path
    working_folder = Path("D:/Scans/Working")
    error_folder = Path("D:/Scans/Error")
    destination = working_folder / source.name
    reference_table = get_reference_table()

    try:
        print(f"\nMoving scan to Working: {source.name}")

        shutil.move(str(source), str(destination))

        print(f"Moved to: {destination}")

        reader = PdfReader(str(destination))
        page_count = len(reader.pages)

        print(f"PDF contains {page_count} pages")

        images = convert_from_path(str(destination), dpi=300, first_page=1, last_page=1)
        ocr_text = pytesseract.image_to_string(images[0])

        ocr_text_path = destination.with_suffix(".txt")
        with open(ocr_text_path, "w", encoding="utf-8") as f:
            f.write(ocr_text)

        print(f"OCR text saved to: {ocr_text_path}")

        print("\n--- OCR PREVIEW ---\n")
        print(ocr_text[:500])
        print("\n-------------------\n")

        result = process_ocr_text(ocr_text_path)
        result["vendor"] = normalize_vendor_name(result.get("vendor"))
        result["document_type"] = normalize_document_type(result.get("document_type", ""))
        result["category"] = infer_vendor_category(result)

        extracted_data = {
            "vendor_name": result.get("vendor"),
            "account_number": result.get("account_number"),
            "service_address": result.get("service_address"),
            "property_name": result.get("property"),
            "unit": result.get("unit"),
            "category_guess": result.get("category", ""),
        }

        decision = reference_check(extracted_data, reference_table)
        print("Reference decision:", decision)

        if decision.get("property_name"):
            result["property"] = decision["property_name"]

        if decision.get("unit"):
            result["unit"] = decision["unit"]

        if decision.get("category"):
            result["category"] = decision["category"]

        vendor_profiles = load_vendor_profiles()
        vendor_profile_decision = apply_vendor_profile_match(result, vendor_profiles)
        print("Vendor profile decision:", vendor_profile_decision)

        if vendor_profile_decision.get("property") and not result.get("property"):
            result["property"] = vendor_profile_decision["property"]

        if vendor_profile_decision.get("unit") and not result.get("unit"):
            result["unit"] = vendor_profile_decision["unit"]

        if vendor_profile_decision.get("category"):
            result["category"] = vendor_profile_decision["category"]

        vendor_name = str(result.get("vendor") or "").strip()
        account_number = str(result.get("account_number") or "").strip()

        vendor_profile = vendor_profile_decision.get("vendor_profile", {})
        known_accounts = vendor_profile.get("known_accounts", {})

        similar_account_match = None
        similar_account_score = 0.0
        confirm_similar = ""

        is_new_vendor = not bool(vendor_profile) if vendor_name else True
        is_new_account = bool(vendor_profile) and bool(account_number) and account_number not in known_accounts

        if bool(vendor_profile) and bool(account_number) and account_number not in known_accounts:
            similar_account_match, similar_account_score = find_similar_account(
                account_number,
                known_accounts,
                threshold=0.85,
            )

        if similar_account_match:
            print("\nPossible OCR issue detected.")
            print(f"Vendor: {vendor_name}")
            print(f"Extracted account number: {account_number}")
            print(f"Closest known account: {similar_account_match}")
            print(f"Similarity: {similar_account_score:.0%}")

            confirm_similar = input(
                "Use existing known account instead? [y = yes / n = no, treat as new]: "
            ).strip().lower()

            if confirm_similar == "y":
                result["account_number"] = similar_account_match
                account_number = similar_account_match
                is_new_account = False

                if vendor_profile and similar_account_match in known_accounts:
                    matched_account_data = known_accounts[similar_account_match]

                    if not result.get("property") and matched_account_data.get("property"):
                        result["property"] = matched_account_data.get("property")

                    if not result.get("unit") and matched_account_data.get("unit"):
                        result["unit"] = matched_account_data.get("unit")

                    if not result.get("category") and matched_account_data.get("category"):
                        result["category"] = matched_account_data.get("category")

                print(f"Account number corrected to: {similar_account_match}")
            else:
                print("Keeping extracted account number and treating as new account.")

        reference_match_found = bool(
            decision.get("property_name") or decision.get("unit") or decision.get("category")
        )

        new_doc = is_new_document(vendor_name, account_number, reference_match_found)

        is_handyman = result.get("category") == "handyman services"

        account_match_flag = vendor_profile_decision.get("account_match_found", False)
        if similar_account_match and not is_new_account:
            account_match_flag = True

        confidence = score_document(
            account_match=account_match_flag,
            address_match=bool(result.get("property")),
            vendor_match=bool(vendor_name and vendor_profile),
            unit_match=bool(result.get("unit")),
            is_new_vendor=is_new_vendor,
            is_new_account=is_new_account,
            is_handyman=is_handyman,
            missing_property=not bool(result.get("property")),
            missing_unit=False,
        )

        print("Confidence:", confidence)
        print("Is new document:", new_doc)

        chatgpt_used = bool(new_doc or confidence["decision"] == "review")

        if new_doc or confidence["decision"] == "review":
            print("\nTriggering ChatGPT analysis...\n")

            try:
                ai_result = analyze_document(ocr_text)
                print("ChatGPT extracted data:", ai_result)
            except Exception as ai_error:
                print(f"ChatGPT failed: {ai_error}")

                log_processing_event({
                    "status": "chatgpt_error",
                    "source_file": source.name,
                    "error_message": str(ai_error),
                })

                ai_result = None

            if not ai_result:
                print("Skipping AI review and continuing with local extraction result.")
            else:
                raw_ai_vendor = (
                    ai_result.get("vendor_name_normalized")
                    or ai_result.get("vendor")
                    or ""
                )

                normalized_ai_vendor = normalize_vendor_name(raw_ai_vendor)

                if normalized_ai_vendor:
                    ai_result["vendor_name_normalized"] = normalized_ai_vendor
                    ai_result["vendor"] = normalized_ai_vendor

                ai_result["document_type"] = normalize_document_type(ai_result.get("document_type", ""))
                ai_result["vendor_status"] = (
                    "new vendor" if is_new_vendor else
                    "existing vendor with new account number" if is_new_account else
                    "existing vendor / existing account"
                )
                ai_result["vendor_category"] = infer_vendor_category(ai_result)

                reviewed = review_extracted_data(ai_result)

                if reviewed.get("user_confirmed") is True:
                    print("User confirmed extracted data.")

                    result = apply_ai_result_to_local_result(result, reviewed)

                    reference_record = {
                        "vendor_name": result.get("vendor", ""),
                        "account_number": result.get("account_number", ""),
                        "vendor_category": result.get("category", ""),
                        "property": result.get("property", ""),
                        "unit": result.get("unit", ""),
                        "service_address": result.get("service_address", ""),
                    }
                    upsert_reference_record(reference_record)
                    print("Updated reference_table.csv")

                    vendor_name_reviewed = reviewed.get("vendor_name_normalized", "") or reviewed.get("vendor", "")
                    if vendor_name_reviewed:
                        profile = build_vendor_profile(reviewed)
                        upsert_vendor_profile(vendor_name_reviewed, profile)
                        print(f"Saved vendor profile for: {vendor_name_reviewed}")
                else:
                    print("User did not confirm extracted data. Leaving local result unchanged.")

        renamed_pdf_path = rename_processed_file(ocr_text_path, result)

        final_storage_path = ""

        if renamed_pdf_path:
            final_storage_path = route_processed_file(renamed_pdf_path, result)

        print_final_document_summary(
            result=result,
            confidence=confidence,
            new_doc=new_doc,
            chatgpt_used=chatgpt_used,
            renamed_pdf_path=renamed_pdf_path
        )
        master_record = {
            "document_type": result.get("document_type", ""),
            "vendor_name": result.get("vendor", ""),
            "vendor_category": result.get("category", ""),
            "account_number": result.get("account_number", ""),
            "property": result.get("property", ""),
            "unit": result.get("unit", ""),
            "service_address": result.get("service_address", ""),
            "document_date": result.get("date", ""),
            "due_date": (
                result.get("due_date")
                or result.get("bill_due_date")
                or result.get("payment_due_date")
                or ""
            ),
            "amount_due": (
                result.get("amount_due")
                or result.get("total_amount_due")
                or result.get("total_due")
                or result.get("current_amount_due")
                or ""
            ),
            "source_file": source.name,
            "output_file": str(renamed_pdf_path) if renamed_pdf_path else "",
            "final_storage_path": str(final_storage_path) if final_storage_path else "",
            "confidence_score": confidence.get("score", ""),
            "chatgpt_used": chatgpt_used,
        }

        print("\n--- MASTER RECORD DEBUG ---")
        print(f"Due Date:           {master_record.get('due_date')}")
        print(f"Amount Due:         {master_record.get('amount_due')}")
        print(f"Final Storage Path: {master_record.get('final_storage_path')}")
        print("--------------------------------\n")

        master_logged = append_document_master_record(master_record)
        if master_logged:
            print("Appended document_master_log.csv")
        else:
            print("Skipped duplicate in document_master_log.csv")

        log_processing_event({
            "status": "success",
            "source_file": source.name,
            "working_file": destination.name,
            "final_vendor": result.get("vendor", ""),
            "final_account_number": result.get("account_number", ""),
            "final_property": result.get("property", ""),
            "final_unit": result.get("unit", ""),
            "final_category": result.get("category", ""),
            "document_type": result.get("document_type", ""),
            "reference_match_found": reference_match_found,
            "vendor_profile_found": vendor_profile_decision.get("vendor_profile_found", False),
            "account_match_found": account_match_flag,
            "fuzzy_account_match_found": bool(similar_account_match),
            "fuzzy_account_match_value": similar_account_match or "",
            "fuzzy_account_match_confirmed": confirm_similar == "y",
            "new_document": new_doc,
            "confidence_score": confidence.get("score"),
            "confidence_decision": confidence.get("decision"),
            "chatgpt_used": bool(new_doc or confidence["decision"] == "review"),
            "renamed_pdf_path": str(renamed_pdf_path) if renamed_pdf_path else "",
        })

    except Exception as e:
        print(f"\nERROR processing scan: {source.name}")
        print(f"Reason: {e}")

        log_processing_event({
            "status": "error",
            "source_file": source.name,
            "working_file": destination.name if 'destination' in locals() else "",
            "error_message": str(e),
        })

        try:
            error_folder.mkdir(parents=True, exist_ok=True)

            if destination.exists():
                error_destination = error_folder / destination.name
                shutil.move(str(destination), str(error_destination))
                print(f"Moved failed PDF to: {error_destination}")

            txt_path = destination.with_suffix(".txt")
            if txt_path.exists():
                error_txt_path = error_folder / txt_path.name
                shutil.move(str(txt_path), str(error_txt_path))
                print(f"Moved OCR text to: {error_txt_path}")

        except Exception as move_error:
            print(f"Secondary error while moving failed files: {move_error}")
    
def main() -> None:
    ensure_required_folders()
    report = run_startup_diagnostics()
    print(report.render_text())

    print("\nScanning Incoming folder...\n")

    print("CSV system: reference_table + master_log ready (backups enabled)")

    initialize_csv_files()

    watcher = ScanIntakeWatcher()
    watcher.run_forever(
        lambda job: handle_scan_job(
            job,
            lambda: load_reference_table("data/reference_table.csv")
        )
    )

if __name__ == "__main__":
    main()
