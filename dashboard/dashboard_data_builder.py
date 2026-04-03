from __future__ import annotations
from models.shared_models import ExtractionResult, ReviewPacket, ScanJob
from dashboard.recent_scans_model import load_recent_scans
from dashboard.summary_generator import generate_summary

def build_review_packet(scan_job: ScanJob, preview_images: list[str], extraction: ExtractionResult) -> ReviewPacket:
    extraction.summary = extraction.summary or generate_summary(extraction)
    proposed_filename = None
    if extraction.vendor and extraction.document_date:
        safe_vendor = extraction.vendor.replace(" ", "_").replace("&", "and")
        proposed_filename = f"{safe_vendor}_{extraction.document_date}.pdf"
    return ReviewPacket(
        scan_job=scan_job,
        extraction=extraction,
        preview_images=preview_images,
        proposed_filename=proposed_filename,
        proposed_property=extraction.property_hint,
        proposed_category=extraction.document_type,
        recent_scan_items=load_recent_scans(),
    )
