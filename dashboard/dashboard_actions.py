from __future__ import annotations
from dataclasses import asdict
from typing import Any, Dict
from dashboard.correction_manager import apply_corrections
from dashboard.recent_scans_model import save_recent_scan
from models.shared_models import ReviewPacket

def confirm_review(packet: ReviewPacket, edits: Dict[str, Any] | None = None) -> ReviewPacket:
    if edits:
        apply_corrections(packet.extraction, edits)
    packet.review_required = False
    save_recent_scan({
        "filename": packet.scan_job.filename,
        "document_type": packet.extraction.document_type,
        "vendor": packet.extraction.vendor,
        "amount": packet.extraction.amount,
        "property": packet.proposed_property,
        "summary": packet.extraction.summary,
        "status": "confirmed",
    })
    return packet

def reject_review(packet: ReviewPacket, reason: str = "") -> Dict[str, Any]:
    save_recent_scan({
        "filename": packet.scan_job.filename,
        "document_type": packet.extraction.document_type,
        "summary": f"Rejected: {reason}".strip(),
        "status": "rejected",
    })
    return {"status": "rejected", "reason": reason}
