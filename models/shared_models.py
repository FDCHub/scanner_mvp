from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

@dataclass
class ScanJob:
    source_path: Path
    filename: str
    created_at: datetime
    file_size_bytes: int = 0
    source_type: str = "ADF"
    page_count: Optional[int] = None
    status: str = "detected"
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ExtractionResult:
    document_type: str = "unknown"
    vendor: Optional[str] = None
    amount: Optional[float] = None
    document_date: Optional[str] = None
    account_last4: Optional[str] = None
    property_hint: Optional[str] = None
    summary: str = ""
    ocr_text: str = ""
    fields: Dict[str, Any] = field(default_factory=dict)
    confidence: Dict[str, float] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)

@dataclass
class ReviewPacket:
    scan_job: ScanJob
    extraction: ExtractionResult
    preview_images: List[str] = field(default_factory=list)
    proposed_filename: Optional[str] = None
    proposed_property: Optional[str] = None
    proposed_category: Optional[str] = None
    review_required: bool = True
    recent_scan_items: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class ArchiveRecord:
    original_path: Path
    archive_path: Path
    archived_at: datetime
    document_type: str
    property_name: str
