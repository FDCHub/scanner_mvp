from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

_STANDARD_ARCHIVE_CATEGORIES: List[str] = [
    "Utilities", "Financial", "Insurance", "Maintenance",
    "Repairs", "Permits", "Licenses", "Handyman", "NeedsReview",
]


@dataclass(frozen=True)
class PropertyRecord:
    code: str
    name: str
    city: str
    state: str
    archive_categories: List[str] = field(
        default_factory=lambda: list(_STANDARD_ARCHIVE_CATEGORIES)
    )

class AppConfig:
    PROJECT_NAME = "scanner_mvp"
    INCOMING_SCAN_FOLDER = Path(r"D:\Scans\Incoming")
    WORKING_FOLDER = Path(r"D:\Scans\Working")
    PROCESSED_FOLDER = Path(r"D:\Scans\Processed")
    ERROR_FOLDER = Path(r"D:\Scans\Error")
    DELETED_FOLDER = Path(r"D:\Scans\Deleted")
    ARCHIVE_ROOT = Path(r"D:\PropertyDocs")

    ACCEPTED_EXTENSIONS = {".pdf"}
    FILE_STABLE_WAIT_SECONDS = 3
    FILE_READY_RETRIES = 8
    POLL_INTERVAL_SECONDS = 2

    PROPERTIES: Dict[str, PropertyRecord] = {
        "central":  PropertyRecord("central",  "1423 Central Ave", "Oakland",          "CA"),
        "lincoln":  PropertyRecord("lincoln",  "3715 Lincoln Ave", "Oakland",          "CA"),
        "seamarsh": PropertyRecord("seamarsh", "3047 Sea Marsh Rd","Fernandina Beach", "FL"),
        "business": PropertyRecord("business", "Business",         "N/A",              "N/A"),
    }

    @classmethod
    def runtime_folders(cls) -> List[Path]:
        return [
            cls.INCOMING_SCAN_FOLDER,
            cls.WORKING_FOLDER,
            cls.PROCESSED_FOLDER,
            cls.ERROR_FOLDER,
            cls.DELETED_FOLDER,
            cls.ARCHIVE_ROOT,
        ]
