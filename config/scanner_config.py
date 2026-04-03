from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class ScanProfile:
    name: str
    file_type: str
    dpi: int
    color_mode: str
    source: str
    duplex: bool

class ScannerConfig:
    MAKE = "EPSON"
    MODEL = "ES-C220"
    CONNECTION_TYPE = "USB"
    SOURCES = ("ADF", "Flatbed")
    DEFAULT_FILE_TYPE = "pdf"
    DEFAULT_DPI = 300
    DEFAULT_COLOR_MODE = "grayscale"
    ADF_DUPLEX_DEFAULT = True

    PROFILES = {
        "standard_bill": ScanProfile("standard_bill", "pdf", 300, "grayscale", "ADF", True),
        "receipt": ScanProfile("receipt", "pdf", 300, "grayscale", "Flatbed", False),
        "legal_document": ScanProfile("legal_document", "pdf", 300, "grayscale", "ADF", True),
    }
