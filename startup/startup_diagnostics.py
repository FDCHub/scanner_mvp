from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import List
from config.app_config import AppConfig
from config.scanner_config import ScannerConfig

@dataclass
class StartupDiagnosticsReport:
    ok: bool
    messages: List[str] = field(default_factory=list)

    def render_text(self) -> str:
        status = "OK" if self.ok else "ERROR"
        return "\n".join([f"Startup Status: {status}", *self.messages])

def run_startup_diagnostics() -> StartupDiagnosticsReport:
    messages = []
    ok = True
    for folder in AppConfig.runtime_folders():
        exists = Path(folder).exists()
        messages.append(f"{folder}: {'OK' if exists else 'MISSING'}")
        ok = ok and exists
    messages.append(f"Scanner: {ScannerConfig.MAKE} {ScannerConfig.MODEL} via {ScannerConfig.CONNECTION_TYPE}")
    return StartupDiagnosticsReport(ok=ok, messages=messages)
