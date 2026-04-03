from __future__ import annotations
from pathlib import Path
from config.app_config import AppConfig

def ensure_required_folders() -> None:
    for folder in AppConfig.runtime_folders():
        Path(folder).mkdir(parents=True, exist_ok=True)

    for record in AppConfig.PROPERTIES.values():
        base = AppConfig.ARCHIVE_ROOT / record.name
        base.mkdir(parents=True, exist_ok=True)
        for category in record.archive_categories:
            (base / category).mkdir(parents=True, exist_ok=True)
