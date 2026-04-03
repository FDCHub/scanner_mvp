from pathlib import Path
from datetime import datetime
import json


LOG_PATH = Path("D:/document_ai_system/scanner_mvp/logs/processing_log.jsonl")

def log_processing_event(event: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        **event,
    }

    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")