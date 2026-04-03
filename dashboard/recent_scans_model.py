from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List

STATE_FILE = Path(__file__).resolve().parents[1] / "data" / "state" / "recent_scans.json"

def load_recent_scans() -> List[Dict[str, Any]]:
    if not STATE_FILE.exists():
        return []
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []

def save_recent_scan(item: Dict[str, Any], max_items: int = 20) -> None:
    items = load_recent_scans()
    items.insert(0, item)
    items = items[:max_items]
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(items, indent=2), encoding="utf-8")
