from __future__ import annotations
from datetime import datetime
from pathlib import Path
from models.shared_models import ScanJob

def build_scan_job(path: str | Path) -> ScanJob:
    p = Path(path)
    stat = p.stat()
    return ScanJob(
        source_path=p,
        filename=p.name,
        created_at=datetime.fromtimestamp(stat.st_ctime),
        file_size_bytes=stat.st_size,
        metadata={"extension": p.suffix.lower()},
    )
