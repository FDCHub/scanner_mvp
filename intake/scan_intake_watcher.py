from __future__ import annotations

import time
from pathlib import Path
from typing import Callable, Set

from config.app_config import AppConfig
from intake.file_ready_checker import wait_until_file_ready
from intake.scan_job_loader import build_scan_job
from models.shared_models import ScanJob


class ScanIntakeWatcher:
    def __init__(self, incoming_folder: Path | None = None):
        self.incoming_folder = incoming_folder or AppConfig.INCOMING_SCAN_FOLDER
        self._seen: Set[str] = set()

    def poll_once(self, callback: Callable[[ScanJob], None]) -> int:
        count = 0

        for path in sorted(self.incoming_folder.glob("*.pdf")):
            key = str(path.resolve())

            if key in self._seen:
                continue

            if wait_until_file_ready(path):
                self._seen.add(key)
                callback(build_scan_job(path))
                count += 1

        return count

    def run_forever(self, callback: Callable[[ScanJob], None]) -> None:
        while True:
            self.poll_once(callback)
            time.sleep(AppConfig.POLL_INTERVAL_SECONDS)