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

        # Pre-seed _seen with every PDF that already exists in Incoming at
        # startup time.  This means the watcher will ONLY trigger on files
        # that arrive AFTER the app starts — existing files are left as
        # "pending" in the queue for the user to process manually via
        # "Process This Doc" / "Process All Docs".
        self._seen: Set[str] = {
            str(p.resolve())
            for p in self.incoming_folder.glob("*.pdf")
            if p.is_file()
        }
        if self._seen:
            print(
                f"[Watcher] {len(self._seen)} pre-existing file(s) in Incoming "
                f"will appear as pending — not auto-processed."
            )

    def poll_once(self, callback: Callable[[ScanJob], None]) -> int:
        """
        Check Incoming for PDFs not yet seen.  Only files that were added
        AFTER __init__ ran (i.e. not in self._seen) are processed.
        Returns the number of new files dispatched to the callback.
        """
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