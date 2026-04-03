from __future__ import annotations
import time
from pathlib import Path
from config.app_config import AppConfig

def wait_until_file_ready(path: Path, retries: int | None = None, stable_wait: int | None = None) -> bool:
    retries = retries or AppConfig.FILE_READY_RETRIES
    stable_wait = stable_wait or AppConfig.FILE_STABLE_WAIT_SECONDS
    last_size = -1
    for _ in range(retries):
        try:
            current_size = path.stat().st_size
            with path.open("rb"):
                pass
            if current_size == last_size and current_size > 0:
                return True
            last_size = current_size
        except OSError:
            pass
        time.sleep(stable_wait)
    return False
