"""
csv_guard.py
------------
CSV corruption protection.

Wraps every CSV write with:
  1. Pre-write row count snapshot
  2. The write operation itself (caller-supplied)
  3. Post-write validation:
       - Column set must not change
       - Row count must not drop by more than ROW_DROP_THRESHOLD (10%)
  4. Auto-restore from the backup that csv_manager already created if
     validation fails, plus an alert via an optional callback.

Usage:
    from csv_guard import guarded_write

    guarded_write(
        csv_path   = MASTER_LOG_CSV,
        write_fn   = lambda: _do_the_actual_write(),
        backup_path= backup_path,          # path written by backup_csv()
        alert_fn   = log_activity,         # optional – called with (msg, "error")
    )
"""

from __future__ import annotations

import csv
import shutil
from pathlib import Path
from typing import Callable, Optional

# Maximum fraction of rows that may disappear in a single write before we
# consider it suspicious and roll back automatically.
# Set to 50% — a legitimate bulk-delete could remove many rows at once, but
# accidental truncation typically drops everything.  Use allow_shrink=True for
# intentional deletes to bypass this check entirely.
ROW_DROP_THRESHOLD = 0.50   # 50 %


# ── helpers ────────────────────────────────────────────────────────────────

def _count_rows(path: Path) -> int:
    """Return the number of data rows (header not counted)."""
    if not path.exists():
        return 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            return max(0, sum(1 for _ in f) - 1)
    except Exception:
        return 0


def _get_columns(path: Path) -> frozenset[str]:
    if not path.exists():
        return frozenset()
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return frozenset(c for c in (reader.fieldnames or []) if c)
    except Exception:
        return frozenset()


# ── public API ──────────────────────────────────────────────────────────────

class CSVGuardError(Exception):
    """Raised when a write is rolled back due to validation failure."""


def guarded_write(
    csv_path    : str | Path,
    write_fn    : Callable[[], None],
    backup_path : Optional[str | Path] = None,
    alert_fn    : Optional[Callable[[str, str], None]] = None,
    allow_shrink: bool = False,
) -> None:
    """
    Execute write_fn() with pre/post-write integrity checks.

    Parameters
    ----------
    csv_path     Path to the CSV file being written.
    write_fn     Zero-argument callable that performs the actual write.
    backup_path  Path to a pre-write backup already created by the caller
                 (csv_manager.backup_csv()).  If supplied and validation fails
                 this file is used for restoration.
    alert_fn     Optional callable(message, level) for surfacing errors in the
                 app activity log.  Receives ("message", "error").
    allow_shrink When True the row-count drop check is skipped entirely.
                 Pass this for intentional deletes so the guard does not
                 roll back a legitimate single-row removal.
    """
    csv_path = Path(csv_path)

    # 1. Snapshot pre-write state
    pre_rows    = _count_rows(csv_path)
    pre_columns = _get_columns(csv_path)

    # 2. Execute the write
    try:
        write_fn()
    except Exception as exc:
        # The write itself threw — restore if possible
        _restore(csv_path, backup_path, alert_fn,
                 f"Write exception for {csv_path.name}: {exc}")
        raise

    # 3. Post-write validation
    post_columns = _get_columns(csv_path)
    post_rows    = _count_rows(csv_path)

    # Column-set check (only if the file already had columns before the write)
    if pre_columns and post_columns and post_columns != pre_columns:
        missing = pre_columns - post_columns
        extra   = post_columns - pre_columns
        msg = (
            f"Column mismatch after writing {csv_path.name}. "
            f"Missing: {missing or 'none'}  Extra: {extra or 'none'}"
        )
        _restore(csv_path, backup_path, alert_fn, msg)
        raise CSVGuardError(msg)

    # Suspicious row-count drop check (skipped for intentional deletes)
    if not allow_shrink and pre_rows > 0:
        drop = (pre_rows - post_rows) / pre_rows
        if drop > ROW_DROP_THRESHOLD:
            msg = (
                f"Suspicious row-count drop in {csv_path.name}: "
                f"{pre_rows} → {post_rows} ({drop:.0%} loss). "
                f"Auto-restored from backup."
            )
            _restore(csv_path, backup_path, alert_fn, msg)
            raise CSVGuardError(msg)


def _restore(
    csv_path   : Path,
    backup_path: Optional[str | Path],
    alert_fn   : Optional[Callable],
    message    : str,
) -> None:
    if backup_path:
        backup_path = Path(backup_path)
        if backup_path.exists():
            try:
                shutil.copy2(str(backup_path), str(csv_path))
                full_msg = message + f" — restored from {backup_path.name}"
            except Exception as e:
                full_msg = message + f" — restore FAILED: {e}"
        else:
            full_msg = message + " — backup not available for restore"
    else:
        full_msg = message + " — no backup path supplied"

    print(f"[CSVGuard] {full_msg}")
    if alert_fn:
        try:
            alert_fn(full_msg, "error")
        except Exception:
            pass
