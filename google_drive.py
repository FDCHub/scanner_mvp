"""
google_drive.py
---------------
Google Drive integration for Scanner MVP.

Setup (one-time):
  1. Go to https://console.cloud.google.com
  2. Create a project, enable the Google Drive API
  3. Create OAuth2 credentials (Desktop app type)
  4. Download the JSON and save it as google_credentials.json in the project root
  5. Set GOOGLE_CREDENTIALS_FILE and GOOGLE_TOKEN_FILE in .env (or accept defaults)
  6. On first startup, a browser window will open for you to authorise the app

After that, the token is cached in google_token.json and refreshed automatically.
"""

from __future__ import annotations

import json
import os
import queue as _queue_module
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from dotenv import load_dotenv

load_dotenv()

# ── optional dependency guard ──────────────────────────────────────────────
try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
    _GOOGLE_LIBS_AVAILABLE = True
except ImportError:
    _GOOGLE_LIBS_AVAILABLE = False


# ── configuration ──────────────────────────────────────────────────────────
SCOPES           = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
TOKEN_FILE       = os.getenv("GOOGLE_TOKEN_FILE",       "google_token.json")
SYNC_QUEUE_FILE  = Path("data/sync_queue.json")
DAILY_BACKUP_DIR = Path("backups")

# Google Drive folder names
_DRIVE_PROPERTY_DOCS = "PropertyDocs"
_DRIVE_APP_DATA      = "AppData"
_DRIVE_BACKUPS       = "backups"

# Property sub-folders that must exist under PropertyDocs/
_PROPERTY_FOLDERS = [
    "1423 Central Ave",
    "3715 Lincoln Ave",
    "3047 Sea Marsh Rd",
    "Business",
]

# Standard category folders created under each property
_DRIVE_STANDARD_CATEGORIES = [
    "Utilities",
    "Financial",
    "Insurance",
    "Maintenance",
    "Repairs",
    "Permits",
    "Licenses",
    "Handyman",
    "NeedsReview",
]

# Organisational sub-folders inside Financial/ (mirroring local structure)
_DRIVE_FINANCIAL_SUBFOLDERS = [
    "Bank Statements",
    "Tax Documents",
    "Accounting",
    "Loans & Mortgages",
]

# Local AppData files to keep in sync
_APP_DATA_FILES = [
    Path("data/document_master_log.csv"),
    Path("data/reference_table.csv"),
    Path("config.json"),
    Path("data/vendor_profiles.json"),
]


# ── status constants ────────────────────────────────────────────────────────
class SyncStatus:
    SYNCED       = "synced"
    SYNCING      = "syncing"
    FAILED       = "failed"
    OFFLINE      = "offline"
    UNCONFIGURED = "unconfigured"


# ═══════════════════════════════════════════════════════════════════════════
class GoogleDriveSync:
    """
    Thread-safe Google Drive sync manager.

    Usage:
        drive_sync = GoogleDriveSync()
        drive_sync.start_worker()          # call once at app startup
        drive_sync.queue_pdf_upload(path, "1423 Central Ave")
        drive_sync.queue_appdata_upload(Path("data/document_master_log.csv"))
    """

    def __init__(self):
        self._service       : object | None = None
        self._service_lock  = threading.Lock()

        self._folder_ids    : dict[str, str] = {}  # "PropertyDocs/1423 Central Ave" -> drive id
        self._folder_lock   = threading.Lock()

        self._upload_queue  = _queue_module.Queue()
        self._status        = SyncStatus.UNCONFIGURED if not self._is_configured() else SyncStatus.OFFLINE
        self._status_lock   = threading.Lock()
        self._syncing_count = 0
        self._last_error    = ""

        self._pending       : list[dict] = []   # persisted-queue items from previous sessions
        self._pending_lock  = threading.Lock()

        DAILY_BACKUP_DIR.mkdir(exist_ok=True)
        SYNC_QUEUE_FILE.parent.mkdir(exist_ok=True)
        self._load_persisted_queue()

    # ── public API ──────────────────────────────────────────────────────────

    def queue_pdf_upload(self, local_path: Path, property_name: str,
                          category: str = "", account_code: str = "") -> None:
        """Non-blocking: queue a PDF for upload to PropertyDocs/<prop>/<category>/<account_code>/."""
        self._enqueue({
            "type":         "pdf",
            "path":         str(local_path),
            "property":     property_name,
            "category":     category,
            "account_code": account_code,
            "filename":     local_path.name,
        })

    def queue_appdata_upload(self, local_path: Path) -> None:
        """Non-blocking: queue a data file for upload to AppData/."""
        self._enqueue({"type": "appdata", "path": str(local_path),
                       "filename": local_path.name})

    def queue_backup_upload(self, local_path: Path) -> None:
        """Non-blocking: queue a backup file for upload to AppData/backups/."""
        self._enqueue({"type": "backup", "path": str(local_path),
                       "filename": local_path.name})

    def queue_file_delete(self, local_filed_path: str) -> None:
        """
        Non-blocking: queue a Drive file move to PropertyDocs/Deleted/.

        local_filed_path is the local final_storage_path stored in the master
        log, e.g. D:/PropertyDocs/3047 Sea Marsh Rd/Utilities/Water/bill.pdf.
        The Drive folder is derived by replacing the local PropertyDocs root
        with the Drive 'PropertyDocs/' prefix.
        """
        p = Path(local_filed_path)
        try:
            rel         = p.relative_to(Path("D:/PropertyDocs"))
            drive_folder = "PropertyDocs/" + str(rel.parent).replace("\\", "/")
        except ValueError:
            # Path not under PropertyDocs — skip Drive delete
            print(f"[Drive] queue_file_delete: path not under PropertyDocs, skipping: {p}")
            return
        self._enqueue({
            "type":         "delete",
            "path":         "",          # no local file to check
            "drive_folder": drive_folder,
            "filename":     p.name,
        })

    def get_status(self) -> dict:
        with self._status_lock:
            status = self._status
            error  = self._last_error
        with self._pending_lock:
            pending = len(self._pending)
        return {
            "status":        status,
            "pending_count": pending,
            "last_error":    error,
            "configured":    self._is_configured(),
            "queue_depth":   self._upload_queue.qsize(),
        }

    def retry_failed(self) -> None:
        """Re-enqueue all persisted (previously failed) items."""
        with self._pending_lock:
            items = list(self._pending)
        for item in items:
            self._upload_queue.put(item)
        self._set_status(SyncStatus.SYNCING)
        print(f"[Drive] Retrying {len(items)} failed upload(s)")

    def startup_sync(self) -> None:
        """Queue all AppData files so they are uploaded on startup."""
        for f in _APP_DATA_FILES:
            if f.exists():
                self.queue_appdata_upload(f)

    def start_worker(self) -> None:
        """Start the background upload worker. Call once at app startup."""
        if not self._is_configured():
            print("[Drive] google_credentials.json not found — Drive sync disabled. "
                  "See google_drive.py docstring for setup instructions.")
            self._set_status(SyncStatus.UNCONFIGURED)
            return
        t = threading.Thread(target=self._worker_loop, daemon=True, name="drive-sync")
        t.start()
        print("[Drive] Background sync worker started")

    # ── internal worker ─────────────────────────────────────────────────────

    def _worker_loop(self) -> None:
        # Initialise folder structure on first run (will retry if offline)
        self._try_ensure_folders()
        # Drain persisted queue from previous sessions
        self._replay_persisted_queue()

        while True:
            try:
                item = self._upload_queue.get(timeout=10)
            except _queue_module.Empty:
                continue
            try:
                self._process_item(item)
            except Exception as e:
                print(f"[Drive] Unexpected worker error: {e}")
            finally:
                self._upload_queue.task_done()

    def _try_ensure_folders(self) -> None:
        for attempt in range(5):
            try:
                self._ensure_folder_structure()
                self._set_status(SyncStatus.SYNCED)
                return
            except Exception as e:
                wait = min(30, 5 * (attempt + 1))
                print(f"[Drive] Folder init failed ({e}), retrying in {wait}s…")
                self._set_status(SyncStatus.OFFLINE)
                time.sleep(wait)
        print("[Drive] Could not connect to Google Drive after 5 attempts — operating offline")

    def _process_item(self, item: dict) -> None:
        self._inc_syncing()
        try:
            item_type = item.get("type", "")

            # Delete items have no local file to check — handle separately
            if item_type != "delete":
                local_path = Path(item["path"])
                if not local_path.exists():
                    print(f"[Drive] Skipping missing file: {local_path}")
                    self._remove_from_persisted(item)
                    return

            svc = self._get_service()

            if item_type == "pdf":
                prop         = item["property"]
                category     = item.get("category", "")
                account_code = item.get("account_code", "")

                # Ensure top-level property folder exists
                prop_key   = f"PropertyDocs/{prop}"
                prop_fid   = self._get_folder_id(prop_key)
                if not prop_fid:
                    self._ensure_folder_structure()
                    prop_fid = self._get_folder_id(prop_key)
                if not prop_fid:
                    raise RuntimeError(f"No Drive folder for property: {prop!r}")

                # Ensure category sub-folder
                folder_id   = prop_fid
                drive_path  = f"PropertyDocs/{prop}"
                if category:
                    cat_key = f"{drive_path}/{category}"
                    cat_fid = self._get_folder_id(cat_key)
                    if not cat_fid:
                        cat_fid = self._get_or_create_folder(category, parent_id=prop_fid)
                        with self._folder_lock:
                            self._folder_ids[cat_key] = cat_fid
                    folder_id  = cat_fid
                    drive_path = cat_key

                # Ensure account_code sub-folder
                if account_code:
                    ac_key = f"{drive_path}/{account_code}"
                    ac_fid = self._get_folder_id(ac_key)
                    if not ac_fid:
                        ac_fid = self._get_or_create_folder(account_code, parent_id=folder_id)
                        with self._folder_lock:
                            self._folder_ids[ac_key] = ac_fid
                    folder_id  = ac_fid
                    drive_path = ac_key

                self._upload(local_path, folder_id, item["filename"])
                print(f"[Drive] ✓ {item['filename']} → {drive_path}/")

            elif item_type == "appdata":
                folder_id = self._get_folder_id("AppData")
                self._upload(local_path, folder_id, item["filename"])
                print(f"[Drive] ✓ {item['filename']} → AppData/")

            elif item_type == "backup":
                folder_id = self._get_folder_id("AppData/backups")
                self._upload(local_path, folder_id, item["filename"])
                print(f"[Drive] ✓ {item['filename']} → AppData/backups/")

            elif item_type == "delete":
                drive_folder = item.get("drive_folder", "")
                filename     = item["filename"]

                # Locate the file in Drive — search in its original folder if cached
                parent_fid = self._get_folder_id(drive_folder) if drive_folder else None
                if parent_fid:
                    q = (f"name='{filename}' and '{parent_fid}' in parents "
                         f"and trashed=false")
                else:
                    # Folder not in cache — search by name across entire Drive
                    q = f"name='{filename}' and trashed=false"

                results = svc.files().list(
                    q=q, fields="files(id,parents)", pageSize=5
                ).execute()
                matches = results.get("files", [])

                if not matches:
                    print(f"[Drive] Delete: '{filename}' not found in Drive — skipping")
                    self._remove_from_persisted(item)
                    # Fall through to success path so status resets correctly
                else:
                    file_id         = matches[0]["id"]
                    current_parents = ",".join(matches[0].get("parents", []))

                    # Ensure PropertyDocs/Deleted/ folder exists
                    deleted_fid = self._get_folder_id("PropertyDocs/Deleted")
                    if not deleted_fid:
                        prop_docs_fid = self._get_folder_id("PropertyDocs")
                        if not prop_docs_fid:
                            self._ensure_folder_structure()
                            prop_docs_fid = self._get_folder_id("PropertyDocs")
                        deleted_fid = self._get_or_create_folder(
                            "Deleted", parent_id=prop_docs_fid
                        )
                        with self._folder_lock:
                            self._folder_ids["PropertyDocs/Deleted"] = deleted_fid

                    svc.files().update(
                        fileId=file_id,
                        addParents=deleted_fid,
                        removeParents=current_parents,
                        fields="id,parents",
                    ).execute()
                    print(f"[Drive] ✓ Moved '{filename}' → PropertyDocs/Deleted/")

            self._remove_from_persisted(item)
            with self._status_lock:
                self._syncing_count = max(0, self._syncing_count - 1)
                if self._syncing_count == 0:
                    self._status = SyncStatus.SYNCED
                    self._last_error = ""

        except Exception as e:
            self._last_error = str(e)
            is_offline = any(kw in str(e).lower() for kw in
                             ("connect", "timeout", "network", "ssl", "socket", "unreachable"))
            new_status = SyncStatus.OFFLINE if is_offline else SyncStatus.FAILED
            print(f"[Drive] Upload failed ({item.get('filename', '?')}): {e}")
            self._add_to_persisted(item)
            with self._status_lock:
                self._syncing_count = max(0, self._syncing_count - 1)
                self._status = new_status

    # ── Google Drive API helpers ─────────────────────────────────────────────

    def _is_configured(self) -> bool:
        return _GOOGLE_LIBS_AVAILABLE and Path(CREDENTIALS_FILE).exists()

    def _get_service(self):
        with self._service_lock:
            if self._service:
                return self._service
            if not _GOOGLE_LIBS_AVAILABLE:
                raise RuntimeError(
                    "google-api-python-client is not installed. "
                    "Run: pip install google-api-python-client google-auth-oauthlib"
                )
            creds_path = Path(CREDENTIALS_FILE)
            token_path = Path(TOKEN_FILE)
            if not creds_path.exists():
                raise FileNotFoundError(
                    f"Google credentials not found at: {CREDENTIALS_FILE}\n"
                    "See google_drive.py docstring for setup instructions."
                )
            creds = None
            if token_path.exists():
                creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
                    creds = flow.run_local_server(port=0)
                with open(str(token_path), "w", encoding="utf-8") as tf:
                    tf.write(creds.to_json())
            self._service = build("drive", "v3", credentials=creds)
            return self._service

    def _get_or_create_folder(self, name: str, parent_id: Optional[str] = None) -> str:
        svc     = self._get_service()
        q_parts = [
            f"name='{name}'",
            "mimeType='application/vnd.google-apps.folder'",
            "trashed=false",
        ]
        if parent_id:
            q_parts.append(f"'{parent_id}' in parents")
        results  = svc.files().list(q=" and ".join(q_parts), fields="files(id)").execute()
        existing = results.get("files", [])
        if existing:
            return existing[0]["id"]
        meta: dict = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_id:
            meta["parents"] = [parent_id]
        created = svc.files().create(body=meta, fields="id").execute()
        return created["id"]

    def _ensure_folder_structure(self) -> None:
        prop_docs_id = self._get_or_create_folder(_DRIVE_PROPERTY_DOCS)
        app_data_id  = self._get_or_create_folder(_DRIVE_APP_DATA)
        backups_id   = self._get_or_create_folder(_DRIVE_BACKUPS, parent_id=app_data_id)
        deleted_id   = self._get_or_create_folder("Deleted", parent_id=prop_docs_id)

        with self._folder_lock:
            self._folder_ids["PropertyDocs"]         = prop_docs_id
            self._folder_ids["AppData"]              = app_data_id
            self._folder_ids["AppData/backups"]      = backups_id
            self._folder_ids["PropertyDocs/Deleted"] = deleted_id

        for prop in _PROPERTY_FOLDERS:
            prop_fid = self._get_or_create_folder(prop, parent_id=prop_docs_id)
            prop_key = f"PropertyDocs/{prop}"
            with self._folder_lock:
                self._folder_ids[prop_key] = prop_fid

            # Standard category folders under each property
            for cat in _DRIVE_STANDARD_CATEGORIES:
                cat_fid = self._get_or_create_folder(cat, parent_id=prop_fid)
                cat_key = f"{prop_key}/{cat}"
                with self._folder_lock:
                    self._folder_ids[cat_key] = cat_fid

                # Organisational sub-folders inside Financial/
                if cat == "Financial":
                    for sub in _DRIVE_FINANCIAL_SUBFOLDERS:
                        sub_fid = self._get_or_create_folder(sub, parent_id=cat_fid)
                        with self._folder_lock:
                            self._folder_ids[f"{cat_key}/{sub}"] = sub_fid

        print("[Drive] Folder structure verified/created")

    def _get_folder_id(self, key: str) -> Optional[str]:
        with self._folder_lock:
            return self._folder_ids.get(key)

    def _upload(self, local_path: Path, folder_id: str, filename: str) -> str:
        """Upload or update a file in a Drive folder. Returns Drive file ID."""
        svc   = self._get_service()
        ext   = local_path.suffix.lower()
        if ext == ".pdf":
            mime = "application/pdf"
        elif ext == ".json":
            mime = "application/json"
        else:
            mime = "text/plain"

        # Check for existing file to update rather than duplicate
        q        = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        existing = svc.files().list(q=q, fields="files(id)").execute().get("files", [])
        media    = MediaFileUpload(str(local_path), mimetype=mime, resumable=True)

        if existing:
            result = svc.files().update(
                fileId=existing[0]["id"], media_body=media, fields="id"
            ).execute()
        else:
            meta   = {"name": filename, "parents": [folder_id]}
            result = svc.files().create(body=meta, media_body=media, fields="id").execute()

        return result["id"]

    # ── persisted queue ─────────────────────────────────────────────────────

    def _enqueue(self, item: dict) -> None:
        self._upload_queue.put(item)

    def _load_persisted_queue(self) -> None:
        if not SYNC_QUEUE_FILE.exists():
            return
        try:
            with open(SYNC_QUEUE_FILE, "r", encoding="utf-8") as f:
                self._pending = json.load(f)
            if self._pending:
                print(f"[Drive] {len(self._pending)} item(s) pending sync from previous session")
        except Exception:
            self._pending = []

    def _save_persisted_queue(self) -> None:
        try:
            with open(SYNC_QUEUE_FILE, "w", encoding="utf-8") as f:
                json.dump(self._pending, f, indent=2)
        except Exception as e:
            print(f"[Drive] Could not save sync queue: {e}")

    def _add_to_persisted(self, item: dict) -> None:
        with self._pending_lock:
            # Avoid duplicating the same file in the queue
            key = (item.get("type"), item.get("path"), item.get("filename"))
            if not any(
                (i.get("type"), i.get("path"), i.get("filename")) == key
                for i in self._pending
            ):
                self._pending.append(item)
                self._save_persisted_queue()

    def _remove_from_persisted(self, item: dict) -> None:
        with self._pending_lock:
            key = (item.get("type"), item.get("path"), item.get("filename"))
            self._pending = [
                i for i in self._pending
                if (i.get("type"), i.get("path"), i.get("filename")) != key
            ]
            self._save_persisted_queue()

    def _replay_persisted_queue(self) -> None:
        with self._pending_lock:
            items = list(self._pending)
        if items:
            print(f"[Drive] Re-queuing {len(items)} item(s) from previous session")
            for item in items:
                self._upload_queue.put(item)

    # ── status helpers ───────────────────────────────────────────────────────

    def _set_status(self, status: str) -> None:
        with self._status_lock:
            self._status = status

    def _inc_syncing(self) -> None:
        with self._status_lock:
            self._syncing_count += 1
            self._status = SyncStatus.SYNCING


# ═══════════════════════════════════════════════════════════════════════════
# Daily backup helper (standalone — used by app.py)
# ═══════════════════════════════════════════════════════════════════════════

_BACKUP_SOURCES = [
    Path("data/document_master_log.csv"),
    Path("data/reference_table.csv"),
]
_LAST_BACKUP_FILE = Path("data/last_backup_date.txt")


def do_daily_backup(drive_sync_instance: Optional[GoogleDriveSync] = None) -> list[Path]:
    """
    Create dated local backup copies of CSV files.
    Prune backups older than 30 days.
    Optionally upload backups to Google Drive.
    Returns list of newly created backup Paths.
    """
    DAILY_BACKUP_DIR.mkdir(exist_ok=True)
    today   = datetime.now().strftime("%Y-%m-%d")
    created : list[Path] = []

    for src in _BACKUP_SOURCES:
        if not src.exists():
            continue
        dest = DAILY_BACKUP_DIR / f"{src.stem}_{today}{src.suffix}"
        if not dest.exists():
            shutil.copy2(str(src), str(dest))
            created.append(dest)
            print(f"[Backup] Created daily snapshot: {dest.name}")
            if drive_sync_instance:
                drive_sync_instance.queue_backup_upload(dest)

    # Prune backups older than 30 days
    cutoff = datetime.now().timestamp() - 30 * 86400
    for old in DAILY_BACKUP_DIR.iterdir():
        if old.is_file() and old.stat().st_mtime < cutoff:
            try:
                old.unlink()
                print(f"[Backup] Pruned old backup: {old.name}")
            except Exception:
                pass

    # Record date so we don't repeat today
    try:
        _LAST_BACKUP_FILE.write_text(today, encoding="utf-8")
    except Exception:
        pass

    return created


def should_run_daily_backup() -> bool:
    """Return True if no backup has been run today yet."""
    if not _LAST_BACKUP_FILE.exists():
        return True
    try:
        last = _LAST_BACKUP_FILE.read_text(encoding="utf-8").strip()
        return last != datetime.now().strftime("%Y-%m-%d")
    except Exception:
        return True


# ── module-level singleton ────────────────────────────────────────────────
drive_sync = GoogleDriveSync()
