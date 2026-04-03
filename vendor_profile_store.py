from vendor_normalizer import normalize_vendor_name
import json
import os
import shutil
from datetime import datetime

VENDOR_PROFILE_PATH = "D:/document_ai_system/scanner_mvp/data/vendor_profiles.json"
BACKUP_DIR = "D:/document_ai_system/scanner_mvp/backups"

def ensure_vendor_profiles_file():
    if not os.path.exists(VENDOR_PROFILE_PATH):
        with open(VENDOR_PROFILE_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)


def load_vendor_profiles():
    ensure_vendor_profiles_file()
    with open(VENDOR_PROFILE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def backup_vendor_profiles():
    ensure_vendor_profiles_file()
    os.makedirs(BACKUP_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"vendor_profiles_{timestamp}.json")

    shutil.copy2(VENDOR_PROFILE_PATH, backup_path)
    return backup_path


def save_vendor_profiles(profiles: dict):
    ensure_vendor_profiles_file()
    backup_vendor_profiles()

    temp_path = f"{VENDOR_PROFILE_PATH}.tmp"

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2)

    os.replace(temp_path, VENDOR_PROFILE_PATH)


def upsert_vendor_profile(vendor_name: str, profile_data: dict):
    profiles = load_vendor_profiles()

    canonical_vendor_name = normalize_vendor_name(vendor_name or "")
    if not canonical_vendor_name:
        return

    profiles[canonical_vendor_name] = profile_data

    save_vendor_profiles(profiles)