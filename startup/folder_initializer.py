from __future__ import annotations
from pathlib import Path
from config.app_config import AppConfig

_MEDIA_CATEGORIES: list[str] = ["General", "Damage", "Before-After", "Ads", "Other"]

_PROPERTY_MEDIA_TREE: dict[str, list[str]] = {
    "1423 Central Ave": [
        "Unit A", "Unit B", "Unit C", "Unit D", "Unit E", "Unit F",
        "Unit G", "Unit H", "Unit I", "Unit J", "Unit K", "House",
    ],
    "3715 Lincoln Ave": [
        "Unit 1", "Unit 2", "Unit 3", "Unit 4", "Unit 5", "Unit 6",
        "Unit 7", "Unit 8", "Unit 9", "Unit 10", "House",
    ],
    "3047 Sea Marsh Rd": ["House"],
}

# Explicit list of all four PropertyDocs property folder names.
# Listed here so migrations run even if AppConfig.PROPERTIES changes.
_PROPERTY_FOLDER_NAMES: list[str] = [
    "1423 Central Ave",
    "3715 Lincoln Ave",
    "3047 Sea Marsh Rd",
    "Business",
]

# Standard category folders created under each property in PropertyDocs
_STANDARD_CATEGORIES: list[str] = [
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

# Old folder names that need to be migrated → new canonical names
# Checked in order — earlier entries run first.
_CATEGORY_MIGRATIONS: list[tuple[str, str]] = [
    ("utility",           "Utilities"),   # lowercase legacy name
    ("Utility",           "Utilities"),   # title-case legacy name
    ("handyman services", "Handyman"),    # old infer_vendor_category output
    ("Handyman Services", "Handyman"),    # title-case variant
    ("handyman",          "Handyman"),    # wrong capitalisation
]


def _create_space(space_dir: Path) -> None:
    """Create a property-media space directory with all five category subfolders."""
    for cat in _MEDIA_CATEGORIES:
        (space_dir / cat).mkdir(parents=True, exist_ok=True)


def _migrate_old_category(prop_dir: Path, old_name: str, new_name: str) -> None:
    """
    If old_name exists under prop_dir and differs from new_name, move its
    contents into new_name and remove the old folder.
    Skips individual items that already exist in the destination.
    """
    old_dir = prop_dir / old_name
    new_dir = prop_dir / new_name
    # Nothing to do if the old folder doesn't exist, or it IS the new folder
    # (e.g. "Handyman" → "Handyman" would be a no-op but we guard anyway).
    if not old_dir.exists() or old_dir == new_dir:
        return
    new_dir.mkdir(parents=True, exist_ok=True)
    for item in list(old_dir.iterdir()):
        dest = new_dir / item.name
        if dest.exists():
            print(f"[FolderInit]   Skipping '{item.name}' — already exists in {new_name}/")
        else:
            item.rename(dest)
    try:
        old_dir.rmdir()
        print(f"[FolderInit] Migrated {old_name} → {new_name} in {prop_dir.name}")
    except OSError:
        print(f"[FolderInit] WARNING: '{old_name}/' not empty after migration in {prop_dir.name} — manual review needed")


def _ensure_property_folders(prop_dir: Path) -> None:
    """Run migrations then create all standard folders under a single property dir."""
    prop_dir.mkdir(parents=True, exist_ok=True)

    # 1. Migrate legacy folder names first so they aren't re-created as old names
    for old_name, new_name in _CATEGORY_MIGRATIONS:
        _migrate_old_category(prop_dir, old_name, new_name)

    # 2. Create all standard category folders (idempotent)
    for cat in _STANDARD_CATEGORIES:
        target = prop_dir / cat
        if not target.exists():
            target.mkdir()
            print(f"[FolderInit] Created {cat} in {prop_dir.name}")

    # 3. Ensure Financial/Bank Statements/ exists
    bank_statements = prop_dir / "Financial" / "Bank Statements"
    if not bank_statements.exists():
        bank_statements.mkdir(parents=True)
        print(f"[FolderInit] Created Financial/Bank Statements in {prop_dir.name}")


def _migrate_property_folder(archive_root: Path, old_name: str, new_name: str) -> None:
    """
    Rename a property-level folder under archive_root (e.g. '3047 Sea Marsh' →
    '3047 Sea Marsh Rd').  Moves all contents item-by-item so it works even when
    source and destination are on the same drive without a true rename being
    available, and skips individual items that already exist in the destination.
    """
    old_dir = archive_root / old_name
    new_dir = archive_root / new_name
    if not old_dir.exists() or old_dir == new_dir:
        return
    new_dir.mkdir(parents=True, exist_ok=True)
    for item in list(old_dir.iterdir()):
        dest = new_dir / item.name
        if dest.exists():
            print(f"[FolderInit]   Skipping '{item.name}' — already exists in {new_name}/")
        else:
            item.rename(dest)
    try:
        old_dir.rmdir()
        print(f"[FolderInit] Migrated property folder: {old_name} → {new_name}")
    except OSError:
        print(f"[FolderInit] WARNING: '{old_name}/' not empty after migration — manual review needed")


def _cleanup_root_level_categories(archive_root: Path) -> None:
    """
    Remove any category folders that ended up directly under archive_root instead
    of inside a property sub-folder (caused by an older version of this module).
    Empty folders are deleted; non-empty ones are left and a warning is printed.
    """
    for cat in _STANDARD_CATEGORIES:
        rogue = archive_root / cat
        if not rogue.exists():
            continue
        contents = list(rogue.iterdir())
        if not contents:
            rogue.rmdir()
            print(f"[FolderInit] Removed misplaced root-level folder: {cat}/")
        else:
            print(
                f"[FolderInit] WARNING: '{cat}/' exists directly under PropertyDocs/ "
                f"with {len(contents)} item(s) — manual review needed before deletion"
            )


def ensure_required_folders() -> None:
    # ── 1. Runtime scan folders (Incoming, Working, Processed, Error, Deleted) ──
    for folder in AppConfig.runtime_folders():
        Path(folder).mkdir(parents=True, exist_ok=True)

    # ── 2. PropertyDocs root ──────────────────────────────────────────────────
    archive_root = AppConfig.ARCHIVE_ROOT
    archive_root.mkdir(parents=True, exist_ok=True)

    # 2a. Property-level folder renames (old name → canonical name)
    _migrate_property_folder(archive_root, "3047 Sea Marsh", "3047 Sea Marsh Rd")

    # 2b. Clean up any category folders that were incorrectly created at the root
    _cleanup_root_level_categories(archive_root)

    # 2c. Create / migrate category structure for each property
    for prop_name in _PROPERTY_FOLDER_NAMES:
        _ensure_property_folders(archive_root / prop_name)

    # ── 3. PropertyMedia tree ─────────────────────────────────────────────────
    media_root = Path("D:/PropertyMedia")
    media_root.mkdir(parents=True, exist_ok=True)
    for prop_name, spaces in _PROPERTY_MEDIA_TREE.items():
        prop_dir = media_root / prop_name
        prop_dir.mkdir(parents=True, exist_ok=True)
        _create_space(prop_dir / "_Building")
        for space in spaces:
            _create_space(prop_dir / space)
