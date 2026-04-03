# vendor_normalizer.py

from __future__ import annotations

import re


# Canonical vendor names mapped from many possible OCR / AI variants
VENDOR_NAME_MAP = {
    "PG&E": {
        "pg&e",
        "pge",
        "pacific gas and electric",
        "pacific gas and electric company",
        "pacific gas & electric",
        "pacific gas & electric company",
    },
    "Alameda Municipal Power": {
        "alameda municipal power",
        "amp",
        "a.m.p.",
    },
    "EBMUD": {
        "ebmud",
        "east bay municipal utility district",
    },
    "Comcast": {
        "comcast",
        "comcast business",
        "xfinity",
        "xfinity business",
    },
    "AT&T": {
        "at&t",
        "att",
        "at and t",
    },
    "Oakland Public Works": {
        "oakland public works",
        "city of oakland public works",
        "oakland pw",
    },
    "Alameda County Tax Collector": {
        "alameda county tax collector",
        "county of alameda tax collector",
        "alameda county taxes",
    },
    "State Farm": {
        "state farm",
        "state farm insurance",
    },
    "Farmers Insurance": {
        "farmers",
        "farmers insurance",
    },
}


def _clean_vendor_text(text: str) -> str:
    """
    Normalize formatting so OCR / AI variants compare more reliably.
    """
    if not text:
        return ""

    text = text.strip().lower()

    # Replace common punctuation variations
    text = text.replace("&", " and ")

    # Remove punctuation except spaces/alphanumerics
    text = re.sub(r"[^a-z0-9\s]", "", text)

    # Collapse repeated whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text


def normalize_vendor_name(vendor_name: str | None) -> str | None:
    """
    Return a canonical vendor name if a known alias is found.
    Otherwise return the original trimmed vendor name.
    """
    if vendor_name is None:
        return None

    original = vendor_name.strip()
    if not original:
        return original

    cleaned_input = _clean_vendor_text(original)

    for canonical_name, aliases in VENDOR_NAME_MAP.items():
        cleaned_aliases = {_clean_vendor_text(alias) for alias in aliases}
        cleaned_aliases.add(_clean_vendor_text(canonical_name))

        if cleaned_input in cleaned_aliases:
            return canonical_name

    return original