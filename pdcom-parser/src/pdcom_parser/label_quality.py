"""v0.4 label quality: garbage/place/per-commune blocklists, theme overrides,
typo fixes. Loaded once from configs/label_blocklists.yaml.

Used at extraction time (parser drops garbage before bronze) AND at silver
materialization (typos applied after, theme overrides reapplied for safety)."""
from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from pathlib import Path

import yaml


_CFG_PATH = Path(__file__).resolve().parents[2] / "configs" / "label_blocklists.yaml"


def _strip_accents(s: str) -> str:
    n = unicodedata.normalize("NFD", s)
    return "".join(c for c in n if unicodedata.category(c) != "Mn")


@lru_cache(maxsize=1)
def _load() -> dict:
    if not _CFG_PATH.exists():
        return {}
    with _CFG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache(maxsize=1)
def _compiled():
    cfg = _load()
    garbage = [re.compile(p, re.IGNORECASE) for p in cfg.get("garbage_label_patterns", [])]
    place = {p.strip().lower() for p in cfg.get("place_name_blocklist", []) if p}
    commune_basemap = {
        int(bfs): [s.strip().lower() for s in entries if s]
        for bfs, entries in (cfg.get("commune_basemap_blocklist") or {}).items()
    }
    theme_overrides = []
    for theme, patterns in (cfg.get("theme_overrides") or {}).items():
        for p in patterns:
            theme_overrides.append((re.compile(p, re.IGNORECASE), theme))
    typos = []
    for entry in cfg.get("typo_fixes") or []:
        typos.append((re.compile(entry["pattern"]), entry["replacement"]))
    return garbage, place, commune_basemap, theme_overrides, typos


def is_garbage_label(label: str) -> bool:
    """True if label matches any garbage regex (numbers, percentages, lonely parens)."""
    if not label:
        return True
    garbage, _, _, _, _ = _compiled()
    return any(p.search(label) for p in garbage)


def is_place_name(label: str) -> bool:
    """True if normalized label is a known place/neighborhood name."""
    if not label:
        return False
    _, place, _, _, _ = _compiled()
    norm = _strip_accents(label).lower().strip()
    norm = re.sub(r"[^a-z0-9\s]+", " ", norm)
    norm = re.sub(r"\s+", " ", norm).strip()
    return norm in place


def is_commune_basemap_label(commune_bfs: int, label: str) -> bool:
    """True if label looks like commune-specific basemap noise (e.g. Lancy commerce data)."""
    if not label:
        return False
    _, _, commune_basemap, _, _ = _compiled()
    entries = commune_basemap.get(int(commune_bfs))
    if not entries:
        return False
    norm = _strip_accents(label).lower()
    return any(s in norm for s in entries)


def should_drop_label(label: str, commune_bfs: int) -> tuple[bool, str | None]:
    """Combine all drop checks. Returns (drop, reason)."""
    if is_garbage_label(label):
        return True, "garbage_pattern"
    if is_place_name(label):
        return True, "place_name"
    if is_commune_basemap_label(commune_bfs, label):
        return True, "commune_basemap"
    return False, None


def override_theme(label: str, current_theme: str | None) -> str | None:
    """If the label matches an override regex, return the override theme;
    otherwise return current_theme."""
    if not label:
        return current_theme
    _, _, _, theme_overrides, _ = _compiled()
    norm = _strip_accents(label)
    for pattern, theme in theme_overrides:
        if pattern.search(norm):
            return theme
    return current_theme


def apply_typo_fixes(label: str) -> str:
    """Apply conservative typo fixes (only unambiguous, documented typos)."""
    if not label:
        return label
    _, _, _, _, typos = _compiled()
    out = label
    for pattern, replacement in typos:
        out = pattern.sub(replacement, out)
    return out


# v0.4: dangling-suffix words. A legend label ending with one of these is
# almost certainly truncated — the extractor should look for the next text
# fragment on the same legend row before storing.
DANGLING_SUFFIX_PATTERN = re.compile(
    r"\s+(?:à|de|du|des|et|en|au|aux|par|pour|sur|sous|la|le|les|liée?s?\s+à)\s*$",
    re.IGNORECASE,
)


def is_truncated(label: str) -> bool:
    """True if label ends with a connector word (likely line-wrapped legend entry)."""
    if not label:
        return False
    return bool(DANGLING_SUFFIX_PATTERN.search(label))
