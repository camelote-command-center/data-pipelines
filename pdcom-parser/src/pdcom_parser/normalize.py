from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import yaml

_CONFIG_CACHE: dict = {}


def _load_yaml(name: str) -> dict:
    if name in _CONFIG_CACHE:
        return _CONFIG_CACHE[name]
    here = Path(__file__).resolve().parents[2]
    path = here / "configs" / f"{name}.yaml"
    if not path.exists():
        _CONFIG_CACHE[name] = {}
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    _CONFIG_CACHE[name] = data
    return data


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def slugify_label(label: str) -> str:
    if not label:
        return "unknown"
    aliases = _load_yaml("label_slugs").get("aliases", {})
    lo = label.lower()
    for needle, slug in aliases.items():
        if needle.lower() in lo:
            return slug
    s = _strip_accents(label).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or "unknown"


def classify_theme(title: str) -> str:
    if not title:
        return "unknown"
    cfg = _load_yaml("theme_keywords").get("themes", {})
    lo = _strip_accents(title).lower()
    for theme, kws in cfg.items():
        for kw in kws:
            if _strip_accents(kw).lower() in lo:
                return theme
    return "unknown"


def normalize_commune_name(s: str) -> str:
    s = _strip_accents(s).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    tokens = [t for t in s.split() if t not in {"pdcom", "plan", "directeur", "communal", "commune", "de", "la", "le", "les", "version"} and not t.isdigit() and len(t) > 1]
    return " ".join(tokens)


def rgb_to_hex(rgb) -> str:
    if rgb is None:
        return "#000000"
    r, g, b = (int(round(c * 255)) if 0 <= c <= 1 else int(c) for c in rgb[:3])
    return f"#{r:02x}{g:02x}{b:02x}"


def hex_to_rgb(h: str) -> tuple:
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
