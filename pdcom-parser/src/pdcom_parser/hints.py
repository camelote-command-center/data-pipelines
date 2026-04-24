"""Hints loader for v0.3: reads map_page_hints_auto.yaml (auto-classified) and
the manual hints file, merges them per PDF, and returns the list of pages
the parser should process.

Hints schema (auto):
    pdfs:
      <filename>:
        source_folder: processed | new
        map:   [{page: int, signals: {...}}, ...]   # high-confidence
        maybe: [{page: int, signals: {...}}, ...]   # manual review / stricter gate
        manual: {map_pages: [...]}                  # preserved from manual hints

Manual hints are given higher priority than auto when both exist.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import yaml


@dataclass
class PagePlan:
    """Per-PDF plan of which pages to process at which confidence gate."""
    map_pages: set[int]        # process at default confidence gate (0.8)
    maybe_pages: set[int]      # process at stricter gate (0.85)

    @property
    def all_pages(self) -> set[int]:
        return self.map_pages | self.maybe_pages


def load_page_plan(pdf_filename: str, auto_hints: dict | None, manual_hints: dict | None) -> PagePlan:
    """Return pages to process for a given PDF name. Manual hints override auto
    classification (if Ilan labeled a page as a map, trust it over the classifier)."""
    map_pages: set[int] = set()
    maybe_pages: set[int] = set()

    # Auto hints
    if auto_hints:
        pdfs = auto_hints.get("pdfs", {}) if isinstance(auto_hints, dict) else {}
        entry = pdfs.get(pdf_filename)
        if entry:
            for p in entry.get("map", []) or []:
                pn = p.get("page") if isinstance(p, dict) else p
                if pn:
                    map_pages.add(int(pn))
            for p in entry.get("maybe", []) or []:
                pn = p.get("page") if isinstance(p, dict) else p
                if pn:
                    maybe_pages.add(int(pn))

    # Manual hints — promote to map_pages (high trust)
    manual_pages = _manual_pages(pdf_filename, manual_hints)
    if manual_pages:
        map_pages.update(manual_pages)
        maybe_pages -= map_pages  # if auto said "maybe" but manual says "yes", trust manual

    return PagePlan(map_pages=map_pages, maybe_pages=maybe_pages)


def _manual_pages(pdf_filename: str, manual_hints: dict | None) -> set[int]:
    """Extract manual map_pages for a PDF, handling both direct hints file
    format and auto-hints file's embedded `manual:` key."""
    if not manual_hints:
        return set()
    # The manual file has a flat {filename: {map_pages: [...]}} format
    entry = manual_hints.get(pdf_filename)
    if entry is None:
        # Case-insensitive fallback
        for k, v in manual_hints.items():
            if isinstance(k, str) and k.lower() == pdf_filename.lower():
                entry = v
                break
    if not entry or not isinstance(entry, dict):
        return set()
    pages = entry.get("map_pages") or entry.get("map_pages_annex_raw") or []
    return {int(p) for p in pages if isinstance(p, (int, str)) and str(p).isdigit()}


def load_hint_files(auto_path: Path | None, manual_path: Path | None) -> tuple[dict | None, dict | None]:
    """Load both YAML files, returning parsed dicts (None if missing)."""
    auto = None
    manual = None
    if auto_path and auto_path.exists():
        with auto_path.open("r", encoding="utf-8") as f:
            auto = yaml.safe_load(f)
    if manual_path and manual_path.exists():
        with manual_path.open("r", encoding="utf-8") as f:
            manual = yaml.safe_load(f)
        # Strip meta and list-type keys (canton_atlas/needs_review/etc.)
        if isinstance(manual, dict):
            for k in list(manual.keys()):
                if k in ("meta", "canton_atlas", "needs_review", "unmatched", "matched", "pdfs"):
                    manual.pop(k, None)
    return auto, manual
