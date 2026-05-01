"""v0.5 PDCom urbanisation track — focused extraction from carte-de-synthèse /
strategie-d-evolution-de-la-zone-5 PDFs into 6 canonical zoning categories.

Separate from v0.4's pipeline.extract_pdf() — this is a slim parallel orchestrator
that calls the same low-level primitives (legend, extract, georef) but writes
directly to silver_ch.pdcom_urbanisation (no bronze, no matview, no gold).

Tier 1/2/3 label-to-category matcher is config-driven via
configs/urbanisation_categories.yaml.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Iterable

import fitz
import yaml
from rapidfuzz import fuzz
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.ops import unary_union

from .extract import clip_and_score_layers, extract_layers
from .georef import georeference
from .ingest import sha256_of
from .legend import detect_legend, detect_legend_label_anchored


# ─── Config loader ─────────────────────────────────────────────────────────────

_CFG_PATH = Path(__file__).resolve().parents[2] / "configs" / "urbanisation_categories.yaml"


@lru_cache(maxsize=1)
def _categories() -> dict:
    if not _CFG_PATH.exists():
        raise FileNotFoundError(f"Urbanisation categories config missing: {_CFG_PATH}")
    with _CFG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ─── Label normalization ───────────────────────────────────────────────────────

_BOILER_RE = re.compile(r"selon\s+fiche\s+a\d+\s+du\s+pdcn", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")


def _strip_accents(s: str) -> str:
    n = unicodedata.normalize("NFD", s)
    return "".join(c for c in n if unicodedata.category(c) != "Mn")


def normalize_for_match(s: str) -> str:
    """Normalize a label for substring/fuzzy comparison: strip accents, lowercase,
    collapse en/em-dash to ASCII hyphen, drop 'selon fiche A0X du PDCn' boilerplate,
    collapse whitespace.

    NOTE: parenthesized clauses are PRESERVED — for the MZ category the disambiguator
    'court' vs 'long' lives inside the parens of the canonical label.
    """
    if not s:
        return ""
    out = _strip_accents(s).lower()
    out = out.replace("–", "-").replace("—", "-")
    out = _BOILER_RE.sub(" ", out)
    out = _WS_RE.sub(" ", out).strip()
    return out


# ─── Tier 1/2/3 matcher ────────────────────────────────────────────────────────

@dataclass
class CategoryMatch:
    key: str
    canonical_label: str
    confidence: float  # 0.95 (Tier 1) or 0.80–0.94 (Tier 2)
    tier: int           # 1 or 2


def match_label_to_category(raw_label: str) -> CategoryMatch | None:
    """Apply Tier 1 (exact normalized equality) → Tier 2 (primary + require + reject + partial_ratio≥80)
    → Tier 3 (drop). Returns None on Tier 3.

    For ambiguous matches (multiple categories Tier-2 match), pick the one with
    highest partial_ratio score; if ties, pick the one with the most non-empty
    `require_one_of` matches (most specific).
    """
    if not raw_label:
        return None
    norm = normalize_for_match(raw_label)
    if not norm:
        return None
    cfg = _categories()

    # Tier 1 — exact normalized equality on canonical_label
    for key, entry in cfg.items():
        if normalize_for_match(entry["canonical_label"]) == norm:
            return CategoryMatch(key=key, canonical_label=entry["canonical_label"], confidence=0.95, tier=1)

    # Tier 2 — primary + require + reject + partial_ratio
    if len(norm) < 6:
        return None
    candidates: list[tuple[float, str, str, int]] = []  # (score, key, canon, require_hits)
    for key, entry in cfg.items():
        primary = [normalize_for_match(p) for p in entry.get("primary_terms", [])]
        require = [normalize_for_match(p) for p in entry.get("require_one_of", []) or []]
        reject = [normalize_for_match(p) for p in entry.get("reject_if_contains", []) or []]
        # Primary: at least one must appear as substring
        if not any(p and p in norm for p in primary):
            continue
        # Reject: none must appear
        if any(r and r in norm for r in reject):
            continue
        # Require: if list non-empty, at least one must appear
        require_hits = sum(1 for r in require if r and r in norm)
        if require and require_hits == 0:
            continue
        # Partial ratio against canonical_label
        ratio = fuzz.partial_ratio(norm, normalize_for_match(entry["canonical_label"]))
        if ratio < 80:
            continue
        candidates.append((ratio, key, entry["canonical_label"], require_hits))

    if not candidates:
        return None
    # Pick best: highest ratio, then most require_hits as tiebreak
    candidates.sort(key=lambda c: (-c[0], -c[3]))
    ratio, key, canon, _ = candidates[0]
    # Map ratio 80→0.80, 100→0.94 (reserve 0.95 for Tier 1)
    confidence = round(0.80 + (ratio - 80) / 20 * 0.14, 3)
    return CategoryMatch(key=key, canonical_label=canon, confidence=confidence, tier=2)


# ─── Feature dataclass + extraction ────────────────────────────────────────────

@dataclass
class UrbanisationFeature:
    commune_bfs: int
    commune_name: str
    category_key: str
    category_label: str
    raw_label: str
    source_color: str          # hex
    source_pdf: str            # filename
    source_page: int
    geometry: object           # shapely Polygon or MultiPolygon (LV95)
    confidence: float          # geom-confidence, separate from label match
    label_match_confidence: float
    label_match_tier: int
    pdf_sha256: str = ""       # filled by caller


@dataclass
class ExtractionReport:
    pdf_path: Path
    commune_bfs: int
    commune_name: str
    pages_processed: int = 0
    pages_with_legend: int = 0
    legend_entries_total: int = 0
    matched_entries: int = 0
    unmatched_labels: list[str] = field(default_factory=list)
    features: list[UrbanisationFeature] = field(default_factory=list)
    page_confidences: list[float] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _ensure_multipolygon(geom) -> MultiPolygon | None:
    if geom is None or getattr(geom, "is_empty", False):
        return None
    if geom.geom_type == "MultiPolygon":
        return geom
    if geom.geom_type == "Polygon":
        return MultiPolygon([geom])
    if hasattr(geom, "geoms"):
        polys = [g for g in geom.geoms if g.geom_type == "Polygon" and not g.is_empty]
        if not polys:
            return None
        return MultiPolygon(polys)
    return None


def extract_urbanisation_pdf(
    pdf_path: Path,
    commune_bfs: int,
    commune_name: str,
    boundary_lv95_geojson: dict,
    legend_mode: str = "anchored",      # "anchored" (default) | "cascade"
) -> ExtractionReport:
    """Run urbanisation extraction on a single PDF. Returns a report — caller
    decides whether to persist features (see write_urbanisation_features)."""
    boundary_lv95 = shape(boundary_lv95_geojson)
    rpt = ExtractionReport(pdf_path=pdf_path, commune_bfs=commune_bfs, commune_name=commune_name)

    with fitz.open(pdf_path) as pdf:
        for pi in range(pdf.page_count):
            page = pdf[pi]
            page_number = pi + 1
            rpt.pages_processed += 1

            # 1) Legend detection — anchored first; cascade falls back to detect_legend
            legend = None
            try:
                if legend_mode == "cascade":
                    legend = detect_legend(page)
                    if not legend.get("entries") or len(legend["entries"]) < 2:
                        legend = detect_legend_label_anchored(page)
                else:
                    legend = detect_legend_label_anchored(page)
            except Exception as e:
                rpt.notes.append(f"page {page_number}: legend detector raised {e}")
                continue
            entries = legend.get("entries", []) or []
            if not entries:
                continue
            rpt.pages_with_legend += 1
            rpt.legend_entries_total += len(entries)

            # 2) Map each legend entry through the Tier 1/2/3 matcher.
            #    Build a filtered legend dict with only matched entries — extract_layers
            #    will only hydrate polygons matching those colors.
            matched_entries = []
            entry_meta: list[CategoryMatch] = []
            for e in entries:
                lbl = e.get("label") or ""
                match = match_label_to_category(lbl)
                if match is None:
                    rpt.unmatched_labels.append(lbl)
                    continue
                matched_entries.append(e)
                entry_meta.append(match)
            if not matched_entries:
                continue
            rpt.matched_entries += len(matched_entries)

            filtered_legend = {**legend, "entries": matched_entries}

            # 3) Hydrate polygons per matched legend color.
            try:
                layers_pdf = extract_layers(page, filtered_legend)
            except Exception as e:
                rpt.notes.append(f"page {page_number}: extract_layers raised {e}")
                continue

            # 4) Georeference to LV95.
            try:
                layers_lv95, page_conf = georeference(
                    layers_pdf, boundary_lv95, page,
                    map_bbox=tuple(legend["map_bbox"]) if legend.get("map_bbox") else None,
                )
            except Exception as e:
                rpt.notes.append(f"page {page_number}: georeference raised {e}")
                continue
            rpt.page_confidences.append(round(page_conf, 3))

            # 5) Commune-clip + area filters; computes per-feature confidence.
            features_clipped, _drops = clip_and_score_layers(layers_lv95, boundary_lv95, page_conf)

            # 6) Map back from layer slug → category match (entry_meta)
            #    extract_layers keys layers by entry["slug"]; build a lookup.
            slug_to_match: dict[str, CategoryMatch] = {}
            slug_to_raw_label: dict[str, str] = {}
            for e, m in zip(matched_entries, entry_meta):
                slug = e.get("slug") or ""
                if slug:
                    slug_to_match[slug] = m
                    slug_to_raw_label[slug] = e.get("label") or ""

            for f in features_clipped:
                slug = f.get("slug") or ""
                m = slug_to_match.get(slug)
                if m is None:
                    continue
                geom = _ensure_multipolygon(f.get("geom"))
                if geom is None:
                    continue
                color_hex = f.get("color") or "#000000"
                rpt.features.append(UrbanisationFeature(
                    commune_bfs=commune_bfs,
                    commune_name=commune_name,
                    category_key=m.key,
                    category_label=m.canonical_label,
                    raw_label=slug_to_raw_label.get(slug, "") or f.get("label") or "",
                    source_color=str(color_hex),
                    source_pdf=pdf_path.name,
                    source_page=page_number,
                    geometry=geom,
                    confidence=round(float(f.get("confidence") or 0.0), 3),
                    label_match_confidence=m.confidence,
                    label_match_tier=m.tier,
                ))

    return rpt


# ─── DB write ──────────────────────────────────────────────────────────────────

def write_urbanisation_features(
    conn,
    features: list[UrbanisationFeature],
    pdf_sha256: str,
) -> int:
    """Idempotent per-PDF write to silver_ch.pdcom_urbanisation. Single transaction:
    DELETE WHERE pdf_sha256=%s, then INSERT all rows. Looks up source_url via
    bronze_ch.pdcom_sources by sha256 (NULL if not present)."""
    import json

    with conn.cursor() as cur:
        # Look up source_url from bronze if the same PDF was previously ingested
        cur.execute(
            "SELECT source_url FROM bronze_ch.pdcom_sources WHERE pdf_sha256 = %s LIMIT 1",
            (pdf_sha256,),
        )
        row = cur.fetchone()
        source_url = row[0] if row else None

        # Atomic per-PDF replace
        cur.execute(
            "DELETE FROM silver_ch.pdcom_urbanisation WHERE pdf_sha256 = %s",
            (pdf_sha256,),
        )

        if not features:
            conn.commit()
            return 0

        rows = [
            (
                f.commune_bfs, f.commune_name, f.category_key, f.category_label,
                f.raw_label, f.source_color, f.source_pdf, f.source_page, source_url,
                pdf_sha256,
                json.dumps(mapping(f.geometry)),
                f.confidence,
            )
            for f in features
        ]
        stmt = """
        INSERT INTO silver_ch.pdcom_urbanisation (
            commune_bfs, commune_name, category_key, category_label,
            raw_label, source_color, source_pdf, source_page, source_url,
            pdf_sha256, geometry, confidence
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 2056)),
                %s)
        """
        # Chunk to avoid pooler timeout on large batches
        CHUNK = 500
        for start in range(0, len(rows), CHUNK):
            cur.executemany(stmt, rows[start:start + CHUNK])
        conn.commit()
        return len(rows)


def post_batch_dedup(conn) -> int:
    """After all PDFs are written, dedupe across (commune_bfs, category_key,
    geohash9). Same physical zone might appear in multiple candidate PDFs for
    one commune (e.g. carte-de-synthese + strategie). Keep highest-confidence row.
    Returns count of rows deleted."""
    with conn.cursor() as cur:
        cur.execute("""
            WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY commune_bfs, category_key,
                                        ST_GeoHash(ST_Transform(geometry, 4326), 9)
                           ORDER BY confidence DESC, area_m2 DESC, extracted_at DESC, id
                       ) AS rk
                FROM silver_ch.pdcom_urbanisation
            )
            DELETE FROM silver_ch.pdcom_urbanisation
            WHERE id IN (SELECT id FROM ranked WHERE rk > 1)
        """)
        n = cur.rowcount
        conn.commit()
        return n


# ─── Discovery ─────────────────────────────────────────────────────────────────

_FILENAME_PATTERNS = [
    re.compile(r"strategie.*evolution.*zone.*5", re.IGNORECASE),
    re.compile(r"carte.*synth[eè]se", re.IGNORECASE),
    re.compile(r"synth[eè]se", re.IGNORECASE),
    re.compile(r"urbanisation", re.IGNORECASE),
    re.compile(r"zones.*urbanisation", re.IGNORECASE),
    re.compile(r"plan.*directeur.*synth[eè]se", re.IGNORECASE),
    re.compile(r"affectation", re.IGNORECASE),
    re.compile(r"image.*directrice", re.IGNORECASE),
    re.compile(r"evolution.*urbanisation", re.IGNORECASE),
]


def discover_candidate_pdfs(*roots: Path, page_count_threshold: int = 8) -> list[dict]:
    """Find candidate PDFs: filename pattern OR ≤ page_count_threshold pages.
    Returns list of dicts: {path, filename, page_count, size_bytes, match_reason, commune_slug}."""
    out: list[dict] = []
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        for p in sorted(root.rglob("*.pdf")):
            if p in seen:
                continue
            seen.add(p)
            name = p.name
            filename_match = any(rx.search(name) for rx in _FILENAME_PATTERNS)
            try:
                with fitz.open(p) as pdf:
                    page_count = pdf.page_count
            except Exception:
                continue
            page_match = page_count <= page_count_threshold
            if not (filename_match or page_match):
                continue
            reasons = []
            if filename_match:
                reasons.append("filename")
            if page_match:
                reasons.append("page_count")
            # Crude commune slug: token between "pdcom_" prefix and the next descriptor
            slug = name.lower().replace("pdcom_", "", 1)
            slug = re.split(r"[_\-\.]", slug)[0] if slug else ""
            out.append({
                "path": p,
                "filename": name,
                "page_count": page_count,
                "size_bytes": p.stat().st_size,
                "match_reason": "/".join(reasons),
                "commune_slug": slug,
            })
    out.sort(key=lambda r: (r["commune_slug"], r["filename"]))
    return out
