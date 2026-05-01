"""v0.5.2 PDCom urbanisation v2 — promoteur-focused full-GE extraction.

6 categories: zone_5, densification_accrue, plq_a_etablir, plq_realise,
a_proteger, exploitable. Keyword-any + excludes matcher with priority
resolution (most-specific wins; others go to alternate_categories[]).

Reuses v0.4 primitives: legend.detect_legend_label_anchored,
extract.extract_layers, georef.georeference, extract.clip_and_score_layers.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import fitz
import yaml
from shapely.geometry import MultiPolygon, Polygon, mapping, shape

from .extract import clip_and_score_layers, extract_layers
from .georef import georeference
from .ingest import sha256_of
from .legend import detect_legend, detect_legend_label_anchored


# ─── Config ────────────────────────────────────────────────────────────────────

_CFG_PATH = Path(__file__).resolve().parents[2] / "configs" / "urbanisation_categories.yaml"


@lru_cache(maxsize=1)
def _config() -> dict:
    if not _CFG_PATH.exists():
        raise FileNotFoundError(f"Urbanisation categories config missing: {_CFG_PATH}")
    with _CFG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache(maxsize=1)
def _categories() -> dict[str, dict]:
    return _config().get("categories", {})


@lru_cache(maxsize=1)
def _priority() -> list[str]:
    cfg = _config()
    pri = cfg.get("priority_order", []) or list(cfg.get("categories", {}).keys())
    return list(pri)


# ─── Normalization ─────────────────────────────────────────────────────────────

_BOILER_RE = re.compile(r"selon\s+fiche\s+a\d+\s+du\s+pdcn", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")


def _strip_accents(s: str) -> str:
    n = unicodedata.normalize("NFD", s)
    return "".join(c for c in n if unicodedata.category(c) != "Mn")


# v2.2: meta-label rejection — drop document titles, fiche references, filename
# leakage, and all-caps headers BEFORE keyword matching. Polygons attached to
# these labels are noise (page-content frames, not legend entries). Patterns
# applied to the RAW (un-normalized) label so case/accent signals are preserved.
_META_LABEL_PATTERNS = [
    # Filename leakage: starts with digit run + _/- + alphanumeric jumble
    re.compile(r"^\d+[_-][A-Za-z0-9_-]+$"),
    # "Fiche XX:" / "Fiche XXa." prefixes — section references, not zone labels
    re.compile(r"^[Ff][Ii][Cc][Hh][Ee]\s+\d+[a-z]?\s*[:.]"),
    # Strategy/section titles starting with "Stratégie d…" / "STRATÉGIE D…"
    re.compile(r"^Stratégie\s+[Dd]"),
    re.compile(r"^STRATÉGIE\s+D"),
    # All-caps document headers: 15+ chars, mostly uppercase + digits/punct only
    re.compile(r"^[A-ZÀ-Ÿ\s\d\.\-_:'’«»]{15,}$"),
]


def is_meta_label(raw_label: str) -> bool:
    """True if the label looks like a document title, fiche reference, or
    filename leakage rather than a real legend entry."""
    if not raw_label:
        return True
    s = raw_label.strip()
    return any(p.search(s) for p in _META_LABEL_PATTERNS)


def normalize_for_match(s: str) -> str:
    """Lowercased, accent-stripped, dashes normalized, PDCn boilerplate dropped,
    whitespace collapsed. Parens are PRESERVED so disambiguators inside them
    (e.g. 'court' vs 'long') survive."""
    if not s:
        return ""
    out = _strip_accents(s).lower()
    out = out.replace("–", "-").replace("—", "-")
    out = _BOILER_RE.sub(" ", out)
    out = _WS_RE.sub(" ", out).strip()
    return out


# ─── Matcher ───────────────────────────────────────────────────────────────────

@dataclass
class CategoryMatch:
    category_key: str
    canonical_label: str
    matched_keywords: list[str]
    alternate_categories: list[str]


def match_label_to_category(raw_label: str) -> CategoryMatch | None:
    """Match raw_label against the 6 categories. Returns the most-specific
    match per priority_order. Other matches go to alternate_categories.
    Returns None if nothing matches OR if the label is a meta-label (document
    title / fiche reference / filename leakage / all-caps header).
    """
    if not raw_label:
        return None
    # v2.2: meta-label rejection on raw label (preserves case/accent signals)
    if is_meta_label(raw_label):
        return None
    norm = normalize_for_match(raw_label)
    if not norm:
        return None

    cats = _categories()
    matched: list[tuple[str, list[str]]] = []  # (key, hit_keywords)

    def _word_in(needle: str, hay: str) -> bool:
        """Substring match with word boundaries on both sides — prevents
        'constructible' from matching inside 'inconstructible', or 'plq' from
        matching inside 'plquelque'. Allows multi-word keywords."""
        if not needle:
            return False
        return re.search(r"(?<![a-z0-9])" + re.escape(needle) + r"(?![a-z0-9])", hay) is not None

    for key, entry in cats.items():
        any_kw = [normalize_for_match(k) for k in entry.get("keywords_any", []) or []]
        excl_kw = [normalize_for_match(k) for k in entry.get("keywords_excludes", []) or []]
        if any(_word_in(e, norm) for e in excl_kw):
            continue
        hits = [k for k in any_kw if _word_in(k, norm)]
        if hits:
            matched.append((key, hits))

    if not matched:
        return None

    # Resolve via priority order
    pri = _priority()
    matched_keys_set = {k for k, _ in matched}
    resolved_key = next((k for k in pri if k in matched_keys_set), matched[0][0])

    # Aggregate keywords for the resolved match
    resolved_kws = next((kws for k, kws in matched if k == resolved_key), [])
    alternates = [k for k in pri if k in matched_keys_set and k != resolved_key]

    return CategoryMatch(
        category_key=resolved_key,
        canonical_label=cats[resolved_key]["canonical_label"],
        matched_keywords=sorted(set(resolved_kws)),
        alternate_categories=alternates,
    )


# ─── Feature dataclass ─────────────────────────────────────────────────────────

@dataclass
class UrbanisationFeature:
    commune_bfs: int
    commune_name: str
    category_key: str
    category_label: str
    raw_label: str
    source_color: str
    source_pdf: str
    source_page: int
    geometry: object
    confidence: float
    alternate_categories: list[str] = field(default_factory=list)
    match_keywords: list[str] = field(default_factory=list)
    map_freshness_year: int | None = None
    pdf_sha256: str = ""


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
    map_freshness_year: int | None = None


# ─── Helpers ───────────────────────────────────────────────────────────────────

_YEAR_RE = re.compile(r"(?<!\d)(20[12]\d|19[89]\d)(?!\d)")


def extract_year_from_filename(filename: str) -> int | None:
    """Pull a year signal from the filename (2018, 2020, etc.). None if absent."""
    matches = _YEAR_RE.findall(filename)
    if not matches:
        return None
    return max(int(y) for y in matches)


def _normalize_geom(geom):
    """Pass through Polygons (as MultiPolygon) and LineStrings (as MultiLineString).
    Drop empty/null. The schema accepts geometry(Geometry, 2056) so both kinds
    are stored — stroke_only zone outlines are still valuable for visualization."""
    from shapely.geometry import LineString, MultiLineString
    if geom is None or getattr(geom, "is_empty", False):
        return None
    if geom.geom_type == "MultiPolygon":
        return geom
    if geom.geom_type == "Polygon":
        return MultiPolygon([geom])
    if geom.geom_type == "MultiLineString":
        return geom
    if geom.geom_type == "LineString":
        return MultiLineString([geom])
    if hasattr(geom, "geoms"):
        polys = [g for g in geom.geoms if g.geom_type == "Polygon" and not g.is_empty]
        if polys:
            return MultiPolygon(polys)
        lines = [g for g in geom.geoms if g.geom_type == "LineString" and not g.is_empty]
        if lines:
            return MultiLineString(lines)
    return None


# ─── Per-PDF extraction ────────────────────────────────────────────────────────

def _find_extended_label(anchor_text: str, all_lines: list[dict]) -> str | None:
    """If anchor is short (e.g. 'Secteur B'), find a longer line elsewhere on
    the page that starts with the anchor followed by ': ' or ' - ' or ' :'.
    Used for legends where the swatch nameplate is short but the keyword-bearing
    description is in a separate paragraph.
    """
    a = anchor_text.strip()
    if len(a) > 25:
        return None
    norm_a = a.rstrip(":").strip()
    for ln in all_lines:
        t = ln["text"].strip()
        if t == a:
            continue
        if (t.startswith(norm_a + " :") or t.startswith(norm_a + ":")
                or t.startswith(norm_a + " - ") or t.startswith(norm_a + " — ")
                or t.startswith(norm_a + " – ")):
            if len(t) > len(norm_a) + 2:
                return t
    return None


def _build_keyword_first_legend(page) -> dict:
    """v2 swatch-first legend detection.

    For each filled swatch on the page:
      1. Find its anchor label = nearest text line within ~140pt (any direction).
      2. Try keyword-matching that anchor. If matches → record entry.
      3. Else look for an *extended* form of the anchor (e.g. anchor 'Secteur B'
         and somewhere on the page 'Secteur B : Z5 densification accrue'). If
         the extended form matches → record entry, use extended as raw_label.
      4. Else drop.

    This handles two layouts:
      - Direct: swatch + keyword-bearing label adjacent (most synthese maps).
      - Indirect: swatch + short nameplate + extended description elsewhere
        (Anières-style).

    Basemap clutter, mobility lines etc. don't match canonical keywords →
    naturally filtered out.
    """
    from .legend import _is_swatch_candidate, _rect_of, _is_near_white
    from .normalize import slugify_label, rgb_to_hex
    page_rect = page.rect
    drawings = page.get_drawings()
    swatches = [d for d in drawings if _is_swatch_candidate(d)]

    raw = page.get_text("dict")
    all_lines = []
    for block in raw.get("blocks", []) or []:
        if "lines" not in block:
            continue
        for line in block["lines"]:
            bbox = line.get("bbox")
            text = " ".join(s.get("text", "") for s in line.get("spans", [])).strip()
            if not text or not bbox:
                continue
            all_lines.append({"bbox": _rect_t(bbox), "text": text})

    by_color: dict[str, dict] = {}   # hex → best entry (prefer solid over stroke_only)

    for d in swatches:
        r = _rect_of(d)
        if r is None:
            continue
        # Find nearest text line — within ~140pt (more permissive than label_anchored).
        best_ln = None
        best_score = 1e9
        for ln in all_lines:
            tb = ln["bbox"]
            if tb.x0 >= r.x1 - 2:
                dx = tb.x0 - r.x1
                side = "right"
            elif tb.x1 <= r.x0 + 2:
                dx = r.x0 - tb.x1
                side = "left"
            else:
                # Overlap horizontally: only accept if vertically close
                dx = 0
                side = "vert"
            if dx > 140:
                continue
            sw_cy = (r.y0 + r.y1) / 2
            ln_cy = (tb.y0 + tb.y1) / 2
            dy = abs(sw_cy - ln_cy)
            tol = max(20, (r.height + tb.height) / 2 + 8)
            if dy > tol:
                continue
            # Prefer right-side labels (legend convention), then closer
            penalty = 0 if side == "right" else 5
            score = dx + dy * 1.5 + penalty
            if score < best_score:
                best_score = score
                best_ln = ln
        if best_ln is None:
            continue

        # Try direct keyword match first
        anchor_text = best_ln["text"]
        match = match_label_to_category(anchor_text)
        used_label = anchor_text
        if match is None:
            # Look for an extended form of this short label elsewhere on the page
            extended = _find_extended_label(anchor_text, all_lines)
            if extended:
                match = match_label_to_category(extended)
                if match is not None:
                    used_label = extended
        if match is None:
            continue

        # Hydrate color
        has_fill = d.get("fill") is not None and d.get("fill_opacity", 1) > 0.1
        has_stroke = d.get("color") is not None
        if has_fill:
            color = d.get("fill")
            fill_type = "solid"
        elif has_stroke:
            color = d.get("color")
            fill_type = "stroke_only"
        else:
            continue
        if color is None:
            continue
        if fill_type == "solid" and _is_near_white(color):
            continue
        hex_color = rgb_to_hex(color)
        new_entry = {
            "label": used_label, "slug": slugify_label(used_label),
            "fill_color": list(color[:3]) if color else None,
            "fill_color_hex": hex_color, "fill_type": fill_type,
            "swatch_bbox": [r.x0, r.y0, r.x1, r.y1],
        }
        existing = by_color.get(hex_color)
        if existing is None:
            by_color[hex_color] = new_entry
        else:
            # Prefer solid over stroke_only (downstream extract_layers prefers polygons)
            if existing["fill_type"] == "stroke_only" and fill_type == "solid":
                by_color[hex_color] = new_entry

    entries = list(by_color.values())
    if not entries:
        return {"entries": [], "title": "", "map_bbox": None, "legend_bbox": None, "swatch_unlabeled": 0}

    xs = [e["swatch_bbox"][0] for e in entries] + [e["swatch_bbox"][2] for e in entries]
    ys = [e["swatch_bbox"][1] for e in entries] + [e["swatch_bbox"][3] for e in entries]
    legend_bbox = [min(xs) - 4, min(ys) - 4, max(xs) + 8, max(ys) + 8]
    map_bbox = [0, 0, page_rect.x1, page_rect.y1]
    return {"entries": entries, "title": "", "map_bbox": map_bbox, "legend_bbox": legend_bbox, "swatch_unlabeled": 0}


def _rect_t(bbox):
    """Tiny helper: convert PyMuPDF bbox (4-tuple) to fitz.Rect."""
    import fitz as _fitz
    return _fitz.Rect(bbox)


def extract_urbanisation_pdf(
    pdf_path: Path,
    commune_bfs: int,
    commune_name: str,
    boundary_lv95_geojson: dict,
    legend_mode: str = "anchored",
    pages: list[int] | None = None,    # 1-indexed; if None, process all
) -> ExtractionReport:
    """Run urbanisation v2 extraction on a single PDF. Returns a report — caller
    decides whether to persist features.

    Args:
        pages: Optional 1-indexed page list to restrict extraction to.
               Used for the Genève atlas (page 9 only).
    """
    boundary_lv95 = shape(boundary_lv95_geojson)
    rpt = ExtractionReport(
        pdf_path=pdf_path, commune_bfs=commune_bfs, commune_name=commune_name,
        map_freshness_year=extract_year_from_filename(pdf_path.name),
    )

    with fitz.open(pdf_path) as pdf:
        page_indices = [p - 1 for p in pages] if pages else list(range(pdf.page_count))
        for pi in page_indices:
            if pi < 0 or pi >= pdf.page_count:
                continue
            page = pdf[pi]
            page_number = pi + 1
            rpt.pages_processed += 1

            # v2: keyword-first legend detection (much higher recall on these focused PDFs).
            # If it produces nothing, fall back to v0.3.1 detector(s).
            try:
                legend = _build_keyword_first_legend(page)
                if not legend.get("entries"):
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

            # Match each legend entry. The keyword-first detector pre-filters via match,
            # so its entries are already guaranteed matchable — but we still need the
            # CategoryMatch object to know category_key/label/keywords/alternates.
            matched_entries = []
            entry_meta: list[CategoryMatch] = []
            for e in entries:
                lbl = e.get("label") or ""
                m = match_label_to_category(lbl)
                if m is None:
                    rpt.unmatched_labels.append(lbl)
                    continue
                matched_entries.append(e)
                entry_meta.append(m)
            if not matched_entries:
                continue
            rpt.matched_entries += len(matched_entries)

            filtered_legend = {**legend, "entries": matched_entries}
            try:
                layers_pdf = extract_layers(page, filtered_legend)
            except Exception as e:
                rpt.notes.append(f"page {page_number}: extract_layers raised {e}")
                continue

            try:
                layers_lv95, page_conf = georeference(
                    layers_pdf, boundary_lv95, page,
                    map_bbox=tuple(legend["map_bbox"]) if legend.get("map_bbox") else None,
                )
            except Exception as e:
                rpt.notes.append(f"page {page_number}: georeference raised {e}")
                continue
            rpt.page_confidences.append(round(page_conf, 3))

            features_clipped, _drops = clip_and_score_layers(layers_lv95, boundary_lv95, page_conf)

            slug_to_match: dict[str, CategoryMatch] = {}
            slug_to_raw: dict[str, str] = {}
            for e, m in zip(matched_entries, entry_meta):
                slug = e.get("slug") or ""
                if slug:
                    slug_to_match[slug] = m
                    slug_to_raw[slug] = e.get("label") or ""

            for f in features_clipped:
                slug = f.get("slug") or ""
                m = slug_to_match.get(slug)
                if m is None:
                    continue
                geom = _normalize_geom(f.get("geom"))
                if geom is None:
                    continue
                color_hex = f.get("color") or "#000000"
                rpt.features.append(UrbanisationFeature(
                    commune_bfs=commune_bfs, commune_name=commune_name,
                    category_key=m.category_key,
                    category_label=m.canonical_label,
                    raw_label=slug_to_raw.get(slug, "") or f.get("label") or "",
                    source_color=str(color_hex),
                    source_pdf=pdf_path.name, source_page=page_number,
                    geometry=geom,
                    confidence=round(float(f.get("confidence") or 0.0), 3),
                    alternate_categories=m.alternate_categories,
                    match_keywords=m.matched_keywords,
                    map_freshness_year=rpt.map_freshness_year,
                ))
    return rpt


# ─── DB write ──────────────────────────────────────────────────────────────────

def write_urbanisation_features(
    conn,
    features: list[UrbanisationFeature],
    pdf_sha256: str,
) -> int:
    """Per-commune-scoped write to silver_ch.pdcom_urbanisation.

    v2.2: clears ALL prior rows for this commune (not just same-PDF rows). The
    multi-PDF picker can choose a different best-PDF across runs (e.g. Perly
    flipping from carte-de-synthese to rapport), and per-PDF DELETE leaves
    orphans from the abandoned PDF. Per-commune DELETE eliminates the orphan
    class. Trade-off: less idempotent per-PDF (re-running the same PDF is
    still safe; running multiple PDFs sequentially for the same commune
    overwrites — but the corpus run already calls this once per commune end-
    to-end, so this matches the actual usage pattern).

    Looks up source_url via bronze_ch.pdcom_sources by sha256.
    """
    import json

    if not features:
        # Even with no features, we should clear the commune's prior rows so
        # a previously-extractable PDF that now produces nothing is reflected.
        # But we need a commune_bfs to scope. If features is empty we skip —
        # the caller (run command) handles coverage_blocked separately and
        # doesn't call this function in that path.
        conn.commit()
        return 0

    commune_bfs = features[0].commune_bfs

    with conn.cursor() as cur:
        cur.execute(
            "SELECT source_url FROM bronze_ch.pdcom_sources WHERE pdf_sha256 = %s LIMIT 1",
            (pdf_sha256,),
        )
        row = cur.fetchone()
        source_url = row[0] if row else None

        # v2.2: per-commune DELETE (handles multi-PDF-picker swap case)
        cur.execute(
            "DELETE FROM silver_ch.pdcom_urbanisation WHERE commune_bfs = %s",
            (commune_bfs,),
        )

        rows = [
            (
                f.commune_bfs, f.commune_name, f.category_key, f.category_label,
                f.raw_label, f.source_color, f.source_pdf, f.source_page, source_url,
                pdf_sha256,
                json.dumps(mapping(f.geometry)),
                f.confidence,
                f.alternate_categories or [],
                f.match_keywords or [],
                f.map_freshness_year,
            )
            for f in features
        ]
        stmt = """
        INSERT INTO silver_ch.pdcom_urbanisation (
            commune_bfs, commune_name, category_key, category_label,
            raw_label, source_color, source_pdf, source_page, source_url,
            pdf_sha256, geometry, confidence,
            alternate_categories, match_keywords, map_freshness_year
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 2056)),
                %s,%s,%s,%s)
        """
        CHUNK = 500
        for start in range(0, len(rows), CHUNK):
            cur.executemany(stmt, rows[start:start + CHUNK])
        conn.commit()
        return len(rows)


def post_batch_dedup(conn) -> int:
    """Same-physical-zone dedup across PDFs/pages within a (commune_bfs, category_key).
    Keep highest-confidence row per (commune, category, geohash9)."""
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

# Synthèse-style filename signals
_SYNTHESE_RE = re.compile(
    r"(synth[eè]se|carte|urbanisation|densification|evolution|affectation|image[_ ]directrice|concept[_ ]carte|plan-localise-de-quartier|villas|directrice|z5\b|zone[_ ]?5\b)",
    re.IGNORECASE,
)
# Rapport (long-document) signal — fallback only for communes with no synthèse
_RAPPORT_RE = re.compile(r"(rapport|complet)", re.IGNORECASE)
# Out-of-scope: paths inside this folder are knowledge re-extraction (separate task)
_NEW_MISSING = "New Missing PDCom"


def _commune_slug_from_filename(filename: str) -> str:
    """Crude commune slug: token between 'pdcom_' and the next descriptor.
    'pdcom_aire_la_ville_complet.pdf' → 'aire_la_ville' (best-effort)."""
    name = filename.lower()
    if name.startswith("pdcom_"):
        name = name[len("pdcom_"):]
    name = name.rsplit(".", 1)[0]
    # Take everything up to a known descriptor token
    descriptors = (
        "_2e_", "_strategie", "_carte", "_synth", "_synthese", "_plan_de", "_plans",
        "_rapport", "_complet", "_pdcp", "_z5", "_sz5", "_concept", "_atlas",
        "_pietons", "_energie", "_consultation", "_mesures", "_annexes",
        "_zone-villas", "_zone_villas", "_strategie_d_evolution", "_pdcp-village",
        "_carte-densification", "_plan-synthese", "_plan_synthese",
        "_plan-de-site", "_hameau", "_plan-localise", "_evolution",
        "_z5-planification", "_image-directrice", "_image_directrice",
        "_directeur", "_de-synthese", "_de_synthese", "_-densification",
    )
    cuts = sorted([name.find(d) for d in descriptors if name.find(d) > 0])
    if cuts:
        name = name[:cuts[0]]
    # Normalize hyphens/underscores to single token
    name = re.sub(r"[_\-]+", "_", name).strip("_")
    return name


def _score_pdf_candidate(path: Path, page_count: int) -> int:
    """Higher score = better synthèse candidate. See spec §Discovery."""
    name = path.name.lower()
    score = 0
    if "synthese" in name or "synthèse" in name or "synth" in name:
        score += 3
    if "carte" in name or "urbanisation" in name or "densification" in name:
        score += 2
    if "Plans de Synthese" in str(path) or "plans_de_synthese" in str(path).lower():
        score += 1
    # Year signal
    year = extract_year_from_filename(name)
    if year:
        # +1 per recent year band: 2024+, 2020-2023, 2017-2019, etc.
        if year >= 2024:
            score += 3
        elif year >= 2020:
            score += 2
        elif year >= 2017:
            score += 1
    if "atlas" in name:
        score -= 2
    if "concept" in name:
        score -= 2
    if "pietons" in name or "energie" in name:
        score -= 3   # cleanly off-topic for urbanisation
    if "annexes" in name or "mesures" in name:
        score -= 2
    # Page-count bonus: focused = 1-2 pages; rapport = 50+
    if page_count <= 2:
        score += 2
    elif page_count <= 8:
        score += 1
    elif page_count > 50:
        score -= 2
    return score


def discover_candidate_pdfs(*roots: Path) -> list[dict]:
    """Walk roots recursively. Excludes paths under 'New Missing PDCom' (knowledge
    re-extraction is a separate task). Returns one dict per PDF with score, year,
    page count, classification (synthese / rapport / off-topic)."""
    out: list[dict] = []
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        for p in sorted(root.rglob("*.pdf")):
            if p in seen:
                continue
            # Skip the knowledge re-extraction folder
            if _NEW_MISSING in p.parts:
                continue
            seen.add(p)
            try:
                with fitz.open(p) as pdf:
                    page_count = pdf.page_count
            except Exception:
                continue
            name = p.name
            is_synth = bool(_SYNTHESE_RE.search(name))
            is_rapport = bool(_RAPPORT_RE.search(name)) and page_count > 10
            score = _score_pdf_candidate(p, page_count)
            commune_slug = _commune_slug_from_filename(name)
            year = extract_year_from_filename(name)
            out.append({
                "path": p,
                "filename": name,
                "page_count": page_count,
                "size_bytes": p.stat().st_size,
                "is_synthese": is_synth,
                "is_rapport": is_rapport,
                "score": score,
                "commune_slug": commune_slug,
                "year": year,
            })
    out.sort(key=lambda r: (r["commune_slug"], -r["score"], r["filename"]))
    return out


def best_candidate_per_commune(rows: list[dict]) -> dict[str, dict]:
    """Group candidates by commune slug (filename heuristic), pick the best by score.
    NOTE: filename slugs are noisy (e.g. 'anieres_01' vs 'anieres'). Prefer
    `group_by_commune_bfs()` which uses ingest.match_pdf_to_commune for canonical
    commune resolution."""
    by_slug: dict[str, list[dict]] = {}
    for r in rows:
        by_slug.setdefault(r["commune_slug"], []).append(r)
    out: dict[str, dict] = {}
    for slug, lst in by_slug.items():
        lst.sort(key=lambda r: (-r["score"], -(r["year"] or 0), r["page_count"]))
        out[slug] = {"best": lst[0], "all": lst}
    return out


def group_by_commune_bfs(rows: list[dict], communes: list[dict]) -> dict[int, dict]:
    """Resolve each candidate to a federal BFS via ingest.match_pdf_to_commune
    (fuzzy match against canton communes), group by BFS. Each group sorted by
    score desc, then year desc, then page_count asc."""
    from .ingest import match_pdf_to_commune
    out: dict[int, dict] = {}
    for r in rows:
        m = match_pdf_to_commune(r["path"], communes)
        if m.commune_bfs is None:
            continue
        bfs = m.commune_bfs
        out.setdefault(bfs, {"all": [], "commune_name": None})
        if out[bfs]["commune_name"] is None:
            out[bfs]["commune_name"] = m.commune_name
        out[bfs]["all"].append({**r, "match_status": m.status, "match_score": m.score})
    for bfs, info in out.items():
        info["all"].sort(key=lambda r: (-r["score"], -(r["year"] or 0), r["page_count"]))
        info["best"] = info["all"][0]
    return out
