from __future__ import annotations

import re
from typing import Any

import fitz

from .normalize import slugify_label, rgb_to_hex


def _rect_of(drawing) -> fitz.Rect | None:
    r = drawing.get("rect")
    if r is None:
        return None
    return fitz.Rect(r)


def _is_swatch_candidate(drawing, max_side: float = 25.0, min_side: float = 3.0) -> bool:
    r = _rect_of(drawing)
    if r is None:
        return False
    if r.width < min_side or r.height < min_side:
        return False
    if r.width > max_side or r.height > max_side:
        return False
    if r.width / max(r.height, 0.01) > 6 or r.height / max(r.width, 0.01) > 6:
        return False
    has_fill = drawing.get("fill") is not None and drawing.get("fill_opacity", 1) > 0.1
    has_stroke = drawing.get("color") is not None
    return has_fill or has_stroke


def _has_hatch_pattern(drawing: dict) -> bool:
    """A hatch swatch is a drawing whose own items contain many short line segments
    and no rectangular fill. Checking the swatch drawing itself (not neighbors)
    avoids false positives from basemap lines passing through."""
    items = drawing.get("items") or []
    l_count = sum(1 for it in items if it[0] == "l")
    re_count = sum(1 for it in items if it[0] == "re")
    if l_count >= 6 and re_count == 0 and drawing.get("fill") is None:
        return True
    return False


def _is_near_white(rgb) -> bool:
    if rgb is None:
        return False
    r, g, b = (int(round(c * 255)) if 0 <= c <= 1 else int(c) for c in rgb[:3])
    return min(r, g, b) > 245


def _text_blocks(page: fitz.Page) -> list[dict]:
    raw = page.get_text("dict")
    out = []
    for block in raw.get("blocks", []):
        if "lines" not in block:
            continue
        for line in block["lines"]:
            bbox = line.get("bbox")
            text = " ".join(span.get("text", "") for span in line.get("spans", []))
            text = text.strip()
            if not text or not bbox:
                continue
            out.append({"bbox": fitz.Rect(bbox), "text": text})
    return out


def _find_legend_anchor(texts: list[dict]) -> fitz.Rect | None:
    for t in texts:
        tl = t["text"].lower().strip()
        if tl.startswith("légende") or tl.startswith("legende") or tl == "legend":
            return t["bbox"]
    return None


def _cluster_swatches(candidates: list[dict], anchor: fitz.Rect | None, page_rect: fitz.Rect) -> list[dict]:
    if not candidates:
        return []
    if anchor is not None:
        region = fitz.Rect(anchor.x0 - 20, anchor.y0, page_rect.x1, page_rect.y1)
        region.x0 = max(region.x0, 0)
        region.x1 = min(region.x1, page_rect.x1)
        region.y1 = min(anchor.y1 + max(page_rect.height * 0.6, 300), page_rect.y1)
        return [c for c in candidates if region.intersects(_rect_of(c))]
    # No LEGENDE anchor: try bottom-right quadrant first
    region = fitz.Rect(page_rect.x1 * 0.55, page_rect.y1 * 0.4, page_rect.x1, page_rect.y1)
    in_region = [c for c in candidates if region.intersects(_rect_of(c))]
    # Tables/programme pages can have hundreds of small fills mimicking swatches.
    # A real legend has at most ~30 swatches; if we see >60, bail — better to
    # emit legend_failed than to invent labels.
    if in_region and len(in_region) <= 60:
        return in_region
    if len(candidates) > 60:
        return []
    return candidates


_SKIP_LABELS = {"echelle", "legende", "légende", "commune de", "plan directeur", "urbaplan"}

_MAX_LABEL_LEN = 120  # real legend labels are short; paragraph text isn't
_MAX_HORIZONTAL_GAP_PT = 40  # tightened from 80 (paragraph text reach)

# Paragraph-text rejection patterns — a label matching any of these is NOT a legend label
_SENTENCE_BREAK = re.compile(r"\.\s+[A-ZÉÈÊÀÂÎÔÛÇ]")       # ". Next"
_ENDS_WITH_COLON = re.compile(r":\s*$")
_STARTS_WITH_BULLET = re.compile(r"^\s*(?:[>•·]|-\s|–\s|—\s)")
_PURE_NUMERIC_PATTERN = re.compile(r"^\s*\d+(?:\.\d+)?(?:\s*[-–]\s*\d+(?:\.\d+)?)?\s*$")
_MULTI_SENTENCE = re.compile(r"[.!?]\s+\w+.*[.!?]")        # at least two sentences


def _is_paragraph_text(s: str) -> bool:
    if _SENTENCE_BREAK.search(s):
        return True
    if _ENDS_WITH_COLON.search(s):
        return True
    if _STARTS_WITH_BULLET.search(s):
        return True
    if _PURE_NUMERIC_PATTERN.match(s):
        return True
    if _MULTI_SENTENCE.search(s):
        return True
    return False


def _is_noise_label(label: str) -> bool:
    lo = label.lower().strip(" -/–—")
    if not lo:
        return True
    for bad in _SKIP_LABELS:
        if bad in lo:
            return True
    if re.match(r"^[\d\s'’.,m]+$", lo):  # pure scale bar text
        return True
    if len(lo) < 3:
        return True
    return False


def _pair_swatch_to_label(
    swatch_rect: fitz.Rect,
    texts: list[dict],
    legend_center_x: float | None = None,
) -> str | None:
    """Pair a swatch with its label. Tightened in v0.2:
    - horizontal gap ≤ 40 pt (was 80)
    - cleaned label ≤ 120 chars
    - reject paragraph-text patterns (sentence breaks, colons, bullets, numeric ranges, multi-sentence)
    - positional sanity: text must be on the side opposite to the legend center
      (swatches left-of-center → labels to their right; swatches right-of-center → labels to their left)
    """
    # Decide allowed side based on swatch vs legend center
    allow_right = True
    allow_left = False
    if legend_center_x is not None:
        swatch_cx = (swatch_rect.x0 + swatch_rect.x1) / 2
        if swatch_cx > legend_center_x:
            # swatch is on the right → labels likely to its left
            allow_right = False
            allow_left = True

    best_cleaned = None
    best_score = 1e9
    for t in texts:
        tb: fitz.Rect = t["bbox"]
        text = t["text"]
        if len(text) > _MAX_LABEL_LEN:
            continue
        # Clean separators first, then test for paragraph patterns.
        cleaned = re.sub(r"^[\s\-/–—>·•]+", "", text).strip()
        cleaned = re.sub(r"[\s\-/–—]+$", "", cleaned).strip()
        if not cleaned:
            continue
        if len(cleaned) > _MAX_LABEL_LEN:
            continue
        if _is_paragraph_text(cleaned):
            continue
        if _is_noise_label(cleaned):
            continue
        # Horizontal side + gap
        if tb.x0 >= swatch_rect.x1 - 2 and allow_right:
            dx = tb.x0 - swatch_rect.x1
        elif tb.x1 <= swatch_rect.x0 + 2 and allow_left:
            dx = swatch_rect.x0 - tb.x1
        else:
            continue
        if not (5 <= dx <= _MAX_HORIZONTAL_GAP_PT):
            continue
        y_center_swatch = (swatch_rect.y0 + swatch_rect.y1) / 2
        y_center_text = (tb.y0 + tb.y1) / 2
        dy = abs(y_center_text - y_center_swatch)
        if dy > max(6, (swatch_rect.height + tb.height) / 2):
            continue
        score = dx + dy * 2
        if score < best_score:
            best_score = score
            best_cleaned = cleaned
    return best_cleaned


_MONTHS = r"(?:janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)"


def _find_title(texts: list[dict], page_rect: fitz.Rect, legend_bbox: fitz.Rect | None) -> str:
    candidates = []
    for t in texts:
        if legend_bbox and legend_bbox.intersects(t["bbox"]):
            continue
        txt = t["text"].strip()
        lo = txt.lower()
        if len(txt) < 4 or len(txt) > 80:
            continue
        if "urbaplan" in lo or "plan directeur" in lo:
            continue
        if "commune de" in lo or "légende" in lo or "legende" in lo:
            continue
        if "echelle" in lo or "échelle" in lo:
            continue
        if re.match(r"^[\d\s'’.,m]+$", txt):
            continue  # scale bar numbers/dimensions
        if re.search(_MONTHS, lo):
            continue  # dates
        if re.match(r"^\d{1,2}\s+", txt):  # e.g. "25 octobre 2021"
            continue
        candidates.append((t["bbox"].height, txt, t["bbox"].y0))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: (-x[0], x[2]))
    return candidates[0][1]


def _classify_fill_type(drawing: dict) -> str:
    has_fill = drawing.get("fill") is not None and drawing.get("fill_opacity", 1) > 0.1
    has_stroke = drawing.get("color") is not None
    if _has_hatch_pattern(drawing):
        return "hatch"
    if has_fill:
        return "solid"
    if has_stroke:
        return "stroke_only"
    return "solid"


def detect_legend(page: fitz.Page) -> dict:
    page_rect = page.rect
    drawings = page.get_drawings()
    texts = _text_blocks(page)

    candidates = [d for d in drawings if _is_swatch_candidate(d)]
    anchor = _find_legend_anchor(texts)
    swatches = _cluster_swatches(candidates, anchor, page_rect)

    # Compute legend center x from swatch cluster centroid (for positional sanity in pairing)
    legend_center_x = None
    if swatches:
        xs = []
        for sw in swatches:
            r = _rect_of(sw)
            if r is None:
                continue
            xs.append((r.x0 + r.x1) / 2)
        if xs:
            legend_center_x = sum(xs) / len(xs)

    raw_entries = []
    unlabeled = 0
    for sw in swatches:
        rect = _rect_of(sw)
        if rect is None:
            continue
        label = _pair_swatch_to_label(rect, texts, legend_center_x=legend_center_x)
        if not label:
            unlabeled += 1
            continue
        fill_type = _classify_fill_type(sw)
        if fill_type == "solid":
            color = sw.get("fill") or sw.get("color")
        else:
            color = sw.get("color") or sw.get("fill")
        if color is None:
            continue
        # Drop near-white background swatches (common noise)
        if fill_type == "solid" and _is_near_white(color):
            continue
        hex_color = rgb_to_hex(color)
        raw_entries.append({
            "label": label,
            "slug": slugify_label(label),
            "fill_color": list(color[:3]) if color else None,
            "fill_color_hex": hex_color,
            "fill_type": fill_type,
            "swatch_bbox": [rect.x0, rect.y0, rect.x1, rect.y1],
        })

    # Dedupe in two passes:
    # 1. By swatch bbox: if the same swatch was paired with multiple labels (y-alignment
    #    fuzziness), keep only the closest.
    # 2. By (color, fill_type): same color → same layer; keep first-wins.
    # 3. By slug, preferring fill_type solid > stroke_only > hatch.
    seen_bbox: set[tuple] = set()
    pass_1 = []
    for e in raw_entries:
        key = tuple(round(c, 1) for c in e["swatch_bbox"])
        if key in seen_bbox:
            continue
        seen_bbox.add(key)
        pass_1.append(e)

    seen_color: dict[tuple, dict] = {}
    for e in pass_1:
        key = (e["fill_color_hex"], e["fill_type"])
        if key not in seen_color:
            seen_color[key] = e
    pass_2 = list(seen_color.values())

    _priority = {"solid": 0, "stroke_only": 1, "hatch": 2}
    by_slug: dict[str, dict] = {}
    for e in pass_2:
        prev = by_slug.get(e["slug"])
        if prev is None or _priority[e["fill_type"]] < _priority[prev["fill_type"]]:
            by_slug[e["slug"]] = e
    entries = list(by_slug.values())

    legend_bbox = None
    if entries:
        xs0 = min(e["swatch_bbox"][0] for e in entries)
        ys0 = min(e["swatch_bbox"][1] for e in entries)
        xs1 = max(e["swatch_bbox"][2] for e in entries)
        ys1 = max(e["swatch_bbox"][3] for e in entries)
        if anchor is not None:
            xs0 = min(xs0, anchor.x0)
            ys0 = min(ys0, anchor.y0)
        legend_bbox = fitz.Rect(xs0 - 4, ys0 - 4, min(page_rect.x1, xs1 + 250), min(page_rect.y1, ys1 + 10))

    title = _find_title(texts, page_rect, legend_bbox)

    # Compute map bbox = page bbox minus legend + minus large text frames at edges
    map_bbox = fitz.Rect(page_rect)
    if legend_bbox is not None:
        # If legend is on the right side (common), shrink map_bbox.x1
        if legend_bbox.x0 > page_rect.width * 0.55:
            map_bbox.x1 = min(map_bbox.x1, legend_bbox.x0 - 2)
        elif legend_bbox.x1 < page_rect.width * 0.45:
            map_bbox.x0 = max(map_bbox.x0, legend_bbox.x1 + 2)
        elif legend_bbox.y0 > page_rect.height * 0.55:
            map_bbox.y1 = min(map_bbox.y1, legend_bbox.y0 - 2)
        elif legend_bbox.y1 < page_rect.height * 0.45:
            map_bbox.y0 = max(map_bbox.y0, legend_bbox.y1 + 2)

    return {
        "legend_bbox": [legend_bbox.x0, legend_bbox.y0, legend_bbox.x1, legend_bbox.y1] if legend_bbox else None,
        "map_bbox": [map_bbox.x0, map_bbox.y0, map_bbox.x1, map_bbox.y1],
        "title": title,
        "entries": entries,
        "swatch_unlabeled": unlabeled,
    }
