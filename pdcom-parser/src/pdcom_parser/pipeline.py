"""Per-commune extraction pipeline — glues modules together."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import fitz
from shapely.geometry import shape
from shapely.ops import transform as shp_transform

from .classify import classify_page
from .extract import clip_and_score_layers, extract_layers
from .georef import georeference
from .hatch import recover_hatch_zones
from .legend import detect_legend
from .normalize import classify_theme
from .qa import render_qa_image
from .report import append_log
from .export import write_layer_geojson, write_manifest, write_combined_geojson


def _page_type_to_status(page_type: str) -> str | None:
    if page_type in ("cover", "text", "toc", "unknown"):
        return "nonmap"
    return None


def _guess_template(pdf: fitz.Document, first_map_page_idx: int | None) -> str:
    """Rough fingerprint of which firm's template the PDCom uses. Checked once per
    PDF on the first map page. 'urbaplan_standard' is the reference template
    (matches v0.2 fixture); anything else we fail to recognise is 'alternate_legend'."""
    if first_map_page_idx is None:
        return "unknown"
    try:
        text = pdf[first_map_page_idx].get_text().lower()
    except Exception:
        return "unknown"
    if "urbaplan" in text:
        return "urbaplan_standard"
    return "alternate_legend"


def extract_pdf(
    pdf_path: Path,
    commune_bfs: int,
    commune_name: str,
    boundary_lv95_geojson: dict,
    output_dir: Path,
    log_path: Path,
    min_confidence: float = 0.5,
    page_plan: "PagePlan | None" = None,
    reject_labels: set[str] | None = None,
) -> dict:
    """Run extraction on all pages of a PDF. Returns a manifest dict.

    v0.3 changes:
    - When `page_plan` is provided, process ONLY the pages listed there
      (bypass classify entirely). `map` pages use min_confidence=0.8,
      `maybe` pages use 0.85 (stricter to compensate for lower confidence hints).
    - `reject_labels` is a set of strings (normalized lowercase) — layer_labels
      matching any entry are dropped as leaks (commune-name-as-label protection).
    """
    from .hints import PagePlan  # local import to avoid circular
    boundary_lv95 = shape(boundary_lv95_geojson)

    pages_report: list[dict] = []
    features_by_page: dict[int, list[dict]] = {}
    all_layer_entries: list[dict] = []
    reject_labels_lower = {l.lower().strip() for l in (reject_labels or set())}

    first_map_page_idx: int | None = None
    legend_failed_streak = 0
    LEGEND_FAILED_BAIL_THRESHOLD = 15

    with fitz.open(pdf_path) as pdf:
        total_pages = pdf.page_count

        # v0.3: hints-driven page selection. When a plan is supplied, skip all
        # pages not in it and bypass classify for pages that ARE in it.
        if page_plan is not None:
            pages_to_process = sorted(page_plan.all_pages)
            append_log(log_path, {
                "kind": "pdf_plan", "commune_bfs": commune_bfs, "pdf": pdf_path.name,
                "map_pages": len(page_plan.map_pages), "maybe_pages": len(page_plan.maybe_pages),
            })
            template_guess = "hints_driven"
        else:
            # Legacy v0.2 behaviour: detect first map page → guess template → skip alternates
            for pi in range(total_pages):
                if first_map_page_idx is None and classify_page(pdf[pi])["type"] == "map":
                    first_map_page_idx = pi
                    break
            template_guess = _guess_template(pdf, first_map_page_idx)
            if template_guess == "alternate_legend":
                append_log(log_path, {
                    "kind": "pdf_bail", "commune_bfs": commune_bfs, "pdf": pdf_path.name,
                    "reason": "alternate_legend template — v0.2 legend detector doesn't handle it",
                    "template_guess": "alternate_legend",
                })
                manifest = {
                    "commune_bfs": commune_bfs, "commune_name": commune_name,
                    "pdf_filename": pdf_path.name, "pdf_page_count": total_pages,
                    "pages_processed": 0, "pages_map": 0, "pages_ok": 0,
                    "pages_legend_failed": 0, "pages_low_confidence": 0,
                    "features_total": 0, "themes_found": [],
                    "template_guess": template_guess, "pages": [],
                }
                write_manifest(output_dir / "manifest.json", manifest)
                try:
                    render_qa_image(boundary_lv95, [], output_dir / "qa.png",
                                    title=f"{commune_name} — {pdf_path.name} (alternate_legend — skipped)")
                except Exception:
                    pass
                append_log(log_path, {"kind": "commune", "commune_bfs": commune_bfs,
                                       "commune_name": commune_name, "pdf": pdf_path.name,
                                       "status": "skipped_alternate_template",
                                       "pages_ok": 0, "pages_total_map": 0, "features": 0})
                return {"manifest": manifest, "features_by_page": {}, "pages_report": []}
            pages_to_process = list(range(1, total_pages + 1))

        for page_number in pages_to_process:
            pi = page_number - 1
            if pi < 0 or pi >= total_pages:
                continue
            page = pdf[pi]

            # Determine effective confidence gate for this page
            if page_plan is not None:
                is_maybe_page = page_number in page_plan.maybe_pages and page_number not in page_plan.map_pages
                page_min_confidence = 0.85 if is_maybe_page else min_confidence
            else:
                is_maybe_page = False
                page_min_confidence = min_confidence

            info = classify_page(page)
            page_rec = {
                "page_number": page_number,
                "page_type": info["type"],
                "drawing_count": info["drawing_count"],
                "has_raster": info["has_raster"],
                "map_theme": None,
                "map_title": None,
                "legend_json": None,
                "georef_confidence": None,
                "extraction_status": None,
                "hints_tier": "maybe" if is_maybe_page else ("map" if page_plan else "auto"),
            }

            # Non-map skip only applies when NO hints drive us (hints override classify)
            if page_plan is None:
                non_map = _page_type_to_status(info["type"])
                if non_map:
                    page_rec["extraction_status"] = "skipped"
                    pages_report.append(page_rec)
                    append_log(log_path, {"kind": "page", "commune_bfs": commune_bfs, "page": page_number, "status": "nonmap", "type": info["type"], "feature_count": 0})
                    continue

            try:
                legend = detect_legend(page)
            except Exception as e:
                page_rec["extraction_status"] = "legend_failed"
                pages_report.append(page_rec)
                append_log(log_path, {"kind": "page", "commune_bfs": commune_bfs, "page": page_number, "status": "legend_failed", "error": str(e), "feature_count": 0})
                continue

            entries_ok = legend.get("entries", [])
            if len(entries_ok) < 3:
                page_rec["extraction_status"] = "legend_failed"
                page_rec["legend_json"] = {"entries_detected": len(entries_ok), "map_title": legend.get("title")}
                pages_report.append(page_rec)
                append_log(log_path, {"kind": "page", "commune_bfs": commune_bfs, "page": page_number, "status": "legend_failed", "entries_detected": len(entries_ok), "feature_count": 0})
                legend_failed_streak += 1
                # Only bail early in legacy (non-hints) mode. In hints mode we trust the
                # hints file — each listed page might use a different template; don't
                # bail on a streak of failures in one section.
                if page_plan is None and legend_failed_streak >= LEGEND_FAILED_BAIL_THRESHOLD:
                    append_log(log_path, {"kind": "pdf_bail", "commune_bfs": commune_bfs, "pdf": pdf_path.name, "reason": f"{LEGEND_FAILED_BAIL_THRESHOLD} consecutive map pages with <3 legend entries — alternate template"})
                    break
                continue
            legend_failed_streak = 0

            title = legend.get("title", "")
            legend_labels = [e.get("label", "") for e in legend.get("entries", [])]
            theme = classify_theme(title, legend_labels=legend_labels)
            page_rec["map_title"] = title
            page_rec["map_theme"] = theme
            page_rec["legend_json"] = {"title": title, "entries": legend.get("entries", []), "map_bbox": legend.get("map_bbox"), "legend_bbox": legend.get("legend_bbox")}

            try:
                layers_pdf = extract_layers(page, legend)
            except Exception as e:
                page_rec["extraction_status"] = "extract_failed"
                pages_report.append(page_rec)
                append_log(log_path, {"kind": "page", "commune_bfs": commune_bfs, "page": page_number, "status": "extract_failed", "error": str(e), "feature_count": 0})
                continue

            # Handle hatches: if a layer's fill_type is 'hatch', use raw_drawings → lines → recover polygons
            for slug, entry in list(layers_pdf.items()):
                if entry.get("fill_type") != "hatch":
                    continue
                raw = entry.get("raw_drawings") or []
                from .extract import drawing_to_lines
                page_h = page.rect.height
                lines = []
                for d in raw:
                    lines.extend(drawing_to_lines(d, page_h))
                polys = recover_hatch_zones(lines)
                entry["geoms"] = polys
                entry.pop("raw_drawings", None)

            try:
                layers_lv95, confidence = georeference(
                    layers_pdf, boundary_lv95, page,
                    map_bbox=tuple(legend["map_bbox"]) if legend.get("map_bbox") else None,
                )
            except Exception as e:
                page_rec["extraction_status"] = "extract_failed"
                pages_report.append(page_rec)
                append_log(log_path, {"kind": "page", "commune_bfs": commune_bfs, "page": page_number, "status": "extract_failed", "error": str(e), "feature_count": 0})
                continue

            page_rec["georef_confidence"] = round(confidence, 3)

            if confidence < page_min_confidence:
                page_rec["extraction_status"] = "low_confidence"
                pages_report.append(page_rec)
                append_log(log_path, {"kind": "page", "commune_bfs": commune_bfs, "page": page_number, "status": "low_confidence", "confidence": confidence, "gate": page_min_confidence, "feature_count": 0})
                continue

            # Fix 1 + 2 + 4: clip to commune polygon in LV95, world-coord area filters,
            # per-feature confidence = page_conf × clip_ratio.
            features_clipped, drops = clip_and_score_layers(layers_lv95, boundary_lv95, confidence)

            # v0.3: commune-name-as-label leak filter ("Bellevue" bug fix).
            # Drop any feature whose legend label matches a commune name in the canton.
            if reject_labels_lower:
                before = len(features_clipped)
                features_clipped = [
                    f for f in features_clipped
                    if (f.get("label") or "").strip().lower() not in reject_labels_lower
                ]
                dropped_for_commune_name = before - len(features_clipped)
                if dropped_for_commune_name > 0:
                    append_log(log_path, {
                        "kind": "commune_name_label_dropped", "commune_bfs": commune_bfs,
                        "page": page_number, "count": dropped_for_commune_name,
                    })

            # Export per-page layer files + accumulate features for DB insert
            page_dir = output_dir / f"pages/p{page_number:03d}_{theme or 'unknown'}"
            feats_for_page: list[dict] = []
            # Group by slug for per-layer geojson file (carry per-feature properties through)
            by_slug_entries: dict[str, dict] = {}
            for f in features_clipped:
                from shapely.geometry import mapping
                props = {**f.get("properties", {}), "page_number": page_number, "confidence": f["confidence"]}
                feats_for_page.append({
                    "map_theme": theme or "unknown",
                    "label": f["label"],
                    "slug": f["slug"],
                    "color": f["color"],
                    "fill_type": f["fill_type"],
                    "geometry": mapping(f["geom"]),
                    "confidence": f["confidence"],
                    "properties": props,
                })
                slot = by_slug_entries.setdefault(f["slug"], {
                    "slug": f["slug"], "label": f["label"], "color": f["color"],
                    "fill_type": f["fill_type"], "geom_props": [],
                })
                slot["geom_props"].append((f["geom"], props))
            for slug, entry in by_slug_entries.items():
                write_layer_geojson(page_dir / f"{slug}.geojson", entry, slug)
                all_layer_entries.append({
                    **entry,
                    "geoms": [gp[0] for gp in entry["geom_props"]],
                    "map_theme": theme, "page_number": page_number,
                })

            features_by_page[page_number] = feats_for_page
            page_rec["extraction_status"] = "ok"
            page_rec["features_count"] = len(feats_for_page)
            page_rec["dropped_count"] = len(drops)
            pages_report.append(page_rec)
            append_log(log_path, {
                "kind": "page", "commune_bfs": commune_bfs, "page": page_number,
                "status": "ok", "confidence": round(confidence, 3),
                "feature_count": len(feats_for_page), "dropped_count": len(drops),
                "theme": theme,
            })

    # Combined GeoJSON per commune
    combined = output_dir / "combined/all_layers.geojson"
    combined_count = write_combined_geojson(combined, all_layer_entries)

    # QA image (Fix 6). One PNG per PDF output dir; overlays features on commune boundary.
    try:
        qa_features = []
        for entry in all_layer_entries:
            for g in entry.get("geoms", []):
                qa_features.append({
                    "geom": g,
                    "color": entry.get("color"),
                    "label": entry.get("label"),
                    "fill_type": entry.get("fill_type", "solid"),
                })
        render_qa_image(
            boundary_lv95,
            qa_features,
            output_dir / "qa.png",
            title=f"{commune_name} — {pdf_path.name} ({len(qa_features)} features)",
        )
    except Exception as e:
        append_log(log_path, {"kind": "qa", "commune_bfs": commune_bfs, "status": "failed", "error": str(e)})

    pages_map = sum(1 for p in pages_report if p["page_type"] == "map")
    pages_ok = sum(1 for p in pages_report if p["extraction_status"] == "ok")
    pages_legend_failed = sum(1 for p in pages_report if p["extraction_status"] == "legend_failed")
    pages_low_confidence = sum(1 for p in pages_report if p["extraction_status"] == "low_confidence")
    themes_found = sorted({p.get("map_theme") for p in pages_report if p.get("map_theme") and p.get("map_theme") != "unknown"})
    manifest = {
        "commune_bfs": commune_bfs,
        "commune_name": commune_name,
        "pdf_filename": pdf_path.name,
        "pdf_page_count": total_pages,
        "pages_processed": len(pages_report),
        "pages_map": pages_map,
        "pages_ok": pages_ok,
        "pages_legend_failed": pages_legend_failed,
        "pages_low_confidence": pages_low_confidence,
        "features_total": sum(len(v) for v in features_by_page.values()),
        "themes_found": themes_found,
        "template_guess": template_guess,
        "pages": pages_report,
    }
    write_manifest(output_dir / "manifest.json", manifest)

    ok_count = manifest["pages_ok"]
    map_pages = sum(1 for p in pages_report if p["extraction_status"] not in ("skipped", None))
    status = "ok" if ok_count >= max(1, int(0.7 * max(map_pages, 1))) else ("partial" if ok_count > 0 else "failed")
    append_log(log_path, {"kind": "commune", "commune_bfs": commune_bfs, "commune_name": commune_name, "pdf": pdf_path.name, "status": status, "pages_ok": ok_count, "pages_total_map": map_pages, "features": manifest["features_total"]})

    return {"manifest": manifest, "features_by_page": features_by_page, "pages_report": pages_report}
