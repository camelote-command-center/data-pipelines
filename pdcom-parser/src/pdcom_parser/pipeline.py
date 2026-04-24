"""Per-commune extraction pipeline — glues modules together."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import fitz
from shapely.geometry import shape
from shapely.ops import transform as shp_transform

from .classify import classify_page
from .extract import extract_layers
from .georef import georeference
from .hatch import recover_hatch_zones
from .legend import detect_legend
from .normalize import classify_theme
from .report import append_log
from .export import write_layer_geojson, write_manifest, write_combined_geojson


def _page_type_to_status(page_type: str) -> str | None:
    if page_type in ("cover", "text", "toc", "unknown"):
        return "nonmap"
    return None


def extract_pdf(
    pdf_path: Path,
    commune_bfs: int,
    commune_name: str,
    boundary_lv95_geojson: dict,
    output_dir: Path,
    log_path: Path,
    min_confidence: float = 0.5,
) -> dict:
    """Run extraction on all pages of a PDF. Returns a manifest dict."""
    boundary_lv95 = shape(boundary_lv95_geojson)

    pages_report: list[dict] = []
    features_by_page: dict[int, list[dict]] = {}
    all_layer_entries: list[dict] = []

    with fitz.open(pdf_path) as pdf:
        total_pages = pdf.page_count
        for pi in range(total_pages):
            page = pdf[pi]
            page_number = pi + 1
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
            }

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
                continue

            title = legend.get("title", "")
            theme = classify_theme(title)
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

            if confidence < min_confidence:
                page_rec["extraction_status"] = "low_confidence"
                pages_report.append(page_rec)
                append_log(log_path, {"kind": "page", "commune_bfs": commune_bfs, "page": page_number, "status": "low_confidence", "confidence": confidence, "feature_count": 0})
                continue

            # Export per-page layer files + accumulate features for DB insert
            page_dir = output_dir / f"pages/p{page_number:03d}_{theme or 'unknown'}"
            feats_for_page: list[dict] = []
            for slug, entry in layers_lv95.items():
                ge = [g for g in entry.get("geoms", []) if g is not None and not g.is_empty]
                if not ge:
                    continue
                write_layer_geojson(page_dir / f"{slug}.geojson", {**entry, "geoms": ge}, slug)
                for g in ge:
                    from shapely.geometry import mapping
                    feats_for_page.append({
                        "map_theme": theme or "unknown",
                        "label": entry.get("label"),
                        "slug": slug,
                        "color": entry.get("color"),
                        "fill_type": entry.get("fill_type"),
                        "geometry": mapping(g),
                        "confidence": round(confidence, 3),
                        "properties": {"page_number": page_number},
                    })
                all_layer_entries.append({**entry, "slug": slug, "map_theme": theme, "page_number": page_number})
            features_by_page[page_number] = feats_for_page
            page_rec["extraction_status"] = "ok"
            pages_report.append(page_rec)
            append_log(log_path, {"kind": "page", "commune_bfs": commune_bfs, "page": page_number, "status": "ok", "confidence": round(confidence, 3), "feature_count": len(feats_for_page), "theme": theme})

    # Combined GeoJSON per commune
    combined = output_dir / "combined/all_layers.geojson"
    combined_count = write_combined_geojson(combined, all_layer_entries)

    manifest = {
        "commune_bfs": commune_bfs,
        "commune_name": commune_name,
        "pdf_filename": pdf_path.name,
        "pdf_page_count": total_pages,
        "pages_processed": len(pages_report),
        "pages_ok": sum(1 for p in pages_report if p["extraction_status"] == "ok"),
        "features_total": sum(len(v) for v in features_by_page.values()),
        "pages": pages_report,
    }
    write_manifest(output_dir / "manifest.json", manifest)

    ok_count = manifest["pages_ok"]
    map_pages = sum(1 for p in pages_report if p["extraction_status"] not in ("skipped", None))
    status = "ok" if ok_count >= max(1, int(0.7 * max(map_pages, 1))) else ("partial" if ok_count > 0 else "failed")
    append_log(log_path, {"kind": "commune", "commune_bfs": commune_bfs, "commune_name": commune_name, "pdf": pdf_path.name, "status": status, "pages_ok": ok_count, "pages_total_map": map_pages, "features": manifest["features_total"]})

    return {"manifest": manifest, "features_by_page": features_by_page, "pages_report": pages_report}
