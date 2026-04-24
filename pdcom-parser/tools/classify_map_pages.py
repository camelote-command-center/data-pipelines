#!/usr/bin/env python3
"""Classify every page of every PDCom PDF as MAP / MAYBE / TEXT.

Walks a PDF corpus folder recursively, scores each page using cheap
PyMuPDF-derived signals (distinct fill colors, fill count, text ratio,
coverage), and emits:

1. A per-PDF terminal report with per-page classification and signals.
2. A YAML hints file (map_page_hints_auto.yaml) with `source_folder`
   tags so downstream tooling knows whether a PDF is in the already-
   processed batch or the new_ones/ batch.
3. A calibration report when the provided manual hints file is given
   (recall + precision per PDF that has manual ground truth).

Usage:
    python tools/classify_map_pages.py \\
        --pdf-dir "/Users/a/Desktop/Lamap Reshape/PDCom/" \\
        --out configs/map_page_hints_auto.yaml \\
        --manual-hints configs/map_page_hints.yaml

No new dependencies — uses PyMuPDF (already in the parser project) + PyYAML.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import fitz  # PyMuPDF
import yaml


# ---- Classification thresholds ----------------------------------------------
# Calibrated against manual hints (§4): initial spec values hit ~20% recall because
# alternate-template map pages often have <100 vector fills (raster-backed) or high
# text_ratio (dense legend annotations). Spec says "recall ≥ 0.95" is the hard
# target; precision as low as 0.60 is fine because MAYBE means manual review.
# So MAP stays strict (high precision) and MAYBE is ultra-permissive (high recall).

MAP_MIN_DISTINCT_COLORS = 6
MAP_MIN_FILL_COUNT = 200
MAP_MAX_TEXT_RATIO = 0.35
MAP_MIN_COVERAGE = 0.25

# MAYBE: cast a wide net. Any page with even a handful of vector fills OR an
# embedded image is worth eyeballing — raster-embedded maps are common in
# alternate templates.
MAYBE_MIN_DISTINCT_COLORS = 1
MAYBE_MIN_FILL_COUNT = 5
MAYBE_MAX_TEXT_RATIO = 0.97   # effectively disabled; pages with lots of text can still be maps

# Cover-page downgrade: big filled area, few colors → TEXT, not MAP
COVER_COVERAGE_THRESHOLD = 0.80
COVER_MAX_COLORS = 3

# Raster-heavy guard: any page with an embedded image AND not-pure-cover should
# be MAYBE'd. Many scanned PDComs have 1 big image per page + minimal vectors.
RASTER_MIN_IMAGES = 1
RASTER_MAX_FILL_COUNT = 200


# ---- Per-page signal extraction ---------------------------------------------

@dataclass
class PageSignals:
    page_number: int
    distinct_colors: int
    fill_count: int
    total_text_chars: int
    fill_coverage: float   # [0, 1]
    image_count: int
    drawing_count: int

    @property
    def text_ratio(self) -> float:
        denom = self.total_text_chars + self.fill_count
        return (self.total_text_chars / denom) if denom > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "colors": self.distinct_colors,
            "fills": self.fill_count,
            "text_ratio": round(self.text_ratio, 3),
            "coverage": round(self.fill_coverage, 3),
        }


def _rgb_key(color):
    """Normalize a PyMuPDF color (tuple of 0..1 floats, or None) to an RGB
    integer tuple for deduping."""
    if color is None:
        return None
    return tuple(int(round(c * 255)) for c in color[:3])


def extract_page_signals(page: fitz.Page) -> PageSignals:
    drawings = page.get_drawings()
    page_area = max(page.rect.width * page.rect.height, 1.0)

    distinct_colors: set = set()
    fill_count = 0
    fill_area_sum = 0.0

    for d in drawings:
        fill = d.get("fill")
        opacity = d.get("fill_opacity", 1.0)
        if fill is None or opacity is None or opacity < 0.1:
            continue
        key = _rgb_key(fill)
        if key is None:
            continue
        distinct_colors.add(key)
        fill_count += 1
        rect = d.get("rect")
        if rect is not None:
            try:
                r = fitz.Rect(rect)
                fill_area_sum += max(r.width, 0) * max(r.height, 0)
            except Exception:
                pass

    try:
        text_chars = len(page.get_text())
    except Exception:
        text_chars = 0

    try:
        images = page.get_images(full=False) or []
    except Exception:
        images = []

    return PageSignals(
        page_number=page.number + 1,
        distinct_colors=len(distinct_colors),
        fill_count=fill_count,
        total_text_chars=text_chars,
        fill_coverage=min(fill_area_sum / page_area, 1.0),
        image_count=len(images),
        drawing_count=len(drawings),
    )


# ---- Classification ---------------------------------------------------------

def classify(signals: PageSignals) -> tuple[str, str | None]:
    """Return (label, optional_note). label ∈ {MAP, MAYBE, TEXT}."""
    # Cover-page rule: big fill + few colors → TEXT
    if signals.fill_coverage > COVER_COVERAGE_THRESHOLD and signals.distinct_colors < COVER_MAX_COLORS:
        return "TEXT", "cover-like (large fill, few colors)"

    # Raster-heavy: any embedded image → MAYBE (scanned/image-backed maps are
    # common in alternate templates). Exception: if the image-bearing page
    # already cleared the MAP thresholds (huge vector content AND images),
    # classify as MAP normally below.
    if (
        signals.image_count >= RASTER_MIN_IMAGES
        and not (
            signals.distinct_colors >= MAP_MIN_DISTINCT_COLORS
            and signals.fill_count >= MAP_MIN_FILL_COUNT
            and signals.text_ratio < MAP_MAX_TEXT_RATIO
            and signals.fill_coverage > MAP_MIN_COVERAGE
        )
    ):
        return "MAYBE", "raster-heavy, manual review"

    # MAP
    if (
        signals.distinct_colors >= MAP_MIN_DISTINCT_COLORS
        and signals.fill_count >= MAP_MIN_FILL_COUNT
        and signals.text_ratio < MAP_MAX_TEXT_RATIO
        and signals.fill_coverage > MAP_MIN_COVERAGE
    ):
        return "MAP", None

    # MAYBE
    if (
        signals.distinct_colors >= MAYBE_MIN_DISTINCT_COLORS
        and signals.fill_count >= MAYBE_MIN_FILL_COUNT
        and signals.text_ratio < MAYBE_MAX_TEXT_RATIO
    ):
        return "MAYBE", None

    return "TEXT", None


# ---- PDF scanning -----------------------------------------------------------

@dataclass
class PdfReport:
    path: Path
    source_folder: str  # "processed" or "new"
    total_pages: int
    map_pages: list[dict] = field(default_factory=list)
    maybe_pages: list[dict] = field(default_factory=list)
    text_pages: int = 0
    error: str | None = None


def _source_folder_for(path: Path, root: Path) -> str:
    try:
        rel = path.relative_to(root)
    except ValueError:
        rel = path
    # If the first path component is 'new_ones' (or any nested folder
    # containing 'new'), flag as new; otherwise processed.
    parts = rel.parts
    if any("new_ones" in p for p in parts[:-1]):
        return "new"
    if len(parts) > 1:
        # Files under any subfolder other than 'new_ones': still tag as new
        # (spec §7: Ilan may add others).
        return "new"
    return "processed"


def scan_pdf(pdf_path: Path, root: Path, max_seconds: float = 120.0, all_signals: dict | None = None) -> PdfReport:
    source_folder = _source_folder_for(pdf_path, root)
    report = PdfReport(path=pdf_path, source_folder=source_folder, total_pages=0)
    start = time.time()
    try:
        pdf = fitz.open(pdf_path)
    except Exception as e:
        report.error = f"open_failed: {e}"
        return report

    try:
        if pdf.needs_pass:
            report.error = "password-protected"
            return report
        report.total_pages = pdf.page_count
        per_pdf_signals: list[dict] = []
        for pi in range(pdf.page_count):
            if time.time() - start > max_seconds:
                report.error = f"timeout after {pi}/{pdf.page_count} pages"
                break
            try:
                page = pdf[pi]
                sig = extract_page_signals(page)
                label, note = classify(sig)
            except Exception as e:
                report.text_pages += 1
                continue
            per_pdf_signals.append({
                "page": sig.page_number,
                "label": label,
                "note": note,
                "signals": sig.to_dict(),
                "image_count": sig.image_count,
            })
            entry = {"page": sig.page_number, "signals": sig.to_dict()}
            if note:
                entry["note"] = note
            if label == "MAP":
                report.map_pages.append(entry)
            elif label == "MAYBE":
                report.maybe_pages.append(entry)
            else:
                report.text_pages += 1
        if all_signals is not None:
            all_signals[pdf_path.name] = {
                "source_folder": source_folder,
                "total_pages": report.total_pages,
                "error": report.error,
                "pages": per_pdf_signals,
            }
    finally:
        pdf.close()

    return report


def reclassify_from_cache(cache: dict) -> list[PdfReport]:
    """Given a signals cache (from a prior run), rebuild PdfReports using current
    thresholds. Skips PDF I/O entirely."""
    reports = []
    for pdf_name, data in cache.items():
        r = PdfReport(path=Path(pdf_name), source_folder=data.get("source_folder", "unknown"), total_pages=data.get("total_pages", 0))
        r.error = data.get("error")
        for p in data.get("pages", []):
            sig_dict = p["signals"]
            # Rehydrate PageSignals enough to classify
            denom = max(1, sig_dict["fills"] * int(1 / max(0.001, 1 - sig_dict["text_ratio"]))) if sig_dict["text_ratio"] < 0.999 else 10_000_000
            # Simpler: rebuild a PageSignals with derived text_chars
            fill_count = sig_dict["fills"]
            text_ratio = sig_dict["text_ratio"]
            # text_chars = text_ratio * (fill_count + text_chars) → text_chars = text_ratio * fill_count / (1 - text_ratio)
            text_chars = int(text_ratio * fill_count / (1 - text_ratio)) if text_ratio < 0.999 else 1_000_000
            sig = PageSignals(
                page_number=p["page"],
                distinct_colors=sig_dict["colors"],
                fill_count=fill_count,
                total_text_chars=text_chars,
                fill_coverage=sig_dict["coverage"],
                image_count=p.get("image_count", 0),
                drawing_count=fill_count,
            )
            label, note = classify(sig)
            entry = {"page": sig.page_number, "signals": sig.to_dict()}
            if note:
                entry["note"] = note
            if label == "MAP":
                r.map_pages.append(entry)
            elif label == "MAYBE":
                r.maybe_pages.append(entry)
            else:
                r.text_pages += 1
        reports.append(r)
    return reports


# ---- Calibration against manual hints ---------------------------------------

def _manual_pages_for(manual_hints: dict, pdf_filename: str) -> set[int] | None:
    """Return set of map_pages from the existing manual hints file, or None if
    the PDF isn't in there."""
    if manual_hints is None:
        return None
    # The existing map_page_hints.yaml is a flat {filename: {map_pages: [...]}}
    # (v0.2.1 format). Try both direct key lookup and case variations.
    entry = manual_hints.get(pdf_filename)
    if entry is None:
        # Try lowercased variant (some filenames on disk differ in case)
        for k, v in manual_hints.items():
            if k.lower() == pdf_filename.lower():
                entry = v
                break
    if entry is None:
        return None
    if isinstance(entry, dict):
        if "map_pages" in entry:
            return set(entry["map_pages"])
        # Avusy-style with annex-raw numbers
        if "map_pages_annex_raw" in entry:
            return set(entry["map_pages_annex_raw"])
    return None


def calibration(reports: list[PdfReport], manual_hints: dict) -> dict:
    """Compute recall/precision per PDF where manual ground truth exists.
    Returns a dict with per-PDF stats + overall summary."""
    per_pdf = {}
    tot_tp = 0
    tot_fn = 0
    tot_fp = 0
    pdfs_with_truth = 0

    for r in reports:
        truth = _manual_pages_for(manual_hints, r.path.name)
        if truth is None:
            continue
        pdfs_with_truth += 1
        found_mapyes = {p["page"] for p in r.map_pages} | {p["page"] for p in r.maybe_pages}
        tp = len(truth & found_mapyes)
        fn = len(truth - found_mapyes)
        fp = len(found_mapyes - truth)
        recall = tp / len(truth) if truth else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        per_pdf[r.path.name] = {
            "truth_pages": len(truth),
            "found_pages": len(found_mapyes),
            "recall": round(recall, 3),
            "precision": round(precision, 3),
            "missed_pages": sorted(truth - found_mapyes),
        }
        tot_tp += tp
        tot_fn += fn
        tot_fp += fp

    overall_recall = tot_tp / (tot_tp + tot_fn) if (tot_tp + tot_fn) > 0 else 0.0
    overall_precision = tot_tp / (tot_tp + tot_fp) if (tot_tp + tot_fp) > 0 else 0.0
    return {
        "overall_recall": round(overall_recall, 3),
        "overall_precision": round(overall_precision, 3),
        "pdfs_with_truth": pdfs_with_truth,
        "per_pdf": per_pdf,
    }


# ---- YAML output ------------------------------------------------------------

def build_yaml_payload(reports: list[PdfReport], manual_hints: dict | None, calibration_stats: dict | None) -> dict:
    """Build the final YAML payload preserving manual hints under a `manual:` key."""
    pdfs_out: dict[str, dict] = {}
    for r in reports:
        entry: dict = {"source_folder": r.source_folder, "total_pages": r.total_pages}
        if r.error:
            entry["error"] = r.error
        if r.map_pages:
            entry["map"] = r.map_pages
        if r.maybe_pages:
            entry["maybe"] = r.maybe_pages
        # Preserve manual ground truth under `manual:` key
        if manual_hints:
            manual = manual_hints.get(r.path.name) or next((v for k, v in manual_hints.items() if k.lower() == r.path.name.lower()), None)
            if manual:
                entry["manual"] = manual
        pdfs_out[r.path.name] = entry

    calib_comment = ""
    if calibration_stats:
        calib_comment = (
            f"# Calibration: recall={calibration_stats['overall_recall']}, "
            f"precision={calibration_stats['overall_precision']} "
            f"over {calibration_stats['pdfs_with_truth']} labeled PDFs\n"
        )

    return {
        "meta": {
            "generated_by": "tools/classify_map_pages.py",
            "thresholds": {
                "MAP": {
                    "distinct_colors>=": MAP_MIN_DISTINCT_COLORS,
                    "fill_count>=": MAP_MIN_FILL_COUNT,
                    "text_ratio<": MAP_MAX_TEXT_RATIO,
                    "coverage>": MAP_MIN_COVERAGE,
                },
                "MAYBE": {
                    "distinct_colors>=": MAYBE_MIN_DISTINCT_COLORS,
                    "fill_count>=": MAYBE_MIN_FILL_COUNT,
                    "text_ratio<": MAYBE_MAX_TEXT_RATIO,
                },
            },
            "calibration": calibration_stats,
        },
        "pdfs": pdfs_out,
    }


# ---- Main -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pdf-dir", type=Path, default=None, help="Root PDCom/ folder (walked recursively). Required unless --from-cache.")
    ap.add_argument("--out", type=Path, required=True, help="Output YAML path.")
    ap.add_argument("--manual-hints", type=Path, default=None, help="Existing manual hints YAML (for calibration + preservation).")
    ap.add_argument("--quiet", action="store_true", help="Suppress per-page terminal output.")
    ap.add_argument("--max-seconds-per-pdf", type=float, default=120.0, help="Per-PDF processing cap.")
    ap.add_argument("--signals-cache", type=Path, default=None, help="JSON path to cache raw per-page signals for fast re-classification.")
    ap.add_argument("--from-cache", type=Path, default=None, help="Instead of scanning PDFs, load signals from this cache and reclassify.")
    args = ap.parse_args()

    manual_hints = None
    if args.manual_hints and args.manual_hints.exists():
        with args.manual_hints.open("r", encoding="utf-8") as f:
            manual_hints = yaml.safe_load(f) or {}
        if "meta" in manual_hints:
            manual_hints.pop("meta")
        print(f"[calibration] loaded {len(manual_hints)} manual hint entries from {args.manual_hints}")

    # Fast path: reclassify from signals cache
    if args.from_cache:
        with args.from_cache.open("r", encoding="utf-8") as f:
            cache = json.load(f)
        print(f"[cache] loaded signals for {len(cache)} PDFs from {args.from_cache} — skipping PDF I/O")
        reports = reclassify_from_cache(cache)
        if not args.quiet:
            for r in reports:
                print(f"{r.path.name} ({r.source_folder}, {r.total_pages} pages)")
                print(f"  → {len(r.map_pages)} map, {len(r.maybe_pages)} maybe, {r.text_pages} text")
        # continue to calibration + output
        root = Path(args.from_cache).parent
    else:
        if args.pdf_dir is None:
            print("ERROR: --pdf-dir required unless --from-cache provided", file=sys.stderr)
            sys.exit(2)
        root = args.pdf_dir.expanduser().resolve()
        if not root.exists():
            print(f"ERROR: pdf-dir not found: {root}", file=sys.stderr)
            sys.exit(2)

        pdf_files = sorted(root.rglob("*.pdf"))
        if not pdf_files:
            print(f"ERROR: no PDFs under {root}", file=sys.stderr)
            sys.exit(2)
        print(f"[scan] {len(pdf_files)} PDFs under {root}\n")

        all_signals: dict | None = {} if args.signals_cache else None
        reports: list[PdfReport] = []
        for pdf_path in pdf_files:
            report = scan_pdf(pdf_path, root, max_seconds=args.max_seconds_per_pdf, all_signals=all_signals)
            reports.append(report)
            if args.quiet:
                continue
            tag = f"{pdf_path.name} ({report.source_folder}, {report.total_pages} pages)"
            print(tag)
            if report.error:
                print(f"  ⚠ error: {report.error}")
            for entry in report.map_pages:
                s = entry["signals"]
                print(f"  Page {entry['page']:>4}: MAP   [{s['colors']:>2} colors, {s['fills']:>4} fills, text-ratio {s['text_ratio']:.2f}, coverage {s['coverage']:.2f}]")
            for entry in report.maybe_pages:
                s = entry["signals"]
                note = entry.get("note", "")
                note_str = f" ({note})" if note else ""
                print(f"  Page {entry['page']:>4}: MAYBE [{s['colors']:>2} colors, {s['fills']:>4} fills, text-ratio {s['text_ratio']:.2f}, coverage {s['coverage']:.2f}]{note_str}")
            print(f"  → {len(report.map_pages)} map, {len(report.maybe_pages)} maybe, {report.text_pages} text\n")

        # Write signals cache if requested (after scanning all PDFs)
        if args.signals_cache and all_signals is not None:
            args.signals_cache.parent.mkdir(parents=True, exist_ok=True)
            with args.signals_cache.open("w", encoding="utf-8") as f:
                json.dump(all_signals, f)
            print(f"[cache] wrote signals for {len(all_signals)} PDFs to {args.signals_cache}\n")

    # Calibration
    calibration_stats = None
    if manual_hints:
        calibration_stats = calibration(reports, manual_hints)
        print("=" * 70)
        print("CALIBRATION REPORT")
        print("=" * 70)
        print(f"PDFs with manual ground truth: {calibration_stats['pdfs_with_truth']}")
        print(f"Overall recall:    {calibration_stats['overall_recall']}")
        print(f"Overall precision: {calibration_stats['overall_precision']}")
        print()
        print(f"{'PDF':<60} {'truth':>6} {'found':>6} {'recall':>7} {'precision':>10}")
        for name, stats in sorted(calibration_stats["per_pdf"].items()):
            print(f"{name[:58]:<60} {stats['truth_pages']:>6} {stats['found_pages']:>6} {stats['recall']:>7} {stats['precision']:>10}")
            if stats["missed_pages"]:
                missed = stats["missed_pages"][:12]
                tail = " …" if len(stats["missed_pages"]) > 12 else ""
                print(f"    missed: {missed}{tail}")
        print()

    # Corpus summary
    by_folder = Counter(r.source_folder for r in reports)
    pages_by_folder: dict[str, dict] = {}
    for r in reports:
        p = pages_by_folder.setdefault(r.source_folder, {"pdfs": 0, "pages": 0, "map": 0, "maybe": 0, "text": 0})
        p["pdfs"] += 1
        p["pages"] += r.total_pages
        p["map"] += len(r.map_pages)
        p["maybe"] += len(r.maybe_pages)
        p["text"] += r.text_pages
    print("=" * 70)
    print("CORPUS SUMMARY")
    print("=" * 70)
    for folder, s in sorted(pages_by_folder.items()):
        print(f"  {folder:<12}: {s['pdfs']:>3} PDFs, {s['pages']:>5} pages, {s['map']:>4} MAP, {s['maybe']:>4} MAYBE, {s['text']:>5} TEXT")
    tot_map = sum(s["map"] for s in pages_by_folder.values())
    tot_maybe = sum(s["maybe"] for s in pages_by_folder.values())
    tot_text = sum(s["text"] for s in pages_by_folder.values())
    print(f"  {'TOTAL':<12}: {len(reports):>3} PDFs, {sum(s['pages'] for s in pages_by_folder.values()):>5} pages, "
          f"{tot_map:>4} MAP, {tot_maybe:>4} MAYBE, {tot_text:>5} TEXT")

    # Write YAML
    payload = build_yaml_payload(reports, manual_hints, calibration_stats)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, sort_keys=True, allow_unicode=True, width=120)
    print(f"\n[output] wrote {args.out}")


if __name__ == "__main__":
    main()
