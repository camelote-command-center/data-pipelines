from __future__ import annotations

import json
from pathlib import Path


def read_run_log(path: Path) -> list[dict]:
    out = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def summarize_run(output_dir: Path, ref_communes_ge: int, db_counts: dict | None = None) -> str:
    run_log = read_run_log(output_dir / "run_log.jsonl")

    pdfs_total = 0
    pdfs_matched = 0
    pdfs_unmatched = 0
    pdfs_review = 0
    communes_attempted: set[int] = set()
    communes_fully_ok: set[int] = set()
    communes_partial: set[int] = set()
    pages_map_ok = 0
    pages_low_conf = 0
    pages_legend_failed = 0
    pages_nonmap = 0
    features_total = 0

    for rec in run_log:
        kind = rec.get("kind")
        if kind == "ingest":
            pdfs_total = rec.get("total", pdfs_total)
            pdfs_matched = rec.get("matched", pdfs_matched)
            pdfs_unmatched = rec.get("unmatched", pdfs_unmatched)
            pdfs_review = rec.get("needs_review", pdfs_review)
        elif kind == "commune":
            bfs = rec.get("commune_bfs")
            if bfs is None:
                continue
            communes_attempted.add(bfs)
            if rec.get("status") == "ok":
                communes_fully_ok.add(bfs)
            elif rec.get("status") == "partial":
                communes_partial.add(bfs)
        elif kind == "page":
            if rec.get("status") == "ok":
                pages_map_ok += 1
            elif rec.get("status") == "low_confidence":
                pages_low_conf += 1
            elif rec.get("status") == "legend_failed":
                pages_legend_failed += 1
            elif rec.get("status") == "nonmap":
                pages_nonmap += 1
            features_total += rec.get("feature_count", 0)

    lines = []
    lines.append("=== PDCom Parser Run Summary ===")
    lines.append(f"PDFs ingested: {pdfs_total}")
    lines.append(f"  ✓ Matched: {pdfs_matched}")
    lines.append(f"  ? Needs review: {pdfs_review}")
    lines.append(f"  ✗ Unmatched: {pdfs_unmatched}")
    lines.append("")
    lines.append(f"Geneva communes in ref.communes: {ref_communes_ge}")
    lines.append(f"  ✓ Fully extracted: {len(communes_fully_ok)}")
    lines.append(f"  ⚠ Partial: {len(communes_partial)}")
    missing = ref_communes_ge - len(communes_attempted)
    lines.append(f"  ✗ No PDF in folder: {missing}")
    lines.append("")
    lines.append(f"Map pages processed:")
    lines.append(f"  ✓ Extracted cleanly: {pages_map_ok}")
    lines.append(f"  ⚠ Low confidence (<0.8): {pages_low_conf}")
    lines.append(f"  ✗ Legend failed: {pages_legend_failed}")
    lines.append(f"  ⊘ Non-map pages: {pages_nonmap}")
    lines.append("")
    lines.append(f"Features extracted: {features_total}")
    if db_counts is not None:
        lines.append("")
        lines.append(f"Database state:")
        for k, v in db_counts.items():
            lines.append(f"  {k}: {v}")
    lines.append("")
    lines.append(f"Output: {output_dir}")
    return "\n".join(lines)


def append_log(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str, ensure_ascii=False))
        f.write("\n")
