"""v0.2.1 corpus rollout — extract all matched communes, run per-commune §5
acceptance gating, distribute only passing communes to lamap_db.ref.pdcom_zones.

Usage:
    LAMAP_DB_URL=... RELLM_DB_URL=... python scripts/run_corpus.py [--pdf-dir PATH]

Produces:
    data/output/run_summary.json   — per-commune diagnostic
    data/output/{bfs}_*/qa.png     — commune QA PNG (generated during extract)
    docs/qa_{slug}.png             — copied for PR commit
"""
from __future__ import annotations

import json
import os
import signal
import sys
import shutil
import subprocess
import unicodedata
from pathlib import Path

import yaml
import psycopg


PDF_TIMEOUT_SEC = 300  # 5 min per PDF — long enough for 300-page docs, short enough to make progress


class _Timeout(Exception):
    pass


def _timeout_handler(signum, frame):
    raise _Timeout("PDF extraction exceeded timeout")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pdcom_parser.ingest import build_ingest_manifest, write_manifest_yaml  # noqa: E402
from pdcom_parser.db import get_communes_geneva_lv95, load_commune_results, connect  # noqa: E402
from pdcom_parser.pipeline import extract_pdf  # noqa: E402
from pdcom_parser import __version__  # noqa: E402


def _slug(name: str) -> str:
    n = unicodedata.normalize("NFD", name.lower())
    n = "".join(c for c in n if unicodedata.category(c) != "Mn")
    import re
    n = re.sub(r"[^a-z0-9]+", "_", n).strip("_")
    return n


def run_extraction(pdf_dir: Path, output_dir: Path, boundaries_dir: Path, lamap_url: str) -> dict:
    # Ingest
    communes = get_communes_geneva_lv95(lamap_url)
    manifest = build_ingest_manifest(pdf_dir, communes)
    write_manifest_yaml(manifest, output_dir / "ingest.yaml")
    print(f"[ingest] {sum(len(v['pdfs']) for v in manifest['matched'].values())} PDFs across {len(manifest['matched'])} communes; "
          f"{len(manifest['needs_review'])} review, {len(manifest['unmatched'])} unmatched, "
          f"{len(manifest.get('canton_atlas', []))} canton atlases")

    # Export boundaries
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    for c in communes:
        p = boundaries_dir / f"{c['commune_bfs']}.geojson"
        with p.open("w") as f:
            json.dump({"type": "Feature", "properties": {"commune_bfs": c["commune_bfs"], "commune_name": c["commune_name"]}, "geometry": c["boundary_lv95_geojson"]}, f)

    # Per-commune extraction
    log_path = output_dir / "run_log.jsonl"
    if log_path.exists():
        log_path.unlink()
    results: dict[int, dict] = {}
    for bfs_key, entry in manifest["matched"].items():
        bfs = int(bfs_key)
        commune_name = entry["commune_name"]
        slug = _slug(commune_name)
        commune_dir = output_dir / f"{bfs}_{slug}"
        boundary_path = boundaries_dir / f"{bfs}.geojson"
        with boundary_path.open("r") as f:
            boundary = json.load(f)["geometry"]
        per_commune_info = {
            "commune_bfs": bfs,
            "commune_name": commune_name,
            "slug": slug,
            "pdfs_processed": 0,
            "pages_total": 0,
            "pages_map": 0,
            "pages_ok": 0,
            "pages_legend_failed": 0,
            "pages_low_confidence": 0,
            "features_extracted": 0,
            "themes_found": set(),
            "template_guess": "unknown",
            "notes": [],
            "pdf_manifests": [],
        }
        for pdf_rec in entry["pdfs"]:
            pdf_path = Path(pdf_rec["path"])
            print(f"[extract] {bfs} {commune_name}: {pdf_path.name}", flush=True)
            out_sub = commune_dir / pdf_path.stem.replace("/", "_")
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(PDF_TIMEOUT_SEC)
            try:
                r = extract_pdf(
                    pdf_path=pdf_path,
                    commune_bfs=bfs,
                    commune_name=commune_name,
                    boundary_lv95_geojson=boundary,
                    output_dir=out_sub,
                    log_path=log_path,
                    min_confidence=0.5,
                )
                signal.alarm(0)
            except _Timeout:
                signal.alarm(0)
                print(f"  ⏱ TIMEOUT after {PDF_TIMEOUT_SEC}s — skipping", flush=True)
                per_commune_info["notes"].append(f"timeout on {pdf_path.name} — likely unary_union O(n²) on basemap-polluted layer")
                continue
            except Exception as e:
                signal.alarm(0)
                print(f"  ✗ FAILED: {e}", flush=True)
                per_commune_info["notes"].append(f"extract raised: {e}")
                continue
            m = r["manifest"]
            per_commune_info["pdfs_processed"] += 1
            per_commune_info["pages_total"] += m["pdf_page_count"]
            per_commune_info["pages_map"] += m["pages_map"]
            per_commune_info["pages_ok"] += m["pages_ok"]
            per_commune_info["pages_legend_failed"] += m.get("pages_legend_failed", 0)
            per_commune_info["pages_low_confidence"] += m.get("pages_low_confidence", 0)
            per_commune_info["features_extracted"] += m["features_total"]
            for t in m.get("themes_found", []):
                per_commune_info["themes_found"].add(t)
            if per_commune_info["template_guess"] == "unknown":
                per_commune_info["template_guess"] = m.get("template_guess", "unknown")
            per_commune_info["pdf_manifests"].append(m["pdf_filename"])
        per_commune_info["themes_found"] = sorted(per_commune_info["themes_found"])
        if per_commune_info["pages_legend_failed"] > 0:
            per_commune_info["notes"].append(
                f"{per_commune_info['pages_legend_failed']} pages with <3 legend entries — typically sub-area detail insets"
            )
        if per_commune_info["pages_ok"] == 0 and per_commune_info["pages_map"] > 0:
            per_commune_info["notes"].append("All map pages failed extraction — likely alternate legend template (v0.3 work).")
        results[bfs] = per_commune_info
    return {"manifest": manifest, "per_commune": results}


def load_to_bronze(output_dir: Path, rellm_url: str, per_commune: dict[int, dict]) -> None:
    """Load extraction results from output_dir into re-LLM bronze_ch. Per-commune."""
    for bfs, info in per_commune.items():
        if info["pdfs_processed"] == 0:
            continue
        commune_dir = output_dir / f"{bfs}_{info['slug']}"
        # Find ingest entries for this commune
        ingest = yaml.safe_load((output_dir / "ingest.yaml").open("r"))
        entry = ingest["matched"].get(bfs, {})
        for pdf_rec in entry.get("pdfs", []):
            pdf_path = Path(pdf_rec["path"])
            out_sub = commune_dir / pdf_path.stem.replace("/", "_")
            manifest_file = out_sub / "manifest.json"
            if not manifest_file.exists():
                continue
            with manifest_file.open("r") as f:
                pdf_manifest = json.load(f)
            pages_for_db = [
                {
                    "page_number": p["page_number"], "page_type": p["page_type"],
                    "map_theme": p.get("map_theme"), "map_title": p.get("map_title"),
                    "legend_json": p.get("legend_json"), "drawing_count": p.get("drawing_count"),
                    "has_raster": p.get("has_raster"),
                    "georef_confidence": p.get("georef_confidence"),
                    "extraction_status": p.get("extraction_status"),
                }
                for p in pdf_manifest.get("pages", [])
            ]
            features_by_page: dict[int, list] = {}
            pages_dir = out_sub / "pages"
            if pages_dir.exists():
                for page_dir in pages_dir.iterdir():
                    try:
                        page_num = int(page_dir.name.split("_")[0][1:])
                    except Exception:
                        continue
                    for gj in page_dir.glob("*.geojson"):
                        with gj.open("r") as f:
                            fc = json.load(f)
                        for feat in fc.get("features", []):
                            props = feat.get("properties", {})
                            feat_conf = props.get("confidence")
                            if feat_conf is None:
                                feat_conf = next((p.get("georef_confidence") for p in pdf_manifest["pages"] if p["page_number"] == page_num), None)
                            db_props = {k: v for k, v in props.items()
                                        if k not in ("layer_slug", "label", "color", "fill_type", "confidence")}
                            db_props["page_number"] = page_num
                            features_by_page.setdefault(page_num, []).append({
                                "map_theme": next((p["map_theme"] for p in pdf_manifest["pages"] if p["page_number"] == page_num), "unknown") or "unknown",
                                "label": props.get("label"), "slug": props.get("layer_slug"),
                                "color": props.get("color"), "fill_type": props.get("fill_type"),
                                "geometry": feat["geometry"],
                                "confidence": feat_conf,
                                "properties": db_props,
                            })
            rec = {
                "path": str(pdf_path), "filename": pdf_path.name,
                "sha256": pdf_rec["sha256"], "size_bytes": pdf_rec["size_bytes"],
                "page_count": pdf_manifest.get("pdf_page_count"),
                "manifest": pdf_manifest,
            }
            c = load_commune_results(rellm_url, bfs, info["commune_name"], rec, pages_for_db, features_by_page)
            print(f"  ✓ loaded bfs={bfs} {pdf_path.name}: {c}")


def refresh_matviews(rellm_url: str) -> None:
    with connect(rellm_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout=0")
        cur.execute("REFRESH MATERIALIZED VIEW silver_ch.pdcom_zones")
        cur.execute("REFRESH MATERIALIZED VIEW gold_ch.pdcom_zones")
        conn.commit()


def per_commune_quality_gate(rellm_url: str, lamap_url: str, per_commune: dict[int, dict]) -> dict[int, dict]:
    """Run §5 checks per commune against silver_ch.pdcom_zones.
    Returns per_commune with added 'quality_status' field ('pass' or 'blocked') and 'quality_checks' details."""
    # Fetch commune areas once from lamap_db
    commune_area_m2: dict[int, float] = {}
    with psycopg.connect(lamap_url) as conn, conn.cursor() as cur:
        for bfs in per_commune:
            cur.execute(
                "SELECT commune_name, ST_Area(geometry::geography) FROM ref.communes WHERE commune_bfs = %s", (bfs,)
            )
            row = cur.fetchone()
            if row:
                commune_area_m2[bfs] = row[1]

    # Run checks per commune in re-LLM silver
    with connect(rellm_url) as conn, conn.cursor() as cur:
        for bfs, info in per_commune.items():
            if info["features_extracted"] == 0:
                info["quality_status"] = "blocked"
                info["quality_checks"] = {"reason": "no features extracted"}
                continue
            checks: dict = {}
            # Use LV95 metric area (silver is SRID 2056)
            cur.execute(
                "SELECT count(*) FROM silver_ch.pdcom_zones WHERE commune_bfs = %s", (bfs,)
            )
            silver_count = cur.fetchone()[0]
            checks["silver_count"] = silver_count
            if silver_count == 0:
                info["quality_status"] = "blocked"
                info["quality_checks"] = {**checks, "reason": "0 features pass 0.8 silver gate"}
                continue

            # A1 max area > 30% of commune
            area_m2 = commune_area_m2.get(bfs, 0)
            cur.execute(
                """
                SELECT count(*) FROM silver_ch.pdcom_zones
                WHERE commune_bfs = %s AND ST_Area(geometry) > %s
                """, (bfs, area_m2 * 0.30)
            )
            checks["A1_over_30pct"] = cur.fetchone()[0]

            # A2 min area < 200 m² (non-stroke)
            cur.execute(
                """
                SELECT count(*) FROM silver_ch.pdcom_zones
                WHERE commune_bfs = %s AND ST_Area(geometry) < 200 AND fill_type != 'stroke_only'
                """, (bfs,)
            )
            checks["A2_under_200m2"] = cur.fetchone()[0]

            # A3 paragraph-text labels
            cur.execute(
                r"""
                SELECT count(*) FROM silver_ch.pdcom_zones
                WHERE commune_bfs = %s
                  AND layer_label ~ '(\. [A-Z])|(:$)|(^>)|(^[-–]\s)|(^\d+(\.\d+)?\s*[-–]\s*\d+)'
                """, (bfs,)
            )
            checks["A3_paragraph_labels"] = cur.fetchone()[0]

            # A4 unknown theme %
            cur.execute(
                """
                SELECT round(100.0 * count(*) FILTER (WHERE map_theme='unknown') / count(*)::numeric, 1)
                FROM silver_ch.pdcom_zones WHERE commune_bfs = %s
                """, (bfs,)
            )
            checks["A4_unknown_pct"] = float(cur.fetchone()[0] or 0)

            # C distinct confidence
            cur.execute(
                """
                SELECT count(DISTINCT georef_confidence) FROM silver_ch.pdcom_zones
                WHERE commune_bfs = %s
                """, (bfs,)
            )
            checks["C_distinct_confs"] = cur.fetchone()[0]

            # Decide pass/blocked
            blocked_reasons = []
            if checks["A1_over_30pct"] > 0:
                blocked_reasons.append(f"A1: {checks['A1_over_30pct']} features > 30% of commune")
            if checks["A2_under_200m2"] > 0:
                blocked_reasons.append(f"A2: {checks['A2_under_200m2']} tiny polygons")
            if checks["A3_paragraph_labels"] > 0:
                blocked_reasons.append(f"A3: {checks['A3_paragraph_labels']} paragraph labels")
            if checks["A4_unknown_pct"] >= 10:
                blocked_reasons.append(f"A4: unknown_pct={checks['A4_unknown_pct']}")
            # Note: §5C 'distinct_confs > 1 per theme' is a soft criterion; not a blocker here since
            # single-page themes inherently have limited conf variance. Log but don't block.
            if blocked_reasons:
                info["quality_status"] = "blocked"
                info["quality_checks"] = {**checks, "reason": "; ".join(blocked_reasons)}
            else:
                info["quality_status"] = "pass"
                info["quality_checks"] = checks
    return per_commune


def drop_blocked_from_bronze(rellm_url: str, per_commune: dict[int, dict]) -> None:
    """Remove all bronze features/pages/sources for quality_blocked communes so the
    gold matview (and therefore ref.pdcom_zones) only reflects passing communes."""
    blocked = [bfs for bfs, info in per_commune.items() if info.get("quality_status") == "blocked"]
    if not blocked:
        return
    print(f"[gate] dropping bronze data for {len(blocked)} quality-blocked communes: {blocked}")
    with connect(rellm_url) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM bronze_ch.pdcom_features WHERE commune_bfs = ANY(%s)", (blocked,))
        cur.execute(
            "DELETE FROM bronze_ch.pdcom_pages WHERE source_id IN (SELECT id FROM bronze_ch.pdcom_sources WHERE commune_bfs = ANY(%s))", (blocked,)
        )
        cur.execute("DELETE FROM bronze_ch.pdcom_sources WHERE commune_bfs = ANY(%s)", (blocked,))
        conn.commit()


def distribute(rellm_url: str) -> int:
    with connect(rellm_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT gold_ch.sync_full_refresh('gold_ch','pdcom_zones','pdcom_zones','lamap_db_server','lamap_db_foreign','lamap_db')"
        )
        (n,) = cur.fetchone()
        conn.commit()
    return n


def copy_qa_pngs(output_dir: Path, docs_dir: Path, per_commune: dict[int, dict]) -> None:
    docs_dir.mkdir(parents=True, exist_ok=True)
    for bfs, info in per_commune.items():
        if info.get("quality_status") != "pass":
            continue
        cdir = output_dir / f"{bfs}_{info['slug']}"
        # first PDF subdir qa.png
        for sub in sorted(cdir.iterdir()):
            qa = sub / "qa.png"
            if qa.exists():
                dst = docs_dir / f"qa_{info['slug']}.png"
                shutil.copy(qa, dst)
                break


def write_summary(out_path: Path, per_commune: dict[int, dict], manifest: dict) -> None:
    flat = []
    for bfs, info in sorted(per_commune.items()):
        flat.append({k: v for k, v in info.items() if k != "pdf_manifests"})
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        json.dump({
            "parser_version": __version__,
            "communes": flat,
            "ingest": {
                "matched_commune_count": len(manifest["matched"]),
                "unmatched": manifest.get("unmatched", []),
                "needs_review": manifest.get("needs_review", []),
                "canton_atlas": manifest.get("canton_atlas", []),
            },
        }, f, indent=2, default=str)


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf-dir", type=Path, default=Path("/Users/a/Desktop/Lamap Reshape/PDCom"))
    ap.add_argument("--output", type=Path, default=ROOT / "data/output")
    ap.add_argument("--boundaries", type=Path, default=ROOT / "data/boundaries")
    ap.add_argument("--docs", type=Path, default=ROOT / "docs")
    args = ap.parse_args()

    lamap_url = os.environ["LAMAP_DB_URL"]
    rellm_url = os.environ["RELLM_DB_URL"]

    # 1. Extract everything
    res = run_extraction(args.pdf_dir, args.output, args.boundaries, lamap_url)
    per_commune = res["per_commune"]

    # 2. Truncate + load to bronze (all matched communes)
    with connect(rellm_url) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE bronze_ch.pdcom_features, bronze_ch.pdcom_pages, bronze_ch.pdcom_sources CASCADE")
        conn.commit()
    print("[db] bronze truncated")
    load_to_bronze(args.output, rellm_url, per_commune)

    # 3. Refresh matviews
    refresh_matviews(rellm_url)
    print("[db] silver+gold refreshed")

    # 4. Per-commune §5 gating
    per_commune = per_commune_quality_gate(rellm_url, lamap_url, per_commune)
    for bfs, info in per_commune.items():
        print(f"[gate] {bfs} {info['commune_name']}: {info.get('quality_status')} — {info.get('quality_checks', {}).get('reason','')}")

    # 5. Drop blocked communes from bronze, refresh, distribute
    drop_blocked_from_bronze(rellm_url, per_commune)
    refresh_matviews(rellm_url)
    n = distribute(rellm_url)
    print(f"[distribute] pushed {n} rows to lamap_db.ref.pdcom_zones")

    # 6. Copy QA PNGs of passing communes to docs/
    copy_qa_pngs(args.output, args.docs, per_commune)

    # 7. Write summary
    write_summary(args.output / "run_summary.json", per_commune, res["manifest"])
    print(f"[summary] {args.output / 'run_summary.json'}")


if __name__ == "__main__":
    main()
