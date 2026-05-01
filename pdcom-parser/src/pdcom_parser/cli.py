from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import click
import yaml

from . import __version__
from .db import (
    count_ref_pdcom_zones,
    count_silver_pdcom_zones,
    get_communes_geneva_lv95,
    load_commune_results,
    refresh_matviews,
    run_distribute,
)
from .ingest import build_ingest_manifest, write_manifest_yaml
from .pipeline import extract_pdf
from .report import append_log, summarize_run

REGISTRY = Path.home() / "supabase-registry" / "supabase-projects.json"


def _registry_lenient() -> dict:
    """Read supabase-projects.json tolerating missing commas between fields (known bug
    in the registry file — logged on lamap's 03_NEXT_STEPS)."""
    if not REGISTRY.exists():
        raise click.ClickException(f"Supabase registry not found at {REGISTRY}.")
    text = REGISTRY.read_text()
    # Insert missing commas before newlines that follow a string value but aren't already comma/brace terminated
    import re
    patched = re.sub(r'(\"[^\"\n]*\")\s*\n(\s*\"[a-z_]+\"\s*:)', r'\1,\n\2', text)
    return json.loads(patched)


def _db_url(project_key: str) -> str:
    env_map = {"lamap-db": "LAMAP_DB_URL", "re-llm": "RELLM_DB_URL", "camelote-data": "CAMELOTE_DB_URL"}
    env_name = env_map.get(project_key)
    if env_name and os.environ.get(env_name):
        return os.environ[env_name]
    reg = _registry_lenient()
    if project_key not in reg:
        raise click.ClickException(f"Project '{project_key}' not in supabase registry. Available: {list(reg)}")
    entry = reg[project_key]
    ref = entry["url"].split("//")[1].split(".")[0]
    pwd = entry["db_password"]
    from urllib.parse import quote_plus
    return f"postgresql://postgres:{quote_plus(pwd)}@db.{ref}.supabase.co:5432/postgres"


def _lamap_db_url() -> str:
    return _db_url("lamap-db")


def _rellm_db_url() -> str:
    return _db_url("re-llm")


@click.group()
@click.version_option(__version__)
def main():
    """Swiss PDCom map extractor → PostGIS."""


@main.command()
@click.option("--pdf-dir", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
@click.option("--output", type=click.Path(path_type=Path), default=Path("data/output"))
def ingest(pdf_dir: Path, output: Path):
    """Match PDFs in pdf_dir to commune_bfs from ref.communes."""
    communes = get_communes_geneva_lv95(_lamap_db_url())
    manifest = build_ingest_manifest(pdf_dir, communes)
    out_path = output / "ingest.yaml"
    write_manifest_yaml(manifest, out_path)
    matched_total = sum(len(v["pdfs"]) for v in manifest["matched"].values())
    append_log(output / "run_log.jsonl", {
        "kind": "ingest",
        "total": matched_total + len(manifest["needs_review"]) + len(manifest["unmatched"]),
        "matched": matched_total,
        "needs_review": len(manifest["needs_review"]),
        "unmatched": len(manifest["unmatched"]),
        "communes_matched": len(manifest["matched"]),
    })
    click.echo(f"Matched {matched_total} PDFs across {len(manifest['matched'])} communes. "
               f"{len(manifest['needs_review'])} need review, {len(manifest['unmatched'])} unmatched.")
    click.echo(f"Manifest: {out_path}")


@main.command("export-boundaries")
@click.option("--output", type=click.Path(path_type=Path), default=Path("data/boundaries"))
def export_boundaries(output: Path):
    """Export ref.communes geometry (LV95) as per-commune GeoJSON files."""
    output.mkdir(parents=True, exist_ok=True)
    communes = get_communes_geneva_lv95(_lamap_db_url())
    for c in communes:
        p = output / f"{c['commune_bfs']}.geojson"
        with p.open("w") as f:
            json.dump({
                "type": "Feature",
                "properties": {"commune_bfs": c["commune_bfs"], "commune_name": c["commune_name"]},
                "geometry": c["boundary_lv95_geojson"],
                "crs": {"type": "name", "properties": {"name": "EPSG:2056"}},
            }, f)
    click.echo(f"Exported {len(communes)} commune boundaries (LV95) → {output}")


def _load_ingest_manifest(output: Path) -> dict:
    p = output / "ingest.yaml"
    if not p.exists():
        raise click.ClickException(f"Run `pdcom ingest` first ({p} missing).")
    with p.open("r") as f:
        return yaml.safe_load(f)


def _extract_one_commune(commune_bfs: int, manifest: dict, output: Path, boundaries: Path, log_path: Path, min_confidence: float) -> list[dict]:
    entry = manifest["matched"].get(commune_bfs) or manifest["matched"].get(str(commune_bfs))
    if not entry:
        raise click.ClickException(f"Commune {commune_bfs} not in ingest manifest.")
    commune_name = entry["name"]
    boundary_path = boundaries / f"{commune_bfs}.geojson"
    if not boundary_path.exists():
        raise click.ClickException(f"Boundary not found for {commune_bfs} at {boundary_path}. Run `pdcom export-boundaries` first.")
    with boundary_path.open("r") as f:
        boundary_feature = json.load(f)
    boundary_geom = boundary_feature["geometry"]

    commune_dir = output / f"{commune_bfs}_{commune_name.lower().replace(' ', '_').replace('é','e').replace('è','e')}"
    results = []
    for pdf_rec in entry["pdfs"]:
        pdf_path = Path(pdf_rec["path"])
        pdf_stem = pdf_path.stem.replace("/", "_")
        out_dir = commune_dir / pdf_stem
        result = extract_pdf(
            pdf_path=pdf_path,
            commune_bfs=commune_bfs,
            commune_name=commune_name,
            boundary_lv95_geojson=boundary_geom,
            output_dir=out_dir,
            log_path=log_path,
            min_confidence=min_confidence,
        )
        pdf_rec_full = {
            "path": str(pdf_path),
            "filename": pdf_path.name,
            "sha256": pdf_rec["sha256"],
            "size_bytes": pdf_rec["size_bytes"],
            "page_count": result["manifest"]["pdf_page_count"],
            "manifest": result["manifest"],
        }
        result["pdf_rec"] = pdf_rec_full
        result["commune_name"] = commune_name
        result["out_dir"] = str(out_dir)
        results.append(result)
    return results


@main.command()
@click.option("--commune-bfs", type=int, required=True)
@click.option("--output", type=click.Path(path_type=Path), default=Path("data/output"))
@click.option("--boundaries", type=click.Path(path_type=Path), default=Path("data/boundaries"))
@click.option("--min-confidence", type=float, default=0.5)
def extract(commune_bfs: int, output: Path, boundaries: Path, min_confidence: float):
    """Extract all map pages for one commune."""
    manifest = _load_ingest_manifest(output)
    log_path = output / "run_log.jsonl"
    _extract_one_commune(commune_bfs, manifest, output, boundaries, log_path, min_confidence)
    click.echo(f"Extraction done for commune_bfs={commune_bfs}")


@main.command("extract-all")
@click.option("--output", type=click.Path(path_type=Path), default=Path("data/output"))
@click.option("--boundaries", type=click.Path(path_type=Path), default=Path("data/boundaries"))
@click.option("--min-confidence", type=float, default=0.5)
def extract_all(output: Path, boundaries: Path, min_confidence: float):
    """Extract all matched communes."""
    manifest = _load_ingest_manifest(output)
    log_path = output / "run_log.jsonl"
    ok, fail = 0, 0
    for bfs_key in list(manifest["matched"].keys()):
        bfs = int(bfs_key)
        try:
            _extract_one_commune(bfs, manifest, output, boundaries, log_path, min_confidence)
            ok += 1
        except Exception as e:
            append_log(log_path, {"kind": "commune", "commune_bfs": bfs, "status": "failed", "error": str(e)})
            click.echo(f"  ✗ {bfs}: {e}", err=True)
            fail += 1
    click.echo(f"extract-all done: {ok} ok, {fail} failed")


@main.command()
@click.option("--output", type=click.Path(path_type=Path), default=Path("data/output"))
def load(output: Path):
    """Load extracted results from data/output/ into re-LLM bronze_ch.pdcom_*."""
    manifest = _load_ingest_manifest(output)
    db_url = _rellm_db_url()
    log_path = output / "run_log.jsonl"
    total = {"sources": 0, "pages": 0, "features": 0}
    for bfs_key, entry in manifest["matched"].items():
        bfs = int(bfs_key)
        commune_name = entry["name"]
        commune_dir_glob = list(output.glob(f"{bfs}_*"))
        if not commune_dir_glob:
            continue
        commune_dir = commune_dir_glob[0]
        for pdf_rec in entry["pdfs"]:
            pdf_path = Path(pdf_rec["path"])
            out_sub = commune_dir / pdf_path.stem.replace("/", "_")
            manifest_file = out_sub / "manifest.json"
            if not manifest_file.exists():
                continue
            with manifest_file.open("r") as f:
                pdf_manifest = json.load(f)
            # Rebuild pages rec list (for DB insert) from manifest
            pages_for_db = [
                {
                    "page_number": p["page_number"],
                    "page_type": p["page_type"],
                    "map_theme": p.get("map_theme"),
                    "map_title": p.get("map_title"),
                    "legend_json": p.get("legend_json"),
                    "drawing_count": p.get("drawing_count"),
                    "has_raster": p.get("has_raster"),
                    "georef_confidence": p.get("georef_confidence"),
                    "extraction_status": p.get("extraction_status"),
                }
                for p in pdf_manifest.get("pages", [])
            ]
            # Reload feature geojsons from disk
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
                            # Preserve per-feature properties (page_conf, clip_ratio, original_area_m2,
                            # clipped_area_m2 etc.) that pipeline.py emitted.
                            feat_conf = props.get("confidence")
                            if feat_conf is None:
                                feat_conf = next((p.get("georef_confidence") for p in pdf_manifest["pages"] if p["page_number"] == page_num), None)
                            # Everything except the per-feature identity fields stays in properties jsonb
                            db_props = {k: v for k, v in props.items()
                                        if k not in ("layer_slug", "label", "color", "fill_type", "confidence")}
                            db_props["page_number"] = page_num
                            features_by_page.setdefault(page_num, []).append({
                                "map_theme": next((p["map_theme"] for p in pdf_manifest["pages"] if p["page_number"] == page_num), "unknown") or "unknown",
                                "label": props.get("label"),
                                "slug": props.get("layer_slug"),
                                "color": props.get("color"),
                                "fill_type": props.get("fill_type"),
                                "geometry": feat["geometry"],
                                "confidence": feat_conf,
                                "properties": db_props,
                            })

            rec = {
                "path": str(pdf_path),
                "filename": pdf_path.name,
                "sha256": pdf_rec["sha256"],
                "size_bytes": pdf_rec["size_bytes"],
                "page_count": pdf_manifest.get("pdf_page_count"),
                "manifest": pdf_manifest,
            }
            counts = load_commune_results(db_url, bfs, commune_name, rec, pages_for_db, features_by_page)
            for k in total:
                total[k] += counts[k]
            click.echo(f"  ✓ {bfs} {commune_name}: {counts}")
            append_log(log_path, {"kind": "db_load", "commune_bfs": bfs, **counts})
    click.echo(f"Loaded into re-LLM bronze_ch: {total}")
    click.echo("Refreshing silver_ch.pdcom_zones + gold_ch.pdcom_zones …")
    refresh_matviews(db_url)
    click.echo("Refresh done.")


@main.command()
def distribute():
    """Invoke run_sync('weekly') on re-LLM to push gold_ch.pdcom_zones → ref.pdcom_zones."""
    url = _rellm_db_url()
    rows = run_distribute(url)
    for source, target, mode, synced in rows:
        flag = "✓" if synced >= 0 else "✗"
        click.echo(f"  {flag} {source} → {target} [{mode}]: {synced}")


@main.command()
@click.option("--output", type=click.Path(path_type=Path), default=Path("data/output"))
def report(output: Path):
    """Print the run summary and DB counts."""
    ge_count = 46
    try:
        with __import__("psycopg").connect(_lamap_db_url()) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ref.communes WHERE canton_code='GE'")
            ge_count = cur.fetchone()[0]
    except Exception:
        pass
    db_counts = {}
    try:
        db_counts["silver_ch.pdcom_zones (re-LLM)"] = count_silver_pdcom_zones(_rellm_db_url())
    except Exception as e:
        db_counts["silver_ch.pdcom_zones (re-LLM)"] = f"ERR: {e}"
    try:
        db_counts["ref.pdcom_zones (lamap_db)"] = count_ref_pdcom_zones(_lamap_db_url())
    except Exception as e:
        db_counts["ref.pdcom_zones (lamap_db)"] = f"ERR: {e}"
    click.echo(summarize_run(output, ge_count, db_counts))


@main.command("run-all")
@click.option("--pdf-dir", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
@click.option("--output", type=click.Path(path_type=Path), default=Path("data/output"))
@click.option("--boundaries", type=click.Path(path_type=Path), default=Path("data/boundaries"))
@click.option("--min-confidence", type=float, default=0.5)
@click.pass_context
def run_all(ctx, pdf_dir: Path, output: Path, boundaries: Path, min_confidence: float):
    """Full pipeline: ingest → export-boundaries → extract-all → load → distribute → report."""
    ctx.invoke(ingest, pdf_dir=pdf_dir, output=output)
    ctx.invoke(export_boundaries, output=boundaries)
    ctx.invoke(extract_all, output=output, boundaries=boundaries, min_confidence=min_confidence)
    ctx.invoke(load, output=output)
    ctx.invoke(distribute)
    ctx.invoke(report, output=output)


@main.command()
@click.option("--commune-bfs", type=int, required=True)
def discover_cmd(commune_bfs: int):
    """Fallback: search online for a PDCom PDF for a commune."""
    from .discover import discover_pdcom_pdf
    with __import__("psycopg").connect(_lamap_db_url()) as conn, conn.cursor() as cur:
        cur.execute("SELECT commune_name FROM ref.communes WHERE commune_bfs = %s", (commune_bfs,))
        row = cur.fetchone()
        if not row:
            raise click.ClickException(f"No commune with bfs={commune_bfs}")
        name = row[0]
    urls = discover_pdcom_pdf(name)
    for u in urls:
        click.echo(u)


# Re-expose discover under the documented name
main.add_command(discover_cmd, name="discover")


# ─── v0.5 / urbanisation focused-extraction track ────────────────────────────

@main.group("urbanisation")
def urbanisation_grp():
    """v0.5 focused urbanisation track: extract zoning polygons from carte-de-synthèse
    PDFs into 6 canonical categories. re-LLM only — does not touch lamap_db.
    """


def _default_pdf_roots() -> list[Path]:
    base = Path.home() / "Desktop" / "Lamap Reshape" / "PDCom"
    return [base, base / "new_ones"]


def _default_qa_dir() -> Path:
    return Path.home() / "Desktop" / "Lamap Reshape" / "PDCom" / "qa_urbanisation"


@urbanisation_grp.command("discover")
@click.option("--pdf-root", type=click.Path(exists=True, file_okay=False, path_type=Path),
              multiple=True, help="One or more directories to scan (default: ~/Desktop/Lamap Reshape/PDCom + new_ones)")
@click.option("--page-count-threshold", type=int, default=8)
def urbanisation_discover(pdf_root, page_count_threshold):
    """Phase 1 — list candidate PDFs (filename pattern OR ≤ N pages). Read-only."""
    from .urbanisation_extractor import discover_candidate_pdfs
    roots = list(pdf_root) if pdf_root else _default_pdf_roots()
    rows = discover_candidate_pdfs(*roots, page_count_threshold=page_count_threshold)
    click.echo(f"# Candidate PDFs: {len(rows)} found across {len(roots)} root(s)")
    click.echo(f"\n| commune | filename | pages | size_bytes | match_reason |")
    click.echo(  "|---------|----------|------:|-----------:|--------------|")
    for r in rows:
        click.echo(f"| {r['commune_slug']} | {r['filename']} | {r['page_count']} | {r['size_bytes']} | {r['match_reason']} |")


def _build_commune_lookup(lamap_url: str) -> dict[str, dict]:
    """Fetch GE communes once; index by lowered name AND simplified slug for filename matching."""
    communes = get_communes_geneva_lv95(lamap_url)
    out: dict[str, dict] = {}
    for c in communes:
        # multiple keys for the same commune
        name = c["commune_name"]
        keys = {name.lower(), name.lower().replace("(ge)", "").strip()}
        # slug-style: lowercase, dashes / underscores, accent-stripped
        import unicodedata
        slug = unicodedata.normalize("NFD", name.lower())
        slug = "".join(ch for ch in slug if unicodedata.category(ch) != "Mn")
        slug = slug.replace(" ", "-").replace("'", "-").replace("(ge)", "").strip("-")
        keys.add(slug)
        # also without dashes
        keys.add(slug.replace("-", ""))
        for k in keys:
            if k:
                out[k] = c
    return out


def _resolve_commune_for_pdf(pdf_filename: str, communes_by_key: dict, lamap_url: str) -> dict | None:
    """Use ingest.match_pdf_to_commune for fuzzy filename → commune resolution."""
    from .ingest import match_pdf_to_commune
    communes_list = get_communes_geneva_lv95(lamap_url)
    # match_pdf_to_commune wants a Path
    m = match_pdf_to_commune(Path(pdf_filename), communes_list)
    if m.commune_bfs is None:
        return None
    for c in communes_list:
        if c["commune_bfs"] == m.commune_bfs:
            return c
    return None


@urbanisation_grp.command("calibrate")
@click.option("--pdf",
              default=str(Path.home() / "Desktop" / "Lamap Reshape" / "PDCom" / "pdcom_bernex_strategie_d_evolution_de_la_zone_5_carte_de_synthese.pdf"),
              type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--legend-mode", type=click.Choice(["anchored", "cascade"]), default="anchored")
@click.option("--qa-dir", type=click.Path(path_type=Path), default=None)
def urbanisation_calibrate(pdf: Path, legend_mode: str, qa_dir: Path | None):
    """Phase 2 — run on a single reference PDF (default: Bernex). Writes nothing
    to DB. Generates a QA PNG and prints disposition per legend entry. Exits non-zero
    if calibration thresholds are not met."""
    from .urbanisation_extractor import extract_urbanisation_pdf
    from .qa import render_qa_image
    from shapely.geometry import shape

    qa_dir = qa_dir or _default_qa_dir()
    qa_dir.mkdir(parents=True, exist_ok=True)

    lamap_url = _lamap_db_url()
    communes_by_key = _build_commune_lookup(lamap_url)
    commune = _resolve_commune_for_pdf(pdf.name, communes_by_key, lamap_url)
    if commune is None:
        raise click.ClickException(f"Could not resolve commune for {pdf.name}")
    click.echo(f"Calibration PDF: {pdf.name}")
    click.echo(f"Resolved commune: {commune['commune_name']} (bfs={commune['commune_bfs']})")
    click.echo(f"Legend mode: {legend_mode}")

    rpt = extract_urbanisation_pdf(
        pdf_path=pdf,
        commune_bfs=commune["commune_bfs"],
        commune_name=commune["commune_name"],
        boundary_lv95_geojson=commune["boundary_lv95_geojson"],
        legend_mode=legend_mode,
    )

    click.echo(f"\nPages processed: {rpt.pages_processed}, with legend: {rpt.pages_with_legend}")
    click.echo(f"Legend entries: {rpt.legend_entries_total} total, {rpt.matched_entries} matched, {len(rpt.unmatched_labels)} unmatched")
    if rpt.unmatched_labels:
        click.echo("Unmatched labels (Tier 3 reject):")
        for u in rpt.unmatched_labels[:20]:
            click.echo(f"  - {u!r}")
    click.echo(f"Page georef confidences: {rpt.page_confidences}")

    # Per-category counts
    by_cat: dict[str, list] = {}
    for f in rpt.features:
        by_cat.setdefault(f.category_key, []).append(f)
    click.echo(f"\nFeatures by category ({len(rpt.features)} total):")
    for k, fs in sorted(by_cat.items()):
        confs = [f.confidence for f in fs]
        mean_c = sum(confs) / len(confs) if confs else 0
        click.echo(f"  {k}: {len(fs)} features, mean conf {mean_c:.3f}")

    # QA PNG
    qa_path = qa_dir / f"{commune['commune_bfs']}_{pdf.stem}_calibration.png"
    boundary = shape(commune["boundary_lv95_geojson"])
    qa_features = [{"geom": f.geometry, "color": f.source_color,
                    "label": f.category_label, "fill_type": "solid"} for f in rpt.features]
    render_qa_image(boundary, qa_features, qa_path,
                    title=f"{commune['commune_name']} — {pdf.name} (calibration)")
    click.echo(f"\nQA PNG: {qa_path}")

    # Calibration thresholds (per plan):
    #   ≥4 of 6 canonical categories matched at Tier 1
    #   mean per-feature conf ≥ 0.85
    #   ≥1 polygon per matched category
    tier1_categories = {f.category_key for f in rpt.features if f.label_match_tier == 1}
    all_confs = [f.confidence for f in rpt.features]
    mean_conf = sum(all_confs) / len(all_confs) if all_confs else 0.0
    click.echo(f"\nCalibration thresholds:")
    click.echo(f"  Tier 1 categories matched: {len(tier1_categories)}/6 (need ≥4) — {sorted(tier1_categories)}")
    click.echo(f"  Mean per-feature confidence: {mean_conf:.3f} (need ≥0.85)")
    click.echo(f"  Categories with ≥1 polygon: {len(by_cat)}/6")

    failed = []
    if len(tier1_categories) < 4:
        failed.append(f"only {len(tier1_categories)} Tier-1 categories matched")
    if mean_conf < 0.85:
        failed.append(f"mean conf {mean_conf:.3f} below 0.85")
    if len(by_cat) < 1:
        failed.append("no categories produced any polygons")

    if failed:
        click.echo(f"\n❌ CALIBRATION FAILED: {'; '.join(failed)}")
        sys.exit(2)
    click.echo("\n✓ Calibration passed.")


@urbanisation_grp.command("run")
@click.option("--pdf-root", type=click.Path(exists=True, file_okay=False, path_type=Path),
              multiple=True)
@click.option("--page-count-threshold", type=int, default=8)
@click.option("--legend-mode", type=click.Choice(["anchored", "cascade"]), default="anchored")
@click.option("--qa-dir", type=click.Path(path_type=Path), default=None)
@click.option("--skip-calibration-gate", is_flag=True, default=False,
              help="Skip the Bernex calibration pre-flight check (use only for debugging).")
def urbanisation_run(pdf_root, page_count_threshold, legend_mode, qa_dir, skip_calibration_gate):
    """Phase 5 — full corpus run. Calibrate-on-Bernex first; abort if it fails."""
    import psycopg
    from .urbanisation_extractor import (
        discover_candidate_pdfs,
        extract_urbanisation_pdf,
        post_batch_dedup,
        write_urbanisation_features,
    )
    from .ingest import sha256_of
    from .qa import render_qa_image
    from shapely.geometry import shape

    qa_dir = qa_dir or _default_qa_dir()
    qa_dir.mkdir(parents=True, exist_ok=True)
    roots = list(pdf_root) if pdf_root else _default_pdf_roots()

    lamap_url = _lamap_db_url()
    rellm_url = _rellm_db_url()
    communes_by_key = _build_commune_lookup(lamap_url)
    communes_list = get_communes_geneva_lv95(lamap_url)

    # Calibration pre-flight (unless skipped)
    if not skip_calibration_gate:
        click.echo("[gate] running calibration on Bernex …")
        bernex_pdf = Path.home() / "Desktop" / "Lamap Reshape" / "PDCom" / "pdcom_bernex_strategie_d_evolution_de_la_zone_5_carte_de_synthese.pdf"
        if not bernex_pdf.exists():
            raise click.ClickException(f"Bernex calibration PDF missing: {bernex_pdf}")
        bernex_commune = next((c for c in communes_list if "bernex" in c["commune_name"].lower()), None)
        if bernex_commune is None:
            raise click.ClickException("Bernex commune not found in ref.communes (canton_code='GE')")
        rpt = extract_urbanisation_pdf(
            pdf_path=bernex_pdf, commune_bfs=bernex_commune["commune_bfs"],
            commune_name=bernex_commune["commune_name"],
            boundary_lv95_geojson=bernex_commune["boundary_lv95_geojson"],
            legend_mode=legend_mode,
        )
        tier1_categories = {f.category_key for f in rpt.features if f.label_match_tier == 1}
        all_confs = [f.confidence for f in rpt.features]
        mean_conf = sum(all_confs) / len(all_confs) if all_confs else 0.0
        if len(tier1_categories) < 4 or mean_conf < 0.85 or not rpt.features:
            click.echo(f"[gate] Tier-1 categories: {len(tier1_categories)}/6, mean conf: {mean_conf:.3f}")
            raise click.ClickException(
                "Calibration FAILED — abort. Run `pdcom urbanisation calibrate` for details."
            )
        click.echo(f"[gate] ✓ Bernex calibration passed (Tier-1 cats={len(tier1_categories)}, mean conf={mean_conf:.3f}, features={len(rpt.features)})")

    candidates = discover_candidate_pdfs(*roots, page_count_threshold=page_count_threshold)
    click.echo(f"[run] {len(candidates)} candidate PDFs across {len(roots)} root(s)")

    by_commune_features: dict[int, list] = {}
    by_commune_meta: dict[int, dict] = {}
    summary: list[dict] = []

    with psycopg.connect(rellm_url) as conn:
        for cand in candidates:
            pdf_path: Path = cand["path"]
            commune = _resolve_commune_for_pdf(pdf_path.name, communes_by_key, lamap_url)
            if commune is None:
                click.echo(f"  ! {pdf_path.name}: cannot resolve commune — skipped")
                summary.append({"pdf": pdf_path.name, "status": "no_commune", "features": 0})
                continue
            try:
                rpt = extract_urbanisation_pdf(
                    pdf_path=pdf_path, commune_bfs=commune["commune_bfs"],
                    commune_name=commune["commune_name"],
                    boundary_lv95_geojson=commune["boundary_lv95_geojson"],
                    legend_mode=legend_mode,
                )
            except Exception as e:
                click.echo(f"  ✗ {pdf_path.name}: extract raised {e}")
                summary.append({"pdf": pdf_path.name, "status": "raised", "error": str(e), "features": 0})
                continue

            sha = sha256_of(pdf_path)
            for f in rpt.features:
                f.pdf_sha256 = sha
            n_written = write_urbanisation_features(conn, rpt.features, sha)
            click.echo(f"  {'✓' if n_written else '·'} {commune['commune_name']:<22} {pdf_path.name}: "
                       f"{len(rpt.features)} feats, {len(rpt.unmatched_labels)} unmatched, "
                       f"matched_categories={sorted({f.category_key for f in rpt.features})}")
            summary.append({
                "pdf": pdf_path.name, "commune_bfs": commune["commune_bfs"],
                "commune_name": commune["commune_name"], "status": "ok",
                "features": n_written, "unmatched": len(rpt.unmatched_labels),
            })

            by_commune_features.setdefault(commune["commune_bfs"], []).extend(rpt.features)
            by_commune_meta[commune["commune_bfs"]] = commune

        # Cross-PDF dedup
        click.echo("[run] post-batch dedup …")
        n_dedup = post_batch_dedup(conn)
        click.echo(f"[run] dedup removed {n_dedup} rows")

    # Per-commune QA PNGs (after dedup)
    click.echo("[run] generating per-commune QA PNGs …")
    for bfs, feats in by_commune_features.items():
        if not feats:
            continue
        commune = by_commune_meta[bfs]
        boundary = shape(commune["boundary_lv95_geojson"])
        slug = commune["commune_name"].lower().replace(" ", "_").replace("(", "").replace(")", "")
        qa_features = [{"geom": f.geometry, "color": f.source_color,
                        "label": f.category_label, "fill_type": "solid"} for f in feats]
        qa_path = qa_dir / f"{bfs}_{slug}.png"
        try:
            render_qa_image(boundary, qa_features, qa_path,
                            title=f"{commune['commune_name']} — urbanisation v0.5 ({len(feats)} feats)")
        except Exception as e:
            click.echo(f"  ! QA PNG failed for {commune['commune_name']}: {e}")

    # Final batch summary
    click.echo("\n[run] batch summary:")
    ok = [s for s in summary if s["status"] == "ok"]
    click.echo(f"  PDFs OK: {len(ok)}/{len(summary)}")
    click.echo(f"  total features written: {sum(s['features'] for s in summary)}")
    by_status: dict[str, int] = {}
    for s in summary:
        by_status[s["status"]] = by_status.get(s["status"], 0) + 1
    click.echo(f"  status breakdown: {by_status}")
    click.echo(f"  QA PNGs: {qa_dir}")


if __name__ == "__main__":
    main()
