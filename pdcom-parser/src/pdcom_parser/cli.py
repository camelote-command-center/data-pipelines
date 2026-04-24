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
                            features_by_page.setdefault(page_num, []).append({
                                "map_theme": next((p["map_theme"] for p in pdf_manifest["pages"] if p["page_number"] == page_num), "unknown") or "unknown",
                                "label": props.get("label"),
                                "slug": props.get("layer_slug"),
                                "color": props.get("color"),
                                "fill_type": props.get("fill_type"),
                                "geometry": feat["geometry"],
                                "confidence": next((p.get("georef_confidence") for p in pdf_manifest["pages"] if p["page_number"] == page_num), None),
                                "properties": {"page_number": page_num},
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


if __name__ == "__main__":
    main()
