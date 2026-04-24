# pdcom-parser

Extracts vector zone geometry from Swiss **Plan directeur communal (PDCom)** PDFs into PostGIS. Output feeds `ref.pdcom_zones` on lamap_db via the existing re-LLM → consumer distribution pipeline.

## How it works

1. **Ingest** — scan `PDCom/` folder, fuzzy-match filenames to `ref.communes` → `commune_bfs`.
2. **Classify** — per page: map / text / cover / toc (drawing_count + text heuristics).
3. **Legend** — auto-detect per page: colored swatches paired to labels, classified as solid/stroke/hatch.
4. **Extract** — color-filter drawings against each legend swatch, flip Y, clip to map bbox, dedupe via unary_union.
5. **Hatch** — recover hatched zones via PDF clip paths (preferred) or DBSCAN concave-hull fallback.
6. **Georef** — detect commune-boundary polygon in PDF (widest stroke), fit scale+translate to `ref.communes` boundary (LV95), apply affine transform.
7. **Normalize** — slugify labels, classify theme from title.
8. **Export** — GeoJSON per layer + per-commune manifest. All in LV95 (SRID 2056).
9. **Load** — write to `bronze_ch.pdcom_sources / pages / features` on re-LLM.
10. **Distribute** — `gold_ch.pdcom_zones` matview (WGS84) registered in `gold_ch.sync_registry`, pushed to `ref.pdcom_zones` on lamap_db via existing `run_sync()` cron.

## CLI

```bash
pdcom run-all --pdf-dir ./PDCom/
pdcom ingest --pdf-dir ./PDCom/
pdcom export-boundaries --db-url $LAMAP_DB_URL
pdcom extract --commune-bfs 6628
pdcom extract-all
pdcom load --db-url $RELLM_DB_URL
pdcom distribute --db-url $LAMAP_DB_URL
pdcom report
pdcom discover --commune-bfs 6627    # fallback only
```

## Architectural invariants

- `commune_bfs` everywhere (matches lamap_db ref.* convention). Never `ofs_code` or `commune_ofs`.
- LV95 (SRID 2056) in re-LLM bronze/silver. WGS84 (SRID 4326) at distribution boundary.
- PostgREST does NOT expose `ref` on lamap_db — public frontend function reads `ref.*` internally.
- Bronze/silver writes stay in re-LLM. Lamap_db gets `ref.pdcom_zones` + `public.get_pdcom_zones_for_plot` + one row in `platform.standards`.
- Distribution rides the existing `gold_ch.sync_registry` + `run_sync()` + FDW mechanism — no new plumbing.

## Layout

```
src/pdcom_parser/    # module code
configs/             # theme_keywords.yaml, label_slugs.yaml
sql/                 # 001–004 migrations
tests/               # fixture-driven tests (Chêne-Bougeries p42/53/64)
data/                # pdfs/, boundaries/, output/ (gitignored)
scripts/             # bootstrap_fixtures.py
```

## Environment

Credentials pulled from `~/supabase-registry/supabase-projects.json`:
- `znrvddgmczdqoucmykij` — re-LLM
- `fckdwddgtdbvhzloejni` — lamap_db
- `dxugbpeacnorjunpljih` — camelote_data (incident logging)
