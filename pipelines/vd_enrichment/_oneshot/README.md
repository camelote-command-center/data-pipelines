# _oneshot — VD RDPPF theme acquisition (2026-07-15)

One-shot acquisition for the three new VD RDPPF themes. **Not on cron** — the VD parsers have never
been deployed (see below), so this ran as a targeted one-off.

## Why not geodienste (read this before "improving" the source)

`geodienste.ch` bulk data is **`Freigabe erforderlich` for VD** on all three MGDM topics —
`kataster_belasteter_standorte`, `planerischer_gewaesserschutz`, **and** `npl_nutzungsplanung`.
VD is 1 of only **3 gated cantons (NW/OW/VD) of 27**; the other 23 are `Frei erhältlich`.

Proven 2026-07-15, not read off the catalog:

| probe | VD | control |
|---|---|---|
| INTERLIS `.../VD/..._lv95.zip` | **401** | ZH → **200**, 4.3 MB |
| WFS GetFeature (identical query) | `numberReturned="0"` | ZH → `numberReturned="3"` |

⚠️ **The WFS failure mode is a silent empty — 200 OK, zero features, no exception.** A parser pointed at
geodienste for VD ingests 0 rows and reports *success*. If you ever do route VD through geodienste,
assert a non-zero feature count or you will reproduce bug `41a643b8`.

`publication_wms` IS `Frei erhältlich` for VD — only *bulk data* is gated, not tiles.

## Source of record: canton-VD ArcGIS

`https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer` — the same route all 17 existing
`vd_*` bronze tables use (they carry `arcgis_objectid` / `source_layer`; VD zoning never came from
geodienste either).

⚠️ **Reachable ONLY from the VPS hosts.** All `*.vd.ch` cantonal infra (`145.232.192.0/24`) refuses
TCP:443 and ICMP from laptops and GitHub Actions. That is *why* all 17 VD datasets carry `host=vps-*`
and `workflow_file IS NULL`. (`map.lausanne.ch`, 51.89.93.7, is a different network and IS reachable.)
The `ogc.vd.ch` host that appears in some briefs is both wrong and unreachable.

⚠️ Service pins **`maxRecordCount=1`** — one feature per request; `resultRecordCount` is ignored.
Use `returnIdsOnly=true` (all objectIds in ONE request) and drive by explicit `OBJECTID=n`. That also
yields the authoritative parity count and avoids `resultOffset` drift if the layer mutates mid-run.
8,003 features ≈ 1 min at 6 workers.

| theme | layer | features |
|---|---|---|
| Zones réservées | `/35 vd.zone_reservee` | 1,765 |
| Eaux souterraines — zones S1/S2/S3 | `/118 vd.zone_protection_eau` | 2,947 |
| Eaux souterraines — secteurs Au/Ao/üB | `/119 vd.secteur_protection_eau` | 799 |
| Eaux souterraines — aires Zu | `/120 vd.aire_alimentation` | 15 |
| Sites pollués (KbS) | `/116 vd.site_pollue` | 2,477 |

## How to run

Two-step by design: **the VPS fetches, your machine writes.** This keeps `SUPABASE_DB_URI` off the VPS
entirely (the designed `/opt/lamap/.env` does not exist — `deploy.sh` has never been run).

```bash
# 1. fetch on a VPS (no secrets on the box)
scp fetch_vd_rdppf_themes.py root@31.97.122.135:/tmp/
ssh root@31.97.122.135 'nohup python3 /tmp/fetch_vd_rdppf_themes.py /tmp/vd_themes.ndjson > /tmp/fetch.log 2>&1 &'
scp root@31.97.122.135:/tmp/vd_themes.ndjson .

# 2. load locally (re-LLM session pooler is reachable here; canton-VD is not)
export RE_LLM_DB_URI="$(jq -r '.["re-llm"].session_pooler_uri' ~/supabase-registry/supabase-projects.json)"
python3 load_vd_rdppf_themes.py vd_themes.ndjson
```

UPSERT only, never TRUNCATE. Rows not seen in a run are soft-deleted (`deleted_at`). Re-running is
idempotent. After reloading bronze, **rebuild `silver_ch._protection_eaux_vd_sub` before
`link_plot_protection_eaux_vd`** — the tiling is derived, not automatic.

## Gotchas worth keeping

- **Geometry is stored native 2056**, matching 17/17 existing VD bronze tables and the source's own
  `wkid:2056`. Do NOT "normalise" bronze to 4326: that forces a `2056→4326→2056` round-trip for the
  spatial join, which is the exact pathology that *created* an invalid geometry in bug `4d930c20`.
- **`esri_to_wkt` maps every ring to its own polygon** — looks like it destroys interior holes, but it
  does not: the `ST_MakeValid` wrap rebuilds them via GEOS even-odd semantics (verified: nested-ring
  MULTIPOLYGON area 116 → 84 = outer minus hole; `vd_zone_affectation` holds 3,022 polygons with holes).
  MakeValid is load-bearing here — don't remove it and don't "fix" `esri_to_wkt`.
- **`/116 NO_COMMUNE` is NOT the federal BFS** (0/2,477 in 5000-5999; 2,473 < 1000) — canton numbering.
  Stored as `no_commune_vd`; `commune_bfs` left NULL. `/35 COMMUNE` *is* genuine BFS.
- **`link_plot_protection_eaux_vd` needs `ST_Subdivide`.** The naive join ran the full 1800s
  statement_timeout and aborted: the `secteur` polygons are canton-scale, so their GIST bbox prunes
  nothing. Tiling to 256 vertices + SUM re-aggregation `GROUP BY (egrid, id)` took the whole migration
  to 2m22s. The SUM is required for correctness — one plot can hit several tiles of the same polygon.
