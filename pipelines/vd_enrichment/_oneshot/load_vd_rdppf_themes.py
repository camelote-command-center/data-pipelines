#!/usr/bin/env python3
"""
Load the VPS-fetched VD RDPPF theme NDJSON into re-LLM bronze.

Runs LOCALLY (re-LLM session pooler is reachable here; canton-VD is not).
Discipline: UPSERT only, never TRUNCATE. Soft-delete rows not seen this run.
Geometry is wrapped in ST_MakeValid at insert (same guard the shared
vd_enrichment upsert applies) — GEOS even-odd semantics correctly rebuild
interior rings from ArcGIS's flat ring list. Verified: nested-ring
MULTIPOLYGON area 116 -> MakeValid -> 84 = outer minus hole.
"""
import json, os, sys, datetime
import psycopg2, psycopg2.extras

NDJSON = sys.argv[1]
DB = os.environ["RE_LLM_DB_URI"]
RUN_TS = datetime.datetime.now(datetime.timezone.utc)

KIND = {"vd.zone_protection_eau": "zone",
        "vd.secteur_protection_eau": "secteur",
        "vd.aire_alimentation": "aire"}


def esri_rings_to_wkt(geom):
    """ArcGIS rings -> EWKT MULTIPOLYGON(2056). Each ring becomes its own
    polygon; ST_MakeValid at insert resolves nesting into proper holes."""
    if not geom or not geom.get("rings"):
        return None
    polys = []
    for ring in geom["rings"]:
        if len(ring) < 4:
            continue
        coords = ", ".join(f"{p[0]} {p[1]}" for p in ring)
        polys.append(f"(({coords}))")
    if not polys:
        return None
    return f"SRID=2056;MULTIPOLYGON({','.join(polys)})"


def ms_to_ts(v):
    """ArcGIS epoch-millis -> timestamptz (None-safe)."""
    if v is None or v == "":
        return None
    try:
        return datetime.datetime.fromtimestamp(int(v) / 1000.0, datetime.timezone.utc)
    except (ValueError, TypeError, OSError, OverflowError):
        return None


def num(v):
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def integer(v):
    if v in (None, ""):
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


SPECS = {
    "vd.zone_reservee": {
        "table": "bronze_ch.vd_zone_reservee",
        "cols": ["raw_data", "code", "designation", "abreviation_type", "disposition_niveau",
                 "statut_juridique", "date_entree_vigueur", "date_fin", "date_enquete",
                 "date_approbation", "no_officiel", "type_doc", "titre", "commune_bfs",
                 "perimetre_m", "surface_m2", "arcgis_objectid", "geometry", "source_layer"],
        "map": lambda a, g, l: (
            psycopg2.extras.Json(a), a.get("CODE"), a.get("DESIGNATION"),
            a.get("ABREVIATION_TYPE"), a.get("DISPOSITION_NIVEAU"), a.get("STATUT_JURIDIQUE"),
            ms_to_ts(a.get("DATE_EV")), ms_to_ts(a.get("DATE_FIN")), ms_to_ts(a.get("DATE_ENQ")),
            ms_to_ts(a.get("DATE_APPRO")), num(a.get("NO_OFFICIEL")), a.get("TYPE_DOC"),
            a.get("TITRE"), integer(a.get("COMMUNE")), num(a.get("PERIMETRE")),
            num(a.get("SURFACE")), a["OBJECTID"], esri_rings_to_wkt(g), l),
    },
    "_protection": {
        "table": "bronze_ch.vd_protection_eaux",
        "cols": ["raw_data", "indice_protection", "protection_kind", "date_acceptation",
                 "commune_bfs", "arcgis_objectid", "geometry", "source_layer"],
        "map": lambda a, g, l: (
            psycopg2.extras.Json(a), a.get("INDICE_PROTECTION"), KIND[l],
            ms_to_ts(a.get("DATE_ACCEPTATION")), None,
            a["OBJECTID"], esri_rings_to_wkt(g), l),
    },
    "vd.site_pollue": {
        "table": "bronze_ch.vd_site_pollue",
        "cols": ["raw_data", "type_site", "nom_site", "activite", "nom_phase", "no_dossier",
                 "no_eva", "parcelles_polluees", "urgence_investig", "investigations_realisees",
                 "volume_decharge", "debut_activite", "fin_activite", "no_commune_vd",
                 "commune_bfs", "arcgis_objectid", "geometry", "source_layer"],
        "map": lambda a, g, l: (
            psycopg2.extras.Json(a), a.get("TYPE_SITE"), a.get("NOM_SITE"), a.get("ACTIVITE"),
            a.get("NOM_PHASE"), a.get("NO_DOSSIER"), a.get("NO_EVA"), a.get("PARCELLES_POLLUEES"),
            integer(a.get("URGENCE_INVESTIG")), a.get("INVESTIGATIONS_REALISEES"),
            integer(a.get("VOLUME_DECHARGE")), a.get("DEBUT_ACTIVITE"), a.get("FIN_ACTIVITE"),
            integer(a.get("NO_COMMUNE")), None,
            a["OBJECTID"], esri_rings_to_wkt(g), l),
    },
}


def spec_for(layer):
    if layer in KIND:
        return SPECS["_protection"]
    return SPECS[layer]


def main():
    buckets, nogeom, seen_layers = {}, {}, set()
    with open(NDJSON, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            a, g, l = rec.get("attributes"), rec.get("geometry"), rec["layer"]
            if not a:
                continue
            seen_layers.add(l)
            sp = spec_for(l)
            row = sp["map"](a, g, l)
            if row[sp["cols"].index("geometry")] is None:
                nogeom[l] = nogeom.get(l, 0) + 1
                continue          # never insert a geometry-less row (41a643b8 guard)
            buckets.setdefault(sp["table"], {"spec": sp, "rows": []})["rows"].append(row)

    conn = psycopg2.connect(DB, connect_timeout=20)
    conn.autocommit = False
    total = {}
    for table, b in buckets.items():
        sp, rows = b["spec"], b["rows"]
        cols = sp["cols"]
        tmpl = "(" + ", ".join("ST_MakeValid(%s::geometry)" if c == "geometry" else "%s"
                               for c in cols) + ", %s, %s)"
        upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols
                        if c not in ("source_layer", "arcgis_objectid"))
        sql = f"""INSERT INTO {table} ({", ".join(cols)}, last_seen_at, deleted_at)
                  VALUES %s
                  ON CONFLICT (source_layer, arcgis_objectid) DO UPDATE SET
                    {upd}, last_seen_at = EXCLUDED.last_seen_at, deleted_at = NULL"""
        ext = [(*r, RUN_TS, None) for r in rows]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, ext, template=tmpl, page_size=200)
        conn.commit()
        total[table] = len(rows)
        print(f"  upserted {len(rows):6d} -> {table}")

    # soft-delete rows not seen this run, scoped to the layers we actually fetched
    for table, b in buckets.items():
        layers = sorted({r[b["spec"]["cols"].index("source_layer")] for r in b["rows"]})
        with conn.cursor() as cur:
            cur.execute(f"""UPDATE {table} SET deleted_at = %s
                             WHERE source_layer = ANY(%s)
                               AND last_seen_at < %s AND deleted_at IS NULL""",
                        (RUN_TS, layers, RUN_TS))
            if cur.rowcount:
                print(f"  soft-deleted {cur.rowcount} stale rows in {table}")
        conn.commit()
    conn.close()
    if nogeom:
        print(f"  SKIPPED (no geometry): {nogeom}")
    print("LOADED " + json.dumps(total))


if __name__ == "__main__":
    main()
