"""
Shared engine for the federal cadastral parcels parsers (T1/T2/T3).

The three tier-specific entry points (pipelines/federal_cadastral_parcels_t{1,2,3}/import.py)
are thin wrappers that hardcode their canton list and call run_tier(...).

All real logic — bbox tiling, GeoAdmin identify, RPC upsert, run telemetry,
soft-delete, camelote freshness — lives here.

Source: https://api3.geo.admin.ch/rest/services/ech/MapServer/identify
        layer ch.kantone.cadastralwebmap-farbe (returns Polygon GeoJSON, EPSG:2056)
        Limits: max 50 features per call, ~20 req/min/IP fair-use.

Target: re-LLM bronze_ch.federal_cadastral_parcels via
        public.upsert_federal_cadastral_parcels_batch(p_rows JSONB) RETURNS (i,u,unc).
        Conflict key: (canton_code, ident_dn, parcel_number).

Soft-delete strategy: after successful canton sweep, mark rows whose
last_seen_at < run_started_at AND canton_code = current AND deleted_at IS NULL
as deleted (sets deleted_at = now()). Rows reappearing on a later run get
deleted_at = NULL via the RPC's UPDATE clause.

Environment:
    RE_LLM_SUPABASE_URL              re-LLM REST URL (https://<ref>.supabase.co)
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY service_role key
    CAMELOTE_SUPABASE_URL            camelote-data REST URL (for dataset freshness)
    CAMELOTE_SUPABASE_KEY            camelote-data service key
    GEOADMIN_RATE_LIMIT              optional, default 20 (requests/min/IP)
"""

import argparse
import hashlib
import json
import math
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import requests


# ──────────────────────────────────────────────────────────────
# Canton bboxes (EPSG:2056) — generous outer envelopes per canton
# ──────────────────────────────────────────────────────────────
# Coords: (min_e, min_n, max_e, max_n) in CH1903+ / LV95.
# These are conservative — slight overlap into neighbouring cantons is fine
# because the GeoAdmin identify layer pre-filters by `ak` (canton code) so
# only the requested canton's plots come back. We use ak=<canton> as a query
# parameter; the bbox just defines the spatial sweep region.

CANTON_BBOX: dict[str, tuple[int, int, int, int]] = {
    "VD": (2497000, 1117000, 2585000, 1203000),
    "NE": (2540000, 1188000, 2580000, 1227000),
    "JU": (2562000, 1227000, 2603000, 1264000),
    "FR": (2548000, 1145000, 2595000, 1200000),
    "VS": (2570000, 1080000, 2680000, 1147000),
    "BE": (2572000, 1136000, 2670000, 1232000),
    "ZH": (2670000, 1225000, 2725000, 1283000),
    "BS": (2607000, 1264000, 2622000, 1276000),
    "BL": (2607000, 1245000, 2640000, 1275000),
    "TI": (2680000, 1080000, 2740000, 1170000),
    "AG": (2640000, 1230000, 2680000, 1275000),
    "LU": (2640000, 1195000, 2680000, 1240000),
    "SG": (2715000, 1218000, 2767000, 1280000),
    "GR": (2727000, 1130000, 2843000, 1218000),
    "TG": (2710000, 1265000, 2747000, 1283000),
    "SZ": (2685000, 1190000, 2725000, 1233000),
    "SH": (2680000, 1278000, 2710000, 1295000),
    "AR": (2740000, 1245000, 2760000, 1262000),
    "GL": (2715000, 1190000, 2745000, 1225000),
    "ZG": (2675000, 1218000, 2696000, 1235000),
    "UR": (2685000, 1167000, 2718000, 1206000),
    "OW": (2655000, 1175000, 2680000, 1198000),
    "NW": (2670000, 1190000, 2693000, 1207000),
    "AI": (2748000, 1238000, 2760000, 1252000),
    "GE": (2485000, 1110000, 2510000, 1135000),  # for completeness; not used in parsers
    "SO": (2600000, 1215000, 2640000, 1245000),
}


# ──────────────────────────────────────────────────────────────
# GeoAdmin client (rate-limited)
# ──────────────────────────────────────────────────────────────

GEOADMIN_URL = "https://api3.geo.admin.ch/rest/services/ech/MapServer/identify"
GEOADMIN_LAYER = "all:ch.kantone.cadastralwebmap-farbe"
FEATURE_CAP = 50  # API hard cap per call


@dataclass
class RateLimiter:
    """Simple token-bucket: at most `rpm` requests per rolling 60s window."""
    rpm: int
    _stamps: list[float] = field(default_factory=list)

    def wait(self) -> None:
        now = time.time()
        # Drop stamps older than 60s
        self._stamps = [t for t in self._stamps if now - t < 60.0]
        if len(self._stamps) >= self.rpm:
            sleep = 60.0 - (now - self._stamps[0]) + 0.1
            if sleep > 0:
                time.sleep(sleep)
            now = time.time()
            self._stamps = [t for t in self._stamps if now - t < 60.0]
        self._stamps.append(time.time())


def fetch_tile(bbox: tuple[int, int, int, int], canton_code: str, limiter: RateLimiter,
               session: requests.Session, max_retries: int = 5) -> list[dict]:
    """Hit /identify for one tile; retry on 429/5xx; return GeoJSON features list."""
    min_e, min_n, max_e, max_n = bbox
    params = {
        "geometry": f"{min_e},{min_n},{max_e},{max_n}",
        "geometryType": "esriGeometryEnvelope",
        "geometryFormat": "geojson",
        "imageDisplay": "1024,768,96",
        "mapExtent": f"{min_e},{min_n},{max_e},{max_n}",
        "tolerance": "0",
        "layers": GEOADMIN_LAYER,
        "returnGeometry": "true",
        "sr": "2056",
    }
    backoff = 2.0
    for attempt in range(max_retries):
        limiter.wait()
        try:
            r = session.get(GEOADMIN_URL, params=params, timeout=60)
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff); backoff *= 2; continue
        if r.status_code == 200:
            data = r.json()
            results = data.get("results", []) or []
            # Pre-filter by canton; layer is national.
            return [f for f in results if (f.get("properties") or {}).get("ak") == canton_code]
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff); backoff *= 2; continue
        r.raise_for_status()
    return []


# ──────────────────────────────────────────────────────────────
# Adaptive bbox tiler
# ──────────────────────────────────────────────────────────────

@dataclass
class TileStats:
    tiles_done: int = 0
    tiles_failed: int = 0
    api_calls: int = 0
    api_errors: int = 0
    features_seen: int = 0
    max_recursion_depth: int = 0


@dataclass
class ResumeContext:
    """Tile-level resume cache. Always records completed tiles; only skips when enabled."""
    enabled: bool
    url: str
    key: str
    fresh_within_hours: int = 168
    _done_cache: set = field(default_factory=set)

    def preload(self, canton_code: str) -> None:
        if not self.enabled:
            return
        cutoff = datetime.now(timezone.utc).timestamp() - self.fresh_within_hours * 3600
        cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
        from urllib.parse import quote
        # PostgREST has a server-side max-rows cap (1000 here). Paginate with
        # offset+limit query params to walk past it.
        page_size = 1000
        offset = 0
        while True:
            h = {"apikey": self.key, "Authorization": f"Bearer {self.key}",
                 "Accept-Profile": "bronze_ch"}
            path = (f"/federal_cadastral_parcels_tile_progress?canton_code=eq.{canton_code}"
                    f"&completed_at=gte.{quote(cutoff_iso, safe='')}"
                    f"&select=e_min,n_min,e_max,n_max"
                    f"&offset={offset}&limit={page_size}"
                    f"&order=e_min.asc,n_min.asc")
            r = requests.get(f"{self.url}/rest/v1{path}", headers=h, timeout=60)
            r.raise_for_status()
            rows = r.json()
            for row in rows:
                self._done_cache.add((canton_code, row['e_min'], row['n_min'], row['e_max'], row['n_max']))
            if len(rows) < page_size:
                break
            offset += page_size

    def is_done(self, canton_code: str, bbox: tuple) -> bool:
        if not self.enabled:
            return False
        return (canton_code, *bbox) in self._done_cache

    def mark_done(self, canton_code: str, bbox: tuple, features_count: int, run_id: str) -> None:
        body = [{
            "canton_code": canton_code,
            "e_min": bbox[0], "n_min": bbox[1], "e_max": bbox[2], "n_max": bbox[3],
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id, "features": features_count,
        }]
        h = {"apikey": self.key, "Authorization": f"Bearer {self.key}",
             "Content-Type": "application/json", "Accept-Profile": "bronze_ch",
             "Content-Profile": "bronze_ch", "Prefer": "resolution=merge-duplicates,return=minimal"}
        try:
            requests.post(f"{self.url}/rest/v1/federal_cadastral_parcels_tile_progress",
                          headers=h, data=json.dumps(body), timeout=30).raise_for_status()
        except Exception as e:
            # Don't fail the run on a tile-progress write failure; just log.
            print(f"[warn] tile_progress write failed for {bbox}: {e}", flush=True)


def walk_canton(bbox: tuple[int, int, int, int], canton_code: str,
                limiter: RateLimiter, session: requests.Session,
                initial_tile_m: int = 2000, min_tile_m: int = 50,
                stats: TileStats | None = None,
                on_features=None,
                resume: ResumeContext | None = None,
                run_id: str | None = None,
                progress_cb=None,
                _depth: int = 0) -> None:
    """
    Recursive bbox split. Initial 2km tiles; quadtree-split any tile that
    returns ≥ FEATURE_CAP features (likely truncated). Stops at min_tile_m
    (50m) — deeper than that means the API is genuinely refusing or the
    plot density is impossible (no urban tile in CH should hit this).

    `on_features` is called once per non-truncated tile with the features
    list (callable; lets the caller stream into the upsert without holding
    everything in memory).
    """
    stats = stats or TileStats()
    stats.max_recursion_depth = max(stats.max_recursion_depth, _depth)
    min_e, min_n, max_e, max_n = bbox

    # Generate initial tile grid only at depth 0
    if _depth == 0:
        for e0 in range(min_e, max_e, initial_tile_m):
            for n0 in range(min_n, max_n, initial_tile_m):
                tile = (e0, n0, min(e0 + initial_tile_m, max_e), min(n0 + initial_tile_m, max_n))
                walk_canton(tile, canton_code, limiter, session,
                            initial_tile_m, min_tile_m, stats, on_features,
                            resume=resume, run_id=run_id, progress_cb=progress_cb, _depth=1)
                if progress_cb:
                    progress_cb(stats)
        return

    # Resume short-circuit: this exact bbox already completed within the freshness window.
    if resume and resume.is_done(canton_code, bbox):
        stats.tiles_done += 1
        return

    # At leaf depth, fetch this tile.
    try:
        feats = fetch_tile(bbox, canton_code, limiter, session)
        stats.api_calls += 1
    except Exception:
        stats.api_errors += 1
        stats.tiles_failed += 1
        return

    if len(feats) >= FEATURE_CAP:
        # Truncated — split. Refuse to split below min_tile_m.
        side_e = max_e - min_e; side_n = max_n - min_n
        if max(side_e, side_n) <= min_tile_m:
            stats.tiles_done += 1
            stats.features_seen += len(feats)
            if on_features: on_features(feats)
            if resume and run_id:
                resume.mark_done(canton_code, bbox, len(feats), run_id)
            return
        prev_failed = stats.tiles_failed
        mid_e = (min_e + max_e) // 2
        mid_n = (min_n + max_n) // 2
        for sub in [(min_e, min_n, mid_e, mid_n),
                    (mid_e, min_n, max_e, mid_n),
                    (min_e, mid_n, mid_e, max_n),
                    (mid_e, mid_n, max_e, max_n)]:
            walk_canton(sub, canton_code, limiter, session,
                        initial_tile_m, min_tile_m, stats, on_features,
                        resume=resume, run_id=run_id, progress_cb=progress_cb,
                        _depth=_depth + 1)
        # Mark this internal node done only if every descendant completed.
        if resume and run_id and stats.tiles_failed == prev_failed:
            resume.mark_done(canton_code, bbox, len(feats), run_id)
    else:
        stats.tiles_done += 1
        stats.features_seen += len(feats)
        if on_features: on_features(feats)
        if resume and run_id:
            resume.mark_done(canton_code, bbox, len(feats), run_id)


# ──────────────────────────────────────────────────────────────
# Feature → row transform
# ──────────────────────────────────────────────────────────────

def feature_to_row(feature: dict) -> dict | None:
    """GeoAdmin feature → RPC row payload. Returns None if essential fields missing."""
    props = feature.get("properties") or {}
    geom = feature.get("geometry")
    if not geom or not props.get("ak") or not props.get("identnd") or not props.get("number"):
        return None

    canton = props["ak"]
    ident = props["identnd"]
    parcel = str(props["number"])

    # commune_bfs is set NULL here by design — federal `identnd` is not standardized
    # across cantons (e.g. NE encodes a sub-canton code in positions 2-6, not the
    # commune). Correct commune_bfs is derived post-insert via spatial join against
    # bronze_ch.federal_communes.bfs_nr (see backfill_commune_bfs).
    commune_bfs = None

    # surface_m2: shoelace on EPSG:2056 polygon (metres → m²).
    surface_m2 = _polygon_area_m2(geom)

    # completeness: existing rows use 'complet'; preserve that idiom.
    completeness = "complet"
    rt = props.get("realestate_type")
    if rt is not None and rt != 0:
        completeness = f"complet_rt{rt}"

    return {
        "canton_code": canton,
        "commune_bfs": commune_bfs,
        "ident_dn": ident,
        "parcel_number": parcel,
        "egrid": props.get("egris_egrid"),
        "surface_m2": surface_m2,
        "completeness": completeness,
        "geometry_geojson": json.dumps(geom),
        "source_url": "api3.geo.admin.ch/identify",
    }


def _polygon_area_m2(geom: dict) -> int:
    """Shoelace on EPSG:2056 (metres) → integer m²; supports Polygon/MultiPolygon."""
    def ring_area(ring):
        s = 0.0
        for i in range(len(ring) - 1):
            x0, y0 = ring[i][0], ring[i][1]
            x1, y1 = ring[i + 1][0], ring[i + 1][1]
            s += x0 * y1 - x1 * y0
        return abs(s) / 2.0
    coords = geom.get("coordinates", [])
    t = geom.get("type")
    a = 0.0
    if t == "Polygon":
        for i, ring in enumerate(coords):
            a += ring_area(ring) * (1 if i == 0 else -1)
    elif t == "MultiPolygon":
        for poly in coords:
            for i, ring in enumerate(poly):
                a += ring_area(ring) * (1 if i == 0 else -1)
    return max(0, int(round(a)))


# ──────────────────────────────────────────────────────────────
# RPC client (re-LLM)
# ──────────────────────────────────────────────────────────────

class CadastralUpsertClient:
    def __init__(self, url: str, key: str):
        self.endpoint = f"{url}/rest/v1/rpc/upsert_federal_cadastral_parcels_batch"
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    def upsert(self, rows: list[dict]) -> dict:
        if not rows:
            return {"inserted": 0, "updated": 0, "unchanged": 0}
        backoff = 5.0
        last_err = None
        for attempt in range(6):  # ~5 + 10 + 20 + 40 + 80 = 155 sec total
            try:
                r = requests.post(self.endpoint, headers=self.headers,
                                  data=json.dumps({"p_rows": rows}), timeout=300)
                r.raise_for_status()
                body = r.json()
                if isinstance(body, list):
                    body = body[0]
                return body
            except (requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                last_err = e
                print(f"[warn] upsert transient error attempt {attempt+1}/6: "
                      f"{type(e).__name__}; retrying in {backoff:.0f}s", flush=True)
                time.sleep(backoff)
                backoff *= 2
            except requests.exceptions.HTTPError as e:
                # 5xx are also transient; 4xx are real bugs.
                if e.response is not None and 500 <= e.response.status_code < 600:
                    last_err = e
                    print(f"[warn] upsert {e.response.status_code} attempt {attempt+1}/6; "
                          f"retrying in {backoff:.0f}s", flush=True)
                    time.sleep(backoff); backoff *= 2
                    continue
                raise
        raise last_err if last_err else RuntimeError("upsert failed after retries")


# ──────────────────────────────────────────────────────────────
# Runs telemetry + soft-delete + freshness
# ──────────────────────────────────────────────────────────────

def _rest(url: str, key: str, method: str, path: str, **kw) -> dict | list | None:
    """REST helper with same retry profile as the upsert client — survives
    Supabase 522 outages, SSL drops, transient ConnectionResetError, etc."""
    h = {"apikey": key, "Authorization": f"Bearer {key}",
         "Content-Type": "application/json", "Accept-Profile": "bronze_ch",
         "Content-Profile": "bronze_ch"}
    h.update(kw.pop("headers", {}))
    full_url = f"{url}/rest/v1{path}"
    backoff = 5.0
    last_err = None
    for attempt in range(6):
        try:
            r = requests.request(method, full_url, headers=h, **kw)
            r.raise_for_status()
            if r.text:
                try: return r.json()
                except Exception: return None
            return None
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_err = e
            print(f"[warn] _rest {method} transient attempt {attempt+1}/6: "
                  f"{type(e).__name__}; retrying in {backoff:.0f}s", flush=True)
            time.sleep(backoff); backoff *= 2
        except requests.exceptions.HTTPError as e:
            if e.response is not None and 500 <= e.response.status_code < 600:
                last_err = e
                print(f"[warn] _rest {method} {e.response.status_code} attempt {attempt+1}/6; "
                      f"retrying in {backoff:.0f}s", flush=True)
                time.sleep(backoff); backoff *= 2
                continue
            raise
    raise last_err if last_err else RuntimeError("_rest failed after retries")


def runs_insert(url: str, key: str, run_id: str, canton_code: str,
                bbox: tuple, tile_size_m: int, notes: dict) -> int:
    body = [{
        "run_id": run_id, "canton_code": canton_code, "status": "running",
        "bbox_min_e": bbox[0], "bbox_min_n": bbox[1], "bbox_max_e": bbox[2], "bbox_max_n": bbox[3],
        "tile_size_m": tile_size_m, "notes": notes,
    }]
    res = _rest(url, key, "POST", "/federal_cadastral_parcels_runs",
                data=json.dumps(body), headers={"Prefer": "return=representation"})
    return res[0]["id"]


def runs_update(url: str, key: str, row_id: int, **fields) -> None:
    fields.setdefault("ended_at", datetime.now(timezone.utc).isoformat())
    _rest(url, key, "PATCH", f"/federal_cadastral_parcels_runs?id=eq.{row_id}",
          data=json.dumps(fields), headers={"Prefer": "return=minimal"})


def soft_delete_missing(url: str, key: str, canton_code: str, run_started_at: str) -> int:
    """Mark rows whose last_seen_at < run_started_at AND not yet deleted.
    Retries on transient SSL/Connection/Timeout errors and 5xx responses, same
    profile as the upsert RPC client."""
    from urllib.parse import quote
    h = {"apikey": key, "Authorization": f"Bearer {key}",
         "Content-Type": "application/json", "Accept-Profile": "bronze_ch",
         "Content-Profile": "bronze_ch", "Prefer": "count=exact,return=minimal"}
    ts = quote(run_started_at, safe="")
    qs = (f"/federal_cadastral_parcels?canton_code=eq.{canton_code}"
          f"&last_seen_at=lt.{ts}&deleted_at=is.null")
    backoff = 5.0
    last_err = None
    for attempt in range(6):
        try:
            r = requests.patch(f"{url}/rest/v1{qs}", headers=h,
                               data=json.dumps({"deleted_at": datetime.now(timezone.utc).isoformat()}),
                               timeout=180)
            r.raise_for_status()
            cr = r.headers.get("Content-Range", "")
            return int(cr.split("/")[-1]) if "/" in cr else 0
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_err = e
            print(f"[warn] soft-delete transient error attempt {attempt+1}/6: "
                  f"{type(e).__name__}; retrying in {backoff:.0f}s", flush=True)
            time.sleep(backoff); backoff *= 2
        except requests.exceptions.HTTPError as e:
            if e.response is not None and 500 <= e.response.status_code < 600:
                last_err = e
                print(f"[warn] soft-delete {e.response.status_code} attempt {attempt+1}/6; "
                      f"retrying in {backoff:.0f}s", flush=True)
                time.sleep(backoff); backoff *= 2
                continue
            raise
    raise last_err if last_err else RuntimeError("soft-delete failed after retries")


_CANTON_TO_KN = {'ZH':1,'BE':2,'LU':3,'UR':4,'SZ':5,'OW':6,'NW':7,'GL':8,'ZG':9,'FR':10,
                 'SO':11,'BS':12,'BL':13,'SH':14,'AR':15,'AI':16,'SG':17,'GR':18,'AG':19,
                 'TG':20,'TI':21,'VD':22,'VS':23,'NE':24,'GE':25,'JU':26}


def backfill_commune_bfs(re_url: str, re_key: str, canton_code: str,
                          batch_size: int = 50000) -> int:
    """Spatial-join commune_bfs from bronze_ch.swiss_communes_geo (authoritative
    swissBOUNDARIES3D source — complete commune coverage with proper federal
    BFS Gemeindenummer). Runs batched. Returns rows updated."""
    import psycopg2
    db_pass = os.environ.get("RE_LLM_DB_PASSWORD")
    if not db_pass:
        try:
            reg = json.load(open(os.path.expanduser("~/supabase-registry/supabase-projects.json")))
            db_pass = reg["re-llm"]["db_password"]
        except Exception:
            print("[backfill_commune_bfs] no DB password available; skipping")
            return 0
    host = "db.znrvddgmczdqoucmykij.supabase.co"
    kn = _CANTON_TO_KN.get(canton_code)
    if kn is None:
        return 0
    conn = psycopg2.connect(host=host, port=5432, user="postgres", password=db_pass,
                            dbname="postgres", keepalives=1, keepalives_idle=30,
                            keepalives_interval=10, keepalives_count=5,
                            application_name="fcp-commune-backfill")
    conn.autocommit = True
    total = 0
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout='10min'")
            while True:
                cur.execute("""
                    WITH t AS (
                      SELECT id FROM bronze_ch.federal_cadastral_parcels
                       WHERE canton_code=%s AND commune_bfs IS NULL
                       LIMIT %s FOR UPDATE SKIP LOCKED
                    )
                    UPDATE bronze_ch.federal_cadastral_parcels p
                       SET commune_bfs = c.bfs_nummer
                      FROM t, bronze_ch.swiss_communes_geo c
                     WHERE p.id = t.id
                       AND c.kantonsnummer = %s
                       AND c.objektart = 'Gemeindegebiet'
                       AND ST_Contains(c.geometry, ST_Centroid(p.geometry))
                """, (canton_code, batch_size, kn))
                n = cur.rowcount
                total += n
                if n == 0:
                    break
    finally:
        conn.close()
    return total


def update_camelote_freshness(camelote_url: str, camelote_key: str, dataset_code: str,
                              record_count: int, error: str | None = None) -> None:
    """PATCH camelote-data datasets row keyed by code; sets last_acquired_at + record_count."""
    if not camelote_url or not camelote_key:
        return
    payload = {
        "last_acquired_at": datetime.now(timezone.utc).isoformat(),
        "record_count": record_count,
        "last_error": error,
    }
    h = {"apikey": camelote_key, "Authorization": f"Bearer {camelote_key}",
         "Content-Type": "application/json", "Prefer": "return=minimal"}
    requests.patch(f"{camelote_url}/rest/v1/datasets?code=eq.{dataset_code}",
                   headers=h, data=json.dumps(payload), timeout=30).raise_for_status()


def count_canton_rows(url: str, key: str, canton_codes: list[str]) -> int:
    """Total rows across the given cantons (excluding soft-deleted).
    Per-canton query to keep each scan fast and avoid PostgREST timeouts on
    multi-canton IN-clauses scanning a 2.8M-row table."""
    h = {"apikey": key, "Authorization": f"Bearer {key}",
         "Accept-Profile": "bronze_ch", "Prefer": "count=exact",
         "Range-Unit": "items", "Range": "0-0"}
    total = 0
    for canton in canton_codes:
        r = requests.get(
            f"{url}/rest/v1/federal_cadastral_parcels?select=id"
            f"&canton_code=eq.{canton}&deleted_at=is.null",
            headers=h, timeout=60)
        r.raise_for_status()
        cr = r.headers.get("Content-Range", "*/0")
        total += int(cr.split("/")[-1])
    return total


# ──────────────────────────────────────────────────────────────
# Tier orchestrator (the entry point T1/T2/T3 wrappers call)
# ──────────────────────────────────────────────────────────────

@dataclass
class TierConfig:
    tier_label: str           # 't1' | 't2' | 't3'
    cantons: list[str]        # priority order
    dataset_code: str         # camelote-data datasets.code
    initial_tile_m: int = 2000
    min_tile_m: int = 50
    upsert_batch_size: int = 500
    rate_limit_rpm: int = None  # default from env


def run_tier(cfg: TierConfig) -> int:
    """
    Main loop. Returns 0 on success, 1 on any canton failure.
    CLI flags override cfg.cantons via --cantons VD,NE; --dry-run; --max-tiles N.
    """
    args = _parse_cli()
    cantons = args.cantons.split(",") if args.cantons else cfg.cantons
    if args.bbox_override:
        parts = [int(x) for x in args.bbox_override.split(",")]
        if len(parts) != 4:
            raise SystemExit("--bbox-override expects min_e,min_n,max_e,max_n")
        CANTON_BBOX[cantons[0]] = tuple(parts)
        print(f"[override] CANTON_BBOX[{cantons[0]}] = {tuple(parts)} (this run only)")

    re_url = os.environ["RE_LLM_SUPABASE_URL"]
    re_key = os.environ["RE_LLM_SUPABASE_SERVICE_ROLE_KEY"]
    cm_url = os.environ.get("CAMELOTE_SUPABASE_URL", "")
    cm_key = os.environ.get("CAMELOTE_SUPABASE_KEY", "")
    rpm = cfg.rate_limit_rpm or int(os.environ.get("GEOADMIN_RATE_LIMIT", "20"))

    limiter = RateLimiter(rpm=rpm)
    session = requests.Session()
    session.headers["User-Agent"] = "camelote-data-pipelines/federal_cadastral_parcels"
    upserter = CadastralUpsertClient(re_url, re_key)

    run_id = str(uuid.uuid4())
    overall_ok = True

    for canton in cantons:
        ok = _run_one_canton(canton, run_id, cfg, args, limiter, session,
                             upserter, re_url, re_key)
        overall_ok = overall_ok and ok

    # Update camelote freshness once per tier run with the tier's row total.
    if not args.dry_run:
        try:
            total = count_canton_rows(re_url, re_key, cfg.cantons)
            update_camelote_freshness(cm_url, cm_key, cfg.dataset_code, total,
                                      None if overall_ok else "one or more cantons failed")
        except Exception as e:
            print(f"[warn] camelote freshness update failed: {e}", file=sys.stderr)

    return 0 if overall_ok else 1


def _run_one_canton(canton: str, run_id: str, cfg: TierConfig, args,
                    limiter: RateLimiter, session: requests.Session,
                    upserter: CadastralUpsertClient,
                    re_url: str, re_key: str) -> bool:
    bbox = CANTON_BBOX[canton]
    started_at = datetime.now(timezone.utc).isoformat()
    print(f"[{canton}] starting (run_id={run_id})")

    # Detect first-refresh: if any row in this canton uses the legacy source_url,
    # this is the first GeoAdmin refresh — flag it in notes so geometry-resync
    # inflated rows_updated counts don't cause confusion.
    is_first_refresh = _detect_first_refresh(re_url, re_key, canton)
    notes = {
        "tier": cfg.tier_label,
        "first_refresh_geometry_resync": is_first_refresh,
        "started_at": started_at,
    }

    # Resume context: always created (so non-resume runs still record tile_progress
    # for future --resume use). enabled flag toggles the skip-check.
    resume_ctx = ResumeContext(enabled=args.resume, url=re_url, key=re_key,
                               fresh_within_hours=args.resume_within_hours)
    if args.resume:
        resume_ctx.preload(canton)
        skipped_pre = len(resume_ctx._done_cache)
        print(f"[{canton}] --resume: {skipped_pre} tiles already complete, will skip", flush=True)

    # Per-canton progress logger (every N seconds).
    last_log = [time.time()]
    def progress_cb(stats: TileStats):
        if time.time() - last_log[0] >= args.progress_every_sec:
            print(f"[{canton}] progress: tiles_done={stats.tiles_done} "
                  f"api_calls={stats.api_calls} api_errors={stats.api_errors} "
                  f"features_seen={stats.features_seen} max_depth={stats.max_recursion_depth}",
                  flush=True)
            last_log[0] = time.time()

    if args.dry_run:
        print(f"[{canton}] DRY-RUN bbox={bbox} — walking tiles, no DB writes")
        stats = TileStats()
        would_insert = 0; would_skip = 0
        sample = []
        def on_feats_dry(feats):
            nonlocal would_insert, would_skip
            for f in feats:
                row = feature_to_row(f)
                if row is None:
                    would_skip += 1
                else:
                    would_insert += 1
                    if len(sample) < 3:
                        sample.append({k: row[k] for k in ("canton_code","commune_bfs","ident_dn",
                                                            "parcel_number","egrid","surface_m2",
                                                            "completeness","source_url")})
        walk_canton(bbox, canton, limiter, session,
                    initial_tile_m=cfg.initial_tile_m, min_tile_m=cfg.min_tile_m,
                    stats=stats, on_features=on_feats_dry)
        print(f"[{canton}] DRY-RUN result: tiles_done={stats.tiles_done} "
              f"api_calls={stats.api_calls} api_errors={stats.api_errors} "
              f"max_recursion_depth={stats.max_recursion_depth} "
              f"features_seen={stats.features_seen} would_insert={would_insert} "
              f"would_skip(no_egrid_or_geom)={would_skip}")
        for i, s in enumerate(sample):
            print(f"[{canton}] DRY-RUN sample {i}: {s}")
        return True

    run_row_id = runs_insert(re_url, re_key, run_id, canton, bbox, cfg.initial_tile_m, notes)

    stats = TileStats()
    totals = {"inserted": 0, "updated": 0, "unchanged": 0}
    err_msg = None
    buf: list[dict] = []
    tiles_seen = 0

    def flush():
        nonlocal totals
        if not buf: return
        res = upserter.upsert(buf)
        for k in totals: totals[k] += int(res.get(k, 0))
        buf.clear()

    def on_features(feats: list[dict]):
        nonlocal tiles_seen
        tiles_seen += 1
        for f in feats:
            row = feature_to_row(f)
            if row: buf.append(row)
        if len(buf) >= cfg.upsert_batch_size:
            flush()
        if args.max_tiles and tiles_seen >= args.max_tiles:
            raise _MaxTilesReached()

    try:
        walk_canton(bbox, canton, limiter, session,
                    initial_tile_m=cfg.initial_tile_m,
                    min_tile_m=cfg.min_tile_m,
                    stats=stats, on_features=on_features,
                    resume=resume_ctx, run_id=run_id, progress_cb=progress_cb)
        flush()
    except _MaxTilesReached:
        flush()
        notes["truncated_at_max_tiles"] = args.max_tiles
    except Exception as e:
        err_msg = f"{type(e).__name__}: {e}"
        try: flush()
        except Exception: pass

    soft_deleted = 0
    status = "failed"
    if not err_msg:
        if args.resume:
            notes["softdelete_skipped"] = "resume mode — partial sweep cannot infer deletions"
            status = "success"
        else:
            try:
                soft_deleted = soft_delete_missing(re_url, re_key, canton, started_at)
                status = "success"
            except Exception as e:
                err_msg = f"soft-delete failed: {type(e).__name__}: {e}"
                status = "partial"
    elif stats.tiles_done > 0:
        status = "partial"

    # Post-canton enrichment: spatial-join commune_bfs from bronze_ch.federal_communes
    # for any rows in this canton with commune_bfs IS NULL. Cheap (canton_code +
    # GiST), idempotent. Don't fail the whole run on enrichment errors.
    try:
        bfs_filled = backfill_commune_bfs(re_url, re_key, canton)
        notes["commune_bfs_filled"] = bfs_filled
    except Exception as e:
        notes["commune_bfs_error"] = f"{type(e).__name__}: {e}"

    runs_update(re_url, re_key, run_row_id,
                status=status,
                tiles_total=stats.tiles_done + stats.tiles_failed,
                tiles_done=stats.tiles_done,
                tiles_failed=stats.tiles_failed,
                max_recursion_depth=stats.max_recursion_depth,
                api_calls=stats.api_calls,
                api_errors=stats.api_errors,
                features_seen=stats.features_seen,
                rows_inserted=totals["inserted"],
                rows_updated=totals["updated"],
                rows_unchanged=totals["unchanged"],
                rows_softdeleted=soft_deleted,
                error_message=err_msg,
                notes=notes)
    print(f"[{canton}] {status} {totals} soft_deleted={soft_deleted} "
          f"tiles={stats.tiles_done} api_calls={stats.api_calls}")
    return status in ("success", "partial")


def _detect_first_refresh(url: str, key: str, canton: str) -> bool:
    """Best-effort flag check; failures default to False (don't kill the walk)."""
    h = {"apikey": key, "Authorization": f"Bearer {key}", "Accept-Profile": "bronze_ch"}
    backoff = 5.0
    for attempt in range(4):
        try:
            r = requests.get(
                f"{url}/rest/v1/federal_cadastral_parcels?select=source_url"
                f"&canton_code=eq.{canton}&source_url=eq.api3.geo.admin.ch/identify&limit=1",
                headers=h, timeout=30)
            r.raise_for_status()
            return len(r.json()) == 0
        except Exception as e:
            print(f"[warn] _detect_first_refresh attempt {attempt+1}/4: {type(e).__name__}; retry {backoff:.0f}s", flush=True)
            time.sleep(backoff); backoff *= 2
    print(f"[warn] _detect_first_refresh gave up; assuming refresh (False)")
    return False


class _MaxTilesReached(Exception): pass


def _parse_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--cantons", help="comma-sep override of tier canton list")
    p.add_argument("--dry-run", action="store_true", help="walk bboxes but don't upsert")
    p.add_argument("--max-tiles", type=int, default=0, help="stop each canton after N tiles")
    p.add_argument("--bbox-override",
                   help="scoped to FIRST canton: replace its CANTON_BBOX with min_e,min_n,max_e,max_n")
    p.add_argument("--resume", action="store_true",
                   help="skip tiles already in tile_progress within --resume-within-hours")
    p.add_argument("--resume-within-hours", type=int, default=168,
                   help="freshness window for --resume (default 168 = 7 days)")
    p.add_argument("--progress-every-sec", type=int, default=1800,
                   help="emit per-canton progress line every N seconds (default 1800 = 30 min)")
    return p.parse_args()
