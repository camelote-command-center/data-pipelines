"""
INSEE SIRENE bulk-load pipeline (CSV/parquet stock files from data.gouv.fr).

This is a one-time initialisation — populate bronze_fr.companies and
bronze_fr.etablissements from the monthly stock files. Once seeded, daily
deltas come via the API in incremental_sync.py.

Source: https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
Files used:
    StockUniteLegale     (parquet, ~691 MB)
    StockEtablissement   (parquet, ~2.17 GB)

Strategy:
    1. Stream parquet by row group with pyarrow (avoids loading 30M rows in RAM)
    2. Filter rows where activitePrincipaleUniteLegale ∈ NAF_WHITELIST
    3. Map → bronze_fr columns
    4. UPSERT in batches of 500 via PostgREST
    5. After UniteLegale completes, repeat for Etablissement, filtering to
       SIRENs already loaded in bronze_fr.companies

Idempotent — re-running on the same stock release is a no-op (UPSERT on
siren / siret keys updates the same rows).

Designed to run LOCALLY (developer Mac), not GHA. The full bulk takes ~30-60 min
and downloads ~3 GB; user wants to monitor and be able to interrupt.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Iterable

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert  # noqa: E402

from .column_mapping import map_stock_etablissement, map_stock_unite_legale  # noqa: E402
from .naf_filter import NAF_WHITELIST, normalise as naf_normalise  # noqa: E402

# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────

# data.gouv.fr download URLs (resolved from the dataset metadata API at runtime
# to always pick the latest publication — see _resolve_resource_urls()).
DATAGOUV_DATASET_API = (
    "https://www.data.gouv.fr/api/1/datasets/"
    "base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/"
)

CACHE_DIR = Path(os.environ.get("SIRENE_CACHE_DIR", "/tmp/sirene_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_BATCH_SIZE = 500
PROGRESS_EVERY = 10_000
PAUSE_EVERY = 50_000


# ──────────────────────────────────────────────────────────────
# Source discovery
# ──────────────────────────────────────────────────────────────

def _resolve_resource_urls() -> tuple[str, str, str]:
    """Return (unite_legale_parquet_url, etablissement_parquet_url, publication_label).

    Prefers parquet over zip-csv (smaller + columnar filter).
    """
    r = requests.get(DATAGOUV_DATASET_API, headers={"User-Agent": "lamap-data-pipelines/1.0"}, timeout=30)
    r.raise_for_status()
    resources = r.json().get("resources", [])

    ul_pq = None
    et_pq = None
    pub_label = None

    for res in resources:
        title = res.get("title", "") or ""
        fmt = (res.get("format", "") or "").lower()
        url = res.get("url")
        if fmt != "parquet":
            continue
        # Skip historique
        if "Historique" in title:
            continue
        if "StockUniteLegale" in title and ul_pq is None:
            ul_pq = url
            pub_label = title.split("du")[-1].strip() if " du " in title else None
        elif "StockEtablissement" in title and et_pq is None:
            et_pq = url

    if not ul_pq or not et_pq:
        raise RuntimeError(
            f"Could not resolve parquet URLs from data.gouv.fr; "
            f"got UL={ul_pq!r}, ET={et_pq!r}"
        )
    return ul_pq, et_pq, pub_label or "unknown"


# ──────────────────────────────────────────────────────────────
# Download with resume + cache
# ──────────────────────────────────────────────────────────────

def _download_to_cache(url: str, label: str) -> Path:
    """Download `url` into CACHE_DIR if not already there. Returns the local path."""
    fname = url.split("/")[-1].split("?")[0] or f"{label}.parquet"
    out = CACHE_DIR / fname
    if out.exists() and out.stat().st_size > 1_000_000:
        print(f"  [{label}] already in cache: {out} ({out.stat().st_size / 1e9:.2f} GB)")
        return out
    print(f"  [{label}] downloading {url} → {out} …")
    t0 = time.time()
    with requests.get(url, stream=True, timeout=300, headers={"User-Agent": "lamap-data-pipelines/1.0"}) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        tmp = out.with_suffix(out.suffix + ".part")
        bytes_in = 0
        last_print = 0
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if not chunk:
                    continue
                f.write(chunk)
                bytes_in += len(chunk)
                if bytes_in - last_print >= 100 * (1 << 20):
                    pct = (bytes_in / total * 100) if total else 0.0
                    print(f"    [{label}] {bytes_in / 1e9:.2f} GB ({pct:.1f}%) … elapsed {time.time() - t0:.0f}s")
                    last_print = bytes_in
        tmp.rename(out)
    print(f"  [{label}] done in {time.time() - t0:.0f}s — {out.stat().st_size / 1e9:.2f} GB")
    return out


# ──────────────────────────────────────────────────────────────
# Parquet streaming + filter
# ──────────────────────────────────────────────────────────────

def _iter_parquet_filtered(
    path: Path,
    naf_field: str,
    columns: list[str],
    extra_filter=None,
) -> Iterable[dict]:
    """Yield filtered rows from `path` as dicts.

    `naf_field` is the column name on which we apply NAF_WHITELIST.
    `columns` is the projection (subset of all columns to read — saves memory).
    `extra_filter` is an optional callable(row) → bool applied after NAF.
    """
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(str(path))
    total_groups = parquet.num_row_groups
    print(f"    parquet: {parquet.metadata.num_rows:,} total rows in {total_groups} row groups")

    matched = 0
    scanned = 0
    for gi in range(total_groups):
        # Read only the needed columns for this row group
        try:
            tbl = parquet.read_row_group(gi, columns=columns)
        except Exception as e:
            # Some columns may not exist in older publications; fall back to all
            print(f"    row_group {gi}: column projection failed ({e}), reading all")
            tbl = parquet.read_row_group(gi)

        # pyarrow → list[dict] in chunks of 50k
        rows = tbl.to_pylist()
        scanned += len(rows)
        for row in rows:
            naf_raw = row.get(naf_field)
            if not naf_raw:
                continue
            naf_norm = naf_normalise(naf_raw)
            if naf_norm not in NAF_WHITELIST:
                continue
            if extra_filter is not None and not extra_filter(row):
                continue
            matched += 1
            yield row
        if (gi + 1) % 10 == 0 or gi == total_groups - 1:
            print(
                f"    row_group {gi + 1}/{total_groups}: scanned {scanned:,}, matched {matched:,}"
            )


# ──────────────────────────────────────────────────────────────
# Upsert helpers
# ──────────────────────────────────────────────────────────────

def _upsert_batched(
    url: str,
    key: str,
    schema: str,
    table: str,
    rows: Iterable[dict],
    conflict_column: str,
    label: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    pause_every: int = PAUSE_EVERY,
    progress_every: int = PROGRESS_EVERY,
) -> int:
    """Buffer rows and call shared.batch_upsert in chunks.

    Pauses every `pause_every` rows so the operator can ctrl-C if anything
    looks off.
    """
    buf: list[dict] = []
    total = 0
    t0 = time.time()
    for r in rows:
        buf.append(r)
        if len(buf) >= batch_size:
            n = batch_upsert(
                url=url, key=key, table=table,
                records=buf, conflict_column=conflict_column,
                schema=schema, batch_size=batch_size,
            )
            total += n
            buf = []
            if total % progress_every == 0:
                rate = total / max(time.time() - t0, 0.001)
                print(f"  [{label}] upserted {total:,} ({rate:.0f} rows/s)")
            if pause_every and total % pause_every == 0:
                # 250ms breather every 50k rows so the DB can catch breath
                time.sleep(0.25)
    if buf:
        n = batch_upsert(
            url=url, key=key, table=table,
            records=buf, conflict_column=conflict_column,
            schema=schema, batch_size=batch_size,
        )
        total += n
    print(f"  [{label}] DONE — upserted {total:,} in {time.time() - t0:.0f}s")
    return total


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def _required_env(name: str) -> str:
    v = os.environ.get(name, "")
    if not v:
        print(f"ERROR: env var {name} required")
        sys.exit(1)
    return v


def _siren_set_from_db(url: str, key: str, schema: str) -> set[str]:
    """Pull all SIREN values currently in bronze_fr.companies (paginated)."""
    print("  Loading SIREN whitelist from bronze_fr.companies …")
    sirens: set[str] = set()
    page_size = 5000
    offset = 0
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept-Profile": schema,
    }
    while True:
        endpoint = f"{url.rstrip('/')}/rest/v1/companies"
        r = requests.get(
            endpoint,
            params={"select": "siren", "limit": page_size, "offset": offset},
            headers=headers,
            timeout=120,
        )
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        for row in chunk:
            s = row.get("siren")
            if s:
                sirens.add(str(s))
        offset += page_size
        if len(chunk) < page_size:
            break
        if offset % 50_000 == 0:
            print(f"    loaded {offset:,} SIRENs so far…")
    print(f"  Whitelist size: {len(sirens):,} SIRENs")
    return sirens


def main():
    ap = argparse.ArgumentParser(description="INSEE SIRENE bulk loader (parquet stock files)")
    ap.add_argument("--unite-legale-only", action="store_true", help="Skip etablissement load")
    ap.add_argument("--etablissement-only", action="store_true", help="Skip unite_legale load (assumes companies table already populated)")
    ap.add_argument("--limit-rows", type=int, default=None, help="Stop after N matched rows per stage (for smoke tests)")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = ap.parse_args()

    url = _required_env("RE_LLM_SUPABASE_URL")
    key = _required_env("RE_LLM_SUPABASE_SERVICE_ROLE_KEY")
    schema = os.environ.get("RE_LLM_SCHEMA", "bronze_fr")

    print("=" * 60)
    print("  INSEE SIRENE bulk loader")
    print(f"  Cache:      {CACHE_DIR}")
    print(f"  Target:     {schema} on {url}")
    print(f"  NAF filter: {sorted(NAF_WHITELIST)}")
    print("=" * 60)

    print("\n[1/4] Resolving latest stock URLs from data.gouv.fr …")
    ul_url, et_url, pub_label = _resolve_resource_urls()
    print(f"  publication: {pub_label}")
    print(f"  UniteLegale:    {ul_url}")
    print(f"  Etablissement:  {et_url}")

    if not args.etablissement_only:
        print("\n[2/4] UniteLegale stock — download …")
        ul_path = _download_to_cache(ul_url, "UniteLegale")

        print("\n[3a/4] UniteLegale stock — filter + upsert …")
        ul_columns = [
            "siren", "siretSiegeUniteLegale", "denominationUniteLegale",
            "denominationUsuelle1UniteLegale", "sigleUniteLegale",
            "categorieJuridiqueUniteLegale", "activitePrincipaleUniteLegale",
            "etatAdministratifUniteLegale", "dateCreationUniteLegale",
            "dateDebut", "dateFin", "nombrePeriodesUniteLegale",
        ]

        def _ul_rows():
            count = 0
            for raw in _iter_parquet_filtered(ul_path, "activitePrincipaleUniteLegale", ul_columns):
                rec = map_stock_unite_legale(raw)
                if not rec:
                    continue
                yield rec
                count += 1
                if args.limit_rows and count >= args.limit_rows:
                    return

        n_companies = _upsert_batched(
            url=url, key=key, schema=schema, table="companies",
            rows=_ul_rows(), conflict_column="siren",
            label="companies", batch_size=args.batch_size,
        )
        print(f"\n  UniteLegale DONE — {n_companies:,} companies upserted")

    if args.unite_legale_only:
        print("\n[skip] etablissement load skipped per --unite-legale-only")
        return

    # Build SIREN whitelist from companies table
    print("\n[3b/4] Etablissement stock — build SIREN whitelist …")
    siren_set = _siren_set_from_db(url, key, schema)
    if not siren_set:
        print("  WARN: bronze_fr.companies is empty; skipping etablissement load.")
        return

    print("\n[4/4] Etablissement stock — download + filter + upsert …")
    et_path = _download_to_cache(et_url, "Etablissement")

    et_columns = [
        "siret", "siren", "nic", "denominationUsuelleEtablissement",
        "enseigne1Etablissement", "activitePrincipaleEtablissement",
        "numeroVoieEtablissement", "typeVoieEtablissement",
        "libelleVoieEtablissement", "complementAdresseEtablissement",
        "codePostalEtablissement", "libelleCommuneEtablissement",
        "codeCommuneEtablissement", "etablissementSiege",
        "etatAdministratifEtablissement", "dateCreationEtablissement",
        "dateDebut", "dateFin", "trancheEffectifsEtablissement",
        "caractereEmployeurEtablissement",
    ]

    def _et_rows():
        # Etablissement filter: SIREN must be in the companies whitelist.
        # We do NOT pre-filter by NAF here — an RE-sector parent company can
        # have non-RE establishments, and we want all of them.
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(str(et_path))
        total_groups = parquet.num_row_groups
        scanned = matched = 0
        count = 0
        for gi in range(total_groups):
            try:
                tbl = parquet.read_row_group(gi, columns=et_columns)
            except Exception:
                tbl = parquet.read_row_group(gi)
            for row in tbl.to_pylist():
                scanned += 1
                siren = row.get("siren")
                if not siren or str(siren) not in siren_set:
                    continue
                matched += 1
                rec = map_stock_etablissement(row)
                if not rec:
                    continue
                yield rec
                count += 1
                if args.limit_rows and count >= args.limit_rows:
                    print(f"    [etablissement] hit --limit-rows {args.limit_rows}, stopping")
                    return
            if (gi + 1) % 20 == 0 or gi == total_groups - 1:
                print(f"    row_group {gi + 1}/{total_groups}: scanned {scanned:,}, matched {matched:,}")

    n_etabs = _upsert_batched(
        url=url, key=key, schema=schema, table="etablissements",
        rows=_et_rows(), conflict_column="siret",
        label="etablissements", batch_size=args.batch_size,
    )
    print(f"\n  Etablissement DONE — {n_etabs:,} establishments upserted")

    print("\n" + "=" * 60)
    print("  BULK LOAD COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
