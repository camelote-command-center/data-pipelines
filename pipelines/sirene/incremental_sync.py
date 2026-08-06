"""
INSEE SIRENE daily incremental sync.

Once daily (cron 05:00 UTC), fetch all companies whose dateDernierTraitementUniteLegale
is ≥ the last successful sync date. UPSERT into bronze_fr.companies. Then for each
updated SIREN, fetch its establishments via the API and UPSERT into bronze_fr.etablissements.

Idempotent — re-running the same window produces 0 net changes (same conflict keys).

NAF filtering is done SERVER-SIDE (changed 2026-08-06). It used to be a local
post-filter, on the reasoning that a company can change its activitePrincipale INTO
real estate and we want to catch that. That reasoning still holds, and the server-side
clause preserves it: `periode(...)` matches if ANY historised period carries the code,
so a company that moved into (or out of) the whitelist is still returned. What changed
is that we no longer download every changed unite legale in France to find them — a
one-day window is ~16k records against a 10k offset ceiling, so the job could never
finish. With the clause it is ~2.6k.

The local is_real_estate() check is kept, but only as a cheap assertion against drift
between the query clause and the whitelist — it is no longer doing the filtering work.

For etablissements, we only fetch ones whose SIREN is already in our companies table.

Environment variables:
    RE_LLM_SUPABASE_URL
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY
    RE_LLM_SCHEMA (default: bronze_fr)
    RE_LLM_DATABASE_URL    direct DSN, used to read the INSEE API key from the vault
    SIRENE_LOOKBACK_DAYS   optional, default 1
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from shared.supabase_client import batch_upsert  # noqa: E402

from .api_client import MAX_OFFSET, CredentialsMissing, SireneClient  # noqa: E402
from .column_mapping import map_api_etablissement, map_api_unite_legale  # noqa: E402
from .naf_filter import is_real_estate, solr_clause  # noqa: E402

DEFAULT_BATCH_SIZE = 500
LOOKBACK_DAYS_DEFAULT = 1   # cron runs daily; pull last 24h (+1 day buffer)


def _required(name: str) -> str:
    v = os.environ.get(name, "")
    if not v:
        print(f"ERROR: env var {name} required")
        sys.exit(1)
    return v


def _last_sync_at(url: str, key: str) -> str | None:
    """Read last_sync_at from bronze_ch._registry for the SIRENE companies dataset."""
    endpoint = f"{url.rstrip('/')}/rest/v1/_registry"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept-Profile": "bronze_ch",
    }
    r = requests.get(
        endpoint,
        params={"select": "last_sync_at", "dataset_code": "eq.insee_sirene_companies"},
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    rows = r.json()
    if rows and rows[0].get("last_sync_at"):
        return rows[0]["last_sync_at"]
    return None


def _update_registry(
    url: str,
    key: str,
    dataset_code: str,
    rows: int,
    status: str,
) -> None:
    endpoint = f"{url.rstrip('/')}/rest/v1/_registry"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Profile": "bronze_ch",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    body = {
        "last_sync_status": status,
        "last_sync_rows": rows,
    }
    # Advance the watermark ONLY on success. Stamping it on failure silently
    # discards every change since the last good run: the next run starts from the
    # failure time, the skipped window is never re-requested, and nothing alerts
    # because the job looks like it "ran". Observed 2026-08-06 — a guarded failure
    # moved this marker from 2026-05-09 to that day, which would have dropped ~90
    # days of changes.
    if status == "success":
        body["last_sync_at"] = datetime.now(timezone.utc).isoformat()
    r = requests.patch(
        endpoint,
        params={"dataset_code": f"eq.{dataset_code}"},
        headers=headers,
        json=body,
        timeout=30,
    )
    if r.status_code not in (200, 204):
        print(f"  WARN: registry update failed {r.status_code}: {r.text[:200]}")


def main():
    url = _required("RE_LLM_SUPABASE_URL")
    key = _required("RE_LLM_SUPABASE_SERVICE_ROLE_KEY")
    schema = os.environ.get("RE_LLM_SCHEMA", "bronze_fr")
    lookback_days = int(os.environ.get("SIRENE_LOOKBACK_DAYS", str(LOOKBACK_DAYS_DEFAULT)))

    print("=" * 60)
    print("  INSEE SIRENE incremental sync")
    print(f"  Target:   {schema} on {url}")
    print(f"  Lookback: {lookback_days} days")
    print("=" * 60)

    try:
        client = SireneClient()
    except CredentialsMissing as e:
        print(f"\n[blocked] {e}")
        print("[blocked] Filed P2 bug; this workflow stays disabled until creds arrive.")
        sys.exit(2)

    # Resolve start date
    last = _last_sync_at(url, key)
    if last:
        start_dt = datetime.fromisoformat(last.replace("Z", "+00:00")) - timedelta(days=1)
        print(f"\n  last_sync_at:  {last}")
    else:
        start_dt = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        print(f"\n  no prior sync; starting from {lookback_days} days ago")
    start_iso = start_dt.date().isoformat()
    print(f"  fetching dateDernierTraitementUniteLegale:[{start_iso} TO *]")

    # ── Companies ────────────────────────────────────────────
    print("\n[1/2] Pulling updated UniteLegale records …")
    # Filter NAF SERVER-SIDE. Fetching every changed unite legale and filtering
    # locally overruns the API's 10k offset ceiling: a one-day window is ~16k
    # changed records (2d ~34k, 7d ~217k), so the job could never complete.
    # With this clause the same window is ~2.6k. See naf_filter.solr_clause.
    q = f"dateDernierTraitementUniteLegale:[{start_iso} TO *] AND ({solr_clause()})"
    total_matched = client.count_unites_legales(q)
    print(f"  server-side match count: {total_matched:,} (offset ceiling {MAX_OFFSET:,})")
    if total_matched >= MAX_OFFSET:
        # Do not import a capped result set silently — a truncated import looks
        # like a clean run while quietly dropping records.
        print(f"  ERROR: {total_matched:,} matches at or above the {MAX_OFFSET:,} ceiling; "
              "the window would be truncated.")
        print("  Narrow SIRENE_LOOKBACK_DAYS or split the date range, then re-run.")
        _update_registry(url, key, "insee_sirene_companies", 0, "error")
        sys.exit(3)

    company_rows: list[dict] = []
    re_sirens: set[str] = set()
    scanned = 0
    t0 = time.time()
    try:
        for ul in client.iter_unites_legales(q):
            scanned += 1
            rec = map_api_unite_legale({"uniteLegale": ul})
            if not rec or not rec.get("siren"):
                continue
            # Kept as a cheap assertion. The server-side clause already restricts
            # to the whitelist; this catches a drift between the clause and the
            # whitelist (e.g. a code added to one and not the other) rather than
            # doing the real filtering work.
            if not is_real_estate(rec.get("activite_principale")):
                continue
            company_rows.append(rec)
            re_sirens.add(rec["siren"])
            if scanned % 1000 == 0:
                print(f"    scanned {scanned:,}, kept {len(company_rows):,} RE-sector")
    except RuntimeError as e:
        # Hit MAX_OFFSET — too many updates in one window. Caller should narrow lookback.
        print(f"  ERROR: {e}")
        print("  Try reducing SIRENE_LOOKBACK_DAYS=0 or splitting the date window.")
        _update_registry(url, key, "insee_sirene_companies", 0, "error")
        sys.exit(3)

    print(f"  scanned {scanned:,}, kept {len(company_rows):,} RE-sector ({time.time() - t0:.0f}s)")

    if company_rows:
        n_c = batch_upsert(
            url=url, key=key, table="companies",
            records=company_rows, conflict_column="siren",
            schema=schema, batch_size=DEFAULT_BATCH_SIZE,
        )
        print(f"  upserted {n_c:,} into {schema}.companies")
    else:
        n_c = 0
        print("  no RE-sector updates in this window")

    # ── Establishments for the SIRENs we just touched ────────
    print(f"\n[2/2] Fetching establishments for {len(re_sirens):,} SIRENs …")
    et_rows: list[dict] = []
    for i, siren in enumerate(sorted(re_sirens), start=1):
        for et in client.iter_etablissements_by_siren(siren):
            rec = map_api_etablissement({"etablissement": et})
            if rec and rec.get("siret"):
                et_rows.append(rec)
        if i % 50 == 0:
            print(f"    {i:,}/{len(re_sirens):,} SIRENs processed, {len(et_rows):,} establishments collected")

    if et_rows:
        n_e = batch_upsert(
            url=url, key=key, table="etablissements",
            records=et_rows, conflict_column="siret",
            schema=schema, batch_size=DEFAULT_BATCH_SIZE,
        )
        print(f"  upserted {n_e:,} into {schema}.etablissements")
    else:
        n_e = 0

    _update_registry(url, key, "insee_sirene_companies", n_c, "success")
    _update_registry(url, key, "insee_sirene_etablissements", n_e, "success")

    print("\n" + "=" * 60)
    print(f"  SYNC COMPLETE — {n_c:,} companies, {n_e:,} establishments")
    print("=" * 60)


if __name__ == "__main__":
    main()
