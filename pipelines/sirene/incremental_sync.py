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
# SIRENs per /siret query. Bounded by the 10k offset ceiling, not URL length:
# measured over 4.88M companies the distribution is avg 1.4 establishments,
# p99 = 5, p99.9 = 8, max 1478, only 5 companies anywhere above 500. 50 SIRENs
# is ~70 rows typically, a few thousand worst case, and turns ~2.6k calls into
# ~53. Batches that would still overflow are split by the client.
SIREN_BATCH_SIZE = int(os.environ.get("SIRENE_SIREN_BATCH_SIZE", "50"))


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
    sync_at: str | None = None,
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
        # Explicit sync_at = the END of the window just completed, so an interrupted
        # backfill resumes exactly where it stopped instead of skipping the remainder.
        body["last_sync_at"] = sync_at or datetime.now(timezone.utc).isoformat()
    r = requests.patch(
        endpoint,
        params={"dataset_code": f"eq.{dataset_code}"},
        headers=headers,
        json=body,
        timeout=30,
    )
    if r.status_code not in (200, 204):
        print(f"  WARN: registry update failed {r.status_code}: {r.text[:200]}")


class WindowTooLarge(RuntimeError):
    """A date window would exceed the API offset ceiling; split it, do not truncate."""


def _sync_window(client, url, key, schema, start_iso: str, end_iso: str):
    """Sync one closed date window. Returns (companies, establishments, matched)."""
    # ── Companies ────────────────────────────────────────────
    print("\n[1/2] Pulling updated UniteLegale records …")
    # Filter NAF SERVER-SIDE. Fetching every changed unite legale and filtering
    # locally overruns the API's 10k offset ceiling: a one-day window is ~16k
    # changed records (2d ~34k, 7d ~217k), so the job could never complete.
    # With this clause the same window is ~2.6k. See naf_filter.solr_clause.
    q = f"dateDernierTraitementUniteLegale:[{start_iso} TO {end_iso}] AND ({solr_clause()})"
    total_matched = client.count_unites_legales(q)
    print(f"  server-side match count: {total_matched:,} (offset ceiling {MAX_OFFSET:,})")
    if total_matched >= MAX_OFFSET:
        # Do not import a capped result set silently — a truncated import looks
        # like a clean run while quietly dropping records.
        print(f"  ERROR: {total_matched:,} matches at or above the {MAX_OFFSET:,} ceiling; "
              "the window would be truncated.")
        print("  Narrow SIRENE_LOOKBACK_DAYS or split the date range, then re-run.")
        raise WindowTooLarge(
            f"{total_matched} matches in [{start_iso} TO {end_iso}] at or above the "
            f"{MAX_OFFSET} ceiling"
        )

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
        raise WindowTooLarge(str(e))

    print(f"  scanned {scanned:,}, kept {len(company_rows):,} RE-sector ({time.time() - t0:.0f}s)")

    if company_rows:
        # No local dedupe: shared.supabase_client.batch_upsert collapses duplicate
        # conflict keys itself (last wins) and logs a count. The Sirene query ORs 13
        # periode() clauses, so a company matching more than one period legitimately
        # comes back twice in a window — expect that DEDUPE line in the log.
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
    # Batched: one /siret call per SIREN_BATCH_SIZE SIRENs instead of one per SIREN.
    # At the 2.1s rate cap that is the difference between ~92 min and ~2 min for a
    # normal day. iter_etablissements_by_sirens splits a batch that would exceed the
    # offset ceiling, so batching cannot cause a silent truncation.
    sirens_sorted = sorted(re_sirens)
    n_batches = (len(sirens_sorted) + SIREN_BATCH_SIZE - 1) // SIREN_BATCH_SIZE
    print(f"\n[2/2] Fetching establishments for {len(sirens_sorted):,} SIRENs "
          f"in {n_batches:,} batch(es) of {SIREN_BATCH_SIZE} …")
    et_rows: list[dict] = []
    t1 = time.time()
    for bi in range(n_batches):
        batch = sirens_sorted[bi * SIREN_BATCH_SIZE:(bi + 1) * SIREN_BATCH_SIZE]
        for et in client.iter_etablissements_by_sirens(batch):
            rec = map_api_etablissement({"etablissement": et})
            if rec and rec.get("siret"):
                et_rows.append(rec)
        if (bi + 1) % 10 == 0 or bi + 1 == n_batches:
            print(f"    batch {bi + 1:,}/{n_batches:,}, "
                  f"{len(et_rows):,} establishments collected ({time.time() - t1:.0f}s)")

    if et_rows:
        n_e = batch_upsert(
            url=url, key=key, table="etablissements",
            records=et_rows, conflict_column="siret",
            schema=schema, batch_size=DEFAULT_BATCH_SIZE,
        )
        print(f"  upserted {n_e:,} into {schema}.etablissements")
    else:
        n_e = 0

    return n_c, n_e, total_matched


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

    # ── Walk the window forward in chunks ────────────────────
    # Each chunk must fit the 10k ceiling AND the workflow time budget. A day is
    # ~2.6k NAF-filtered companies, comfortably inside both. The watermark advances
    # after EACH successful chunk, so an interrupted backfill resumes rather than
    # restarting — and a bounded number of chunks per invocation lets a daily cron
    # chip away at a backlog without ever exceeding its timeout.
    chunk_days = int(os.environ.get("SIRENE_CHUNK_DAYS", "1"))
    max_chunks = int(os.environ.get("SIRENE_MAX_CHUNKS", "25"))
    # Wall-clock budget. A chunk count alone CANNOT bound the runtime: measured
    # 2026-08-07, one day-chunk ranged from 86s (798 companies) to 712s (7387
    # companies) - an 8x spread, because daily INSEE volume is lumpy. Sizing the cron
    # on the fast sample gave ~22 min; the slow one would have been ~178 min against a
    # 30-minute timeout. Stop STARTING chunks once the budget is spent; the watermark
    # has already advanced per completed chunk, so the next run resumes cleanly.
    time_budget_s = int(os.environ.get("SIRENE_TIME_BUDGET_S", "1200"))
    now = datetime.now(timezone.utc)
    cursor = start_dt
    tot_c = tot_e = tot_m = 0
    chunks_done = 0
    t_all = time.time()

    while cursor.date() <= now.date() and chunks_done < max_chunks:
        if chunks_done and time.time() - t_all > time_budget_s:
            print(f"\n  time budget reached ({time_budget_s}s) after {chunks_done} chunk(s); "
                  "stopping cleanly. Watermark is current to the last completed chunk.")
            break
        chunk_end = min(cursor + timedelta(days=chunk_days), now)
        s_iso, e_iso = cursor.date().isoformat(), chunk_end.date().isoformat()
        print(f"\n{'=' * 60}\n  CHUNK {chunks_done + 1}: [{s_iso} TO {e_iso}]\n{'=' * 60}")
        try:
            n_c, n_e, matched = _sync_window(client, url, key, schema, s_iso, e_iso)
        except WindowTooLarge as e:
            print(f"  ERROR: {e}")
            print("  Reduce SIRENE_CHUNK_DAYS and re-run; the watermark is unchanged "
                  "so nothing is skipped.")
            _update_registry(url, key, "insee_sirene_companies", 0, "error")
            sys.exit(3)

        tot_c += n_c; tot_e += n_e; tot_m += matched
        chunks_done += 1
        # Advance the watermark to the END of the chunk just completed.
        _update_registry(url, key, "insee_sirene_companies", n_c, "success",
                         sync_at=chunk_end.isoformat())
        _update_registry(url, key, "insee_sirene_etablissements", n_e, "success",
                         sync_at=chunk_end.isoformat())
        cursor = chunk_end
        if chunk_end >= now:
            break

    caught_up = cursor >= now or cursor.date() >= now.date()
    print(f"\n{'=' * 60}")
    print(f"  chunks: {chunks_done}  matched: {tot_m:,}  "
          f"companies: {tot_c:,}  establishments: {tot_e:,}")
    print(f"  duration: {time.time() - t_all:.0f}s   caught up: {caught_up}")
    if not caught_up:
        print(f"  watermark now {cursor.date().isoformat()}; re-run to continue the backfill.")
    print("=" * 60)


if __name__ == "__main__":
    main()
