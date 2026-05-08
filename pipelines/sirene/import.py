#!/usr/bin/env python3
"""
INSEE SIRENE — Import Pipeline (entry point)

Two modes, selected by --mode flag (default: incremental):

    python pipelines/sirene/import.py --mode bulk         # one-time, local-only
    python pipelines/sirene/import.py --mode incremental  # daily cron

Bulk mode delegates to bulk_load.main() (parquet stock files from data.gouv.fr).
Incremental mode delegates to incremental_sync.main() (INSEE API v3.11).

Hard rules (same as every Lamap parser):
    - bronze on re-LLM only (target schema: bronze_fr)
    - No DROP CASCADE
    - Idempotent UPSERT on siren / siret
    - PostgREST batch_upsert pattern

Environment variables:
    RE_LLM_SUPABASE_URL              - re-LLM project URL (required)
    RE_LLM_SUPABASE_SERVICE_ROLE_KEY - re-LLM service_role key (required)
    RE_LLM_SCHEMA                    - target schema (default: bronze_fr)
    INSEE_CLIENT_ID                  - OAuth2 client id (required for incremental mode only)
    INSEE_CLIENT_SECRET              - OAuth2 client secret (required for incremental mode only)
    SIRENE_CACHE_DIR                 - local download cache (default: /tmp/sirene_cache)
    SIRENE_LOOKBACK_DAYS             - incremental window if no prior sync (default: 1)

See pipelines/sirene/README.md for full source docs.
"""

from __future__ import annotations

import argparse
import os
import sys


def main() -> None:
    ap = argparse.ArgumentParser(description="INSEE SIRENE import pipeline")
    ap.add_argument(
        "--mode",
        choices=["bulk", "incremental"],
        default=os.environ.get("SIRENE_MODE", "incremental"),
        help="bulk = parquet stock files (one-time); incremental = API daily delta",
    )
    # Forward unknown args to the sub-mode parser
    args, rest = ap.parse_known_args()
    sys.argv = [sys.argv[0], *rest]

    if args.mode == "bulk":
        from pipelines.sirene.bulk_load import main as run
    else:
        from pipelines.sirene.incremental_sync import main as run

    run()


if __name__ == "__main__":
    # Make `from pipelines.sirene...` importable when run as a script
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(here, "..", ".."))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    main()
