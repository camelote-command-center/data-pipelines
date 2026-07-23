#!/usr/bin/env python3
"""
Fail any PR that adds a scheduled acquisition workflow which will be INVISIBLE on
https://admin.pixxels.io/monitoring.

Registration there is manual: a parser shows up if and only if a row exists in
pixxels_data.public.datasets (plus a dataset_destinations row). Nothing is derived
from run logs or from the GitHub Actions inventory. That has bitten us twice:

  1. 11 scheduled parsers had NO datasets row at all -- including uk-price-paid
     (31.4M rows) and fr-dvf (20.3M rows). Invisible on the dashboard AND excluded
     from every health sweep.
  2. Rows seeded status=deprecated with empty targets in the 2026-01-10 bulk
     registration were never revisited: ge_cad_batiment_sousol and five
     ge_cad_bati3d_* layers were live parsers hidden behind a dead status flag.

This guard catches BOTH modes.

Run:
  python scripts/check_dataset_registration.py            # exit 1 if any miss
  python scripts/check_dataset_registration.py --list     # report only, exit 0

Needs CAMELOTE_DATA_SUPABASE_URL + CAMELOTE_DATA_SUPABASE_SERVICE_KEY. If they are
absent (fork PRs have no secrets) the check SKIPS rather than fails -- a guard that
blocks forks would just get disabled.

ALLOWLIST is for genuine non-acquisition jobs: things that orchestrate, back up,
lint, or backfill. A job qualifies only if it does not ingest data that anyone would
ever ask "is this fresh?" about. If it writes rows a human might check the freshness
of, it needs a datasets row -- not an allowlist entry.
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"

ALLOWLIST: dict[str, str] = {
    "watchdog.yml":                        "utility: re-triggers other workflows; ingests nothing",
    "repo_backups.yml":                    "utility: DR snapshots to R2",
    "lint-workflows.yml":                  "the linter itself",
    "refresh_dataset_counts.yml":          "utility: WRITES datasets.record_count for every other parser",
    "blog_generate.yml":                   "content generation, not data acquisition",
    "blog-calendar-orchestrator.yml":      "utility: orchestrates blog runs",
    "forum_seed.yml":                      "content seeding, not data acquisition",
    "lolla_promote.yml":                   "content promotion, not data acquisition",
    "fao_multi_backfill.yml":              "one-time historical backfill",
    "re-llm-classification-backfill.yml":  "one-time classification backfill",
}

SCHEDULE_RE = re.compile(r"^\s*-\s*cron:", re.MULTILINE)


def scheduled_workflows() -> list[str]:
    out = []
    for f in sorted(WORKFLOWS.glob("*.yml")):
        if SCHEDULE_RE.search(f.read_text(encoding="utf-8", errors="replace")):
            out.append(f.name)
    return out


def fetch_registry() -> list[dict] | None:
    url = os.environ.get("CAMELOTE_DATA_SUPABASE_URL")
    key = os.environ.get("CAMELOTE_DATA_SUPABASE_SERVICE_KEY")
    if not (url and key):
        return None
    req = urllib.request.Request(
        f"{url}/rest/v1/datasets?select=code,workflow_file,status,target_schema,target_table&workflow_file=not.is.null",
        headers={"apikey": key, "Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def main() -> int:
    report_only = "--list" in sys.argv
    rows = fetch_registry()
    if rows is None:
        print("SKIP: no command-center credentials in env (fork PR?) — registration not checked.")
        return 0

    by_wf: dict[str, list[dict]] = {}
    for r in rows:
        by_wf.setdefault((r.get("workflow_file") or "").strip(), []).append(r)

    unregistered: list[str] = []
    hidden: list[str] = []

    for wf in scheduled_workflows():
        if wf in ALLOWLIST:
            continue
        entries = by_wf.get(wf, [])
        if not entries:
            unregistered.append(wf)
            continue
        # mode 2: every row for a live workflow is deprecated AND has no target
        live = [e for e in entries
                if not (e.get("status") == "deprecated"
                        and not (e.get("target_schema") and e.get("target_table")))]
        if not live:
            codes = ", ".join(e.get("code", "?") for e in entries)
            hidden.append(f"{wf}  (rows exist but all deprecated with empty targets: {codes})")

    print(f"scheduled workflows checked: {len(scheduled_workflows()) - len(ALLOWLIST)}")
    print(f"UNREGISTERED (no datasets row): {len(unregistered)}")
    for w in unregistered:
        print(f"  - {w}")
    print(f"HIDDEN (deprecated + empty target while live): {len(hidden)}")
    for w in hidden:
        print(f"  - {w}")

    if (unregistered or hidden) and not report_only:
        print()
        print("FAIL: a scheduled parser that is not registered is invisible on the")
        print("monitoring page and is skipped by every health sweep.")
        print("Fix: INSERT a public.datasets row (code, status=active, frequency,")
        print("expected_days_between_updates, delay_threshold_days, target_schema,")
        print("target_table, workflow_file, startup_id) + a dataset_destinations row.")
        print("Derive the values from the real destination table — do not guess them.")
        print("If the workflow is genuinely not an acquisition parser, add it to")
        print("ALLOWLIST in this script with a justification.")
        return 1

    print("\nPASS: every scheduled acquisition workflow is registered and visible.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
