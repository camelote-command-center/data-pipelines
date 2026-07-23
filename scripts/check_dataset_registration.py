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

  3. a workflow that writes N target tables but has fewer than N registered datasets.
     gwr.yml hit exactly this: it passed on its building row while the dwelling file
     (bronze_ch.bfs_rebl_housing, 5.3M rows) had no datasets row at all. Checking per
     WORKFLOW cannot see that; the correct unit is per TARGET TABLE.

This guard catches ALL THREE modes.

HOW TARGETS ARE ENUMERATED — and why. Parsing `TABLE = "..."` constants out of pipeline
modules is brittle: a parser that builds table names dynamically, or renames a constant,
would silently produce a FALSE PASS, which is precisely the failure we are fixing. So
targets are DECLARED, not inferred: a pipeline ships `pipelines/<dir>/datasets.json`

    {"workflow": "gwr.yml",
     "datasets": [{"code": "...", "schema": "bronze_ch", "table": "..."}, ...]}

and every declared code must have a `datasets` row. A declaration the guard can read
beats an inference it can get wrong.

Pipelines without a manifest fall back to the per-workflow check (at least one row) and
are COUNTED AND REPORTED as undeclared, so the migration is visible without failing 59
workflows at once.

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
PIPELINES = ROOT / "pipelines"


def load_manifests() -> dict[str, list[dict]]:
    """workflow filename -> declared datasets, from pipelines/*/datasets.json."""
    out: dict[str, list[dict]] = {}
    for mf in sorted(PIPELINES.glob("*/datasets.json")):
        try:
            data = json.loads(mf.read_text())
        except Exception as e:  # noqa: BLE001
            print(f"  WARNING: unreadable manifest {mf}: {e}")
            continue
        # a file may declare ONE workflow (object) or SEVERAL (list) — several
        # workflows can share a pipeline directory, e.g. fao_multi + fao_multi_quarterly.
        blocks = data if isinstance(data, list) else [data]
        for b in blocks:
            wf = (b.get("workflow") or "").strip()
            ds = b.get("datasets") or []
            if wf and ds:
                out.setdefault(wf, []).extend(ds)
    return out


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
        f"{url}/rest/v1/datasets?select=code,workflow_file,status,target_schema,target_table&limit=5000",
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
    by_code: dict[str, dict] = {}
    for r in rows:
        wf = (r.get("workflow_file") or "").strip()
        if wf:
            by_wf.setdefault(wf, []).append(r)
        by_code[(r.get("code") or "").strip()] = r

    manifests = load_manifests()
    unregistered: list[str] = []       # mode 1
    hidden: list[str] = []             # mode 2
    missing_targets: list[str] = []    # mode 3 (per-target)
    undeclared: list[str] = []         # no manifest -> legacy per-workflow check only

    def is_hidden(e: dict) -> bool:
        return (e.get("status") == "deprecated"
                and not (e.get("target_schema") and e.get("target_table")))

    checked = 0
    for wf in scheduled_workflows():
        if wf in ALLOWLIST:
            continue
        checked += 1
        declared = manifests.get(wf)

        if declared:
            # PER-TARGET: every declared target table must have its own datasets row.
            for d in declared:
                code = (d.get("code") or "").strip()
                tgt = f"{d.get('schema','?')}.{d.get('table','?')}"
                row = by_code.get(code)
                if not row:
                    missing_targets.append(f"{wf} -> {tgt}: no datasets row for code '{code}'")
                elif is_hidden(row):
                    missing_targets.append(f"{wf} -> {tgt}: '{code}' is deprecated with an empty target")
            continue

        undeclared.append(wf)
        entries = by_wf.get(wf, [])
        if not entries:
            unregistered.append(wf)
            continue
        if not [e for e in entries if not is_hidden(e)]:
            codes = ", ".join(e.get("code", "?") for e in entries)
            hidden.append(f"{wf}  (rows exist but all deprecated with empty targets: {codes})")

    print(f"scheduled workflows checked: {checked}  "
          f"(manifest-declared: {checked - len(undeclared)}, legacy per-workflow: {len(undeclared)})")
    print(f"UNREGISTERED (no datasets row): {len(unregistered)}")
    for w in unregistered:
        print(f"  - {w}")
    print(f"HIDDEN (deprecated + empty target while live): {len(hidden)}")
    for w in hidden:
        print(f"  - {w}")
    print(f"MISSING TARGET REGISTRATION (declared target with no/hidden dataset): {len(missing_targets)}")
    for w in missing_targets:
        print(f"  - {w}")

    if (unregistered or hidden or missing_targets) and not report_only:
        print()
        print("FAIL: a scheduled parser target that is not registered is invisible on the")
        print("monitoring page and is skipped by every health sweep.")
        print("Fix: INSERT a public.datasets row per TARGET TABLE (code, status=active,")
        print("frequency, expected_days_between_updates, delay_threshold_days,")
        print("target_schema, target_table, startup_id) + a dataset_destinations row.")
        print("Derive the values from the real destination table — do not guess them.")
        print("A workflow writing N tables needs N datasets rows; declare them in")
        print("pipelines/<dir>/datasets.json so this guard can see all of them.")
        print("If the workflow is genuinely not an acquisition parser, add it to ALLOWLIST.")
        return 1

    print("\nPASS: every scheduled acquisition target is registered and visible.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
