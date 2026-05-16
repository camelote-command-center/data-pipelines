#!/usr/bin/env python3
"""
Lint: every parser workflow must PATCH camelote_data.datasets so the
dashboard knows it ran.

Failing this check prevents the silent-success bug class:
  - bug 66298402: no PATCH at all → dashboard stuck yellow → watchdog
    re-triggers in an infinite loop
  - bug 07bf060b: `if: always()` PATCH → failed runs look successful

A workflow passes if EITHER:
  (a) it contains a `rest/v1/datasets?(code|workflow_file)=eq.` PATCH, OR
  (b) it's in the ALLOWLIST below (utility / backfill / no dataset row).

Run:
  python scripts/check_workflow_patches.py            # exit 1 if any miss
  python scripts/check_workflow_patches.py --list     # just list status

Used by:
  .github/workflows/lint-workflows.yml  (runs on every PR)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS_DIR = ROOT / ".github" / "workflows"

# Workflows that legitimately don't need a PATCH step. Justifications inline.
ALLOWLIST: dict[str, str] = {
    "watchdog.yml":                  "utility: re-triggers other workflows; not a parser",
    "repo_backups.yml":              "utility: DR snapshots → R2; no dataset row",
    "fao_multi_backfill.yml":        "one-time historical backfill; PATCH would lie about freshness",
    "re-llm-classification-backfill.yml": "one-time classification backfill",
    "blog-calendar-orchestrator.yml":     "utility: orchestrates blog runs",
    "lint-workflows.yml":            "this linter itself",
}

# Patterns that indicate a PATCH is present (workflow-level)
PATCH_PATTERN = re.compile(
    r"rest/v1/datasets\?(code|workflow_file)=eq",
    re.MULTILINE,
)
# Pattern that flags the dangerous if: always() PATCH (bug 07bf060b)
DANGEROUS_PATTERN = re.compile(
    r"if:\s*always\(\)[^\n]*\n[^\n]*\n[^\n]*rest/v1/datasets",
    re.MULTILINE | re.DOTALL,
)


def lint_workflow(path: Path) -> tuple[str, str]:
    """Return ('ok'|'missing'|'dangerous'|'allowlisted', detail)."""
    name = path.name
    content = path.read_text()
    if name in ALLOWLIST:
        return ("allowlisted", ALLOWLIST[name])
    if DANGEROUS_PATTERN.search(content):
        return ("dangerous", "uses `if: always()` PATCH — bug 07bf060b masks failures; split into success/failure")
    if not PATCH_PATTERN.search(content):
        return ("missing", "no `rest/v1/datasets?...=eq.<name>` PATCH step found")
    return ("ok", "")


def main() -> int:
    list_only = "--list" in sys.argv
    results: dict[str, list[tuple[str, str]]] = {
        "ok": [], "allowlisted": [], "missing": [], "dangerous": [],
    }
    for p in sorted(WORKFLOWS_DIR.glob("*.yml")):
        status, detail = lint_workflow(p)
        results[status].append((p.name, detail))

    # Summary
    print(f"OK:           {len(results['ok'])}")
    print(f"Allowlisted:  {len(results['allowlisted'])}")
    print(f"MISSING:      {len(results['missing'])}")
    print(f"DANGEROUS:    {len(results['dangerous'])}")
    print()

    if results["missing"]:
        print("MISSING (must add success/failure PATCH or add to ALLOWLIST):")
        for name, _ in results["missing"]:
            print(f"  - {name}")
        print()

    if results["dangerous"]:
        print("DANGEROUS (uses `if: always()` PATCH — split into success/failure):")
        for name, detail in results["dangerous"]:
            print(f"  - {name}: {detail}")
        print()

    if list_only:
        print("ALLOWLISTED:")
        for name, reason in results["allowlisted"]:
            print(f"  - {name}: {reason}")
        print()
        return 0

    # Fail CI if any workflow is broken
    if results["missing"] or results["dangerous"]:
        print("FAIL: every parser workflow must PATCH camelote_data.datasets.")
        print("Add a success/failure PATCH block (see properstar.yml for the canonical pattern),")
        print("or add the file to scripts/check_workflow_patches.py ALLOWLIST with a justification.")
        return 1
    print("PASS: every parser workflow updates the dashboard correctly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
