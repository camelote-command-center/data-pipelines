#!/usr/bin/env python3
"""
Quarterly gate: has swisstopo actually published new source data?

WHY THIS EXISTS
    swissSURFACE3D and swissALTI3D refresh roughly yearly, on offset cycles.
    A quarterly CHM rebuild would therefore recompute identical inputs three
    times out of four -- and because the R2 bucket is write-once, each of those
    no-change rebuilds would still need its own version prefix and write another
    full copy of 357 COG tiles.

    So the cadence is quarterly but the WORK is conditional: every quarter we
    ask swisstopo whether anything moved, and only rebuild if it did. The board
    then shows "checked, no change" rather than either silence or a fake
    refresh. A schedule that always runs is not evidence of fresh data; a
    schedule that checks and reports is.

HOW
    The STAC index for each collection is already fetched by prepare_inputs.py.
    This reads the same endpoint and reduces the GE-relevant items to one
    fingerprint: the sorted (item id, checksum-or-datetime) pairs, hashed. Any
    new, removed or re-published tile changes it.

    The previous fingerprint lives in gold_ch.chm_source_fingerprint, because
    GitHub Actions carries no state between scheduled runs.

EXIT / OUTPUT
    Writes `changed=true|false` and `version=<YYYY-Qn>` to $GITHUB_OUTPUT.
    Always exits 0 for a successful check -- "no change" is a normal outcome,
    not a failure. Exits non-zero only if STAC itself is unreachable, which is a
    real problem worth failing on.

Environment:
    RE_LLM_PG_URI   re-LLM connection string
    FORCE=true      record the fingerprint but report changed=true regardless
"""
import hashlib
import json
import os
import subprocess
import sys
import urllib.request
from datetime import date, timezone, datetime

COLLECTIONS = {
    "dsm": "ch.swisstopo.swisssurface3d-raster",
    "dtm": "ch.swisstopo.swissalti3d",
}
# GE bounding box in WGS84, generous margin. Keeps the fingerprint to tiles that
# could affect this canton, so a release touching only eastern Switzerland does
# not trigger a pointless GE rebuild.
BBOX = "5.9,46.1,6.35,46.4"


def stac_items(collection: str) -> list:
    url = (f"https://data.geo.admin.ch/api/stac/v1/collections/{collection}"
           f"/items?bbox={BBOX}&limit=100")
    items, guard = [], 0
    while url and guard < 200:
        guard += 1
        with urllib.request.urlopen(url, timeout=120) as r:
            d = json.load(r)
        items.extend(d.get("features") or [])
        url = next((l["href"] for l in (d.get("links") or [])
                    if l.get("rel") == "next"), None)
    return items


def fingerprint(items: list) -> str:
    parts = []
    for it in items:
        ident = it.get("id", "")
        # Prefer a content checksum; fall back to the publication datetime.
        stamp = ""
        for a in (it.get("assets") or {}).values():
            stamp = a.get("checksum:multihash") or a.get("file:checksum") or ""
            if stamp:
                break
        if not stamp:
            stamp = (it.get("properties") or {}).get("datetime", "")
        parts.append(f"{ident}|{stamp}")
    return hashlib.md5("\n".join(sorted(parts)).encode()).hexdigest()


def psql(pg: str, sql: str) -> str:
    r = subprocess.run(["psql", pg, "-v", "ON_ERROR_STOP=1", "-q", "-tA"],
                       input=sql, text=True, capture_output=True)
    if r.returncode != 0:
        print(r.stderr[:2000], file=sys.stderr)
        raise SystemExit(f"psql failed ({r.returncode})")
    return r.stdout.strip()


def main() -> int:
    pg = os.environ.get("RE_LLM_PG_URI")
    if not pg:
        print("RE_LLM_PG_URI not set", file=sys.stderr)
        return 1

    psql(pg, """
CREATE TABLE IF NOT EXISTS gold_ch.chm_source_fingerprint (
  collection   text PRIMARY KEY,
  fingerprint  text NOT NULL,
  item_count   int  NOT NULL,
  checked_at   timestamptz NOT NULL DEFAULT now(),
  changed_at   timestamptz
);
COMMENT ON TABLE gold_ch.chm_source_fingerprint IS
  'Last-seen swisstopo STAC fingerprint per collection, so the quarterly CHM '
  'chain can tell "source moved" from "checked, nothing to do". GitHub Actions '
  'keeps no state between scheduled runs, so it lives here.';
""")

    changed = os.environ.get("FORCE", "").lower() == "true"
    report = []
    for key, coll in COLLECTIONS.items():
        try:
            items = stac_items(coll)
        except Exception as e:  # noqa: BLE001
            print(f"STAC unreachable for {coll}: {e}", file=sys.stderr)
            return 1
        if not items:
            print(f"STAC returned 0 items for {coll} — refusing to call that "
                  f"'no change', it is more likely a query or bbox fault",
                  file=sys.stderr)
            return 1
        fp = fingerprint(items)
        prev = psql(pg, f"SELECT fingerprint FROM gold_ch.chm_source_fingerprint "
                        f"WHERE collection = '{coll}';")
        moved = (prev != fp)
        changed = changed or moved
        report.append(f"  {key:3} {coll}: {len(items):4} GE items  "
                      f"{'CHANGED' if moved else 'unchanged'}"
                      f"{'' if prev else '  (first run, no baseline)'}")
        psql(pg, f"""
INSERT INTO gold_ch.chm_source_fingerprint (collection, fingerprint, item_count, checked_at, changed_at)
VALUES ('{coll}', '{fp}', {len(items)}, now(), {'now()' if moved else 'NULL'})
ON CONFLICT (collection) DO UPDATE SET
  fingerprint = EXCLUDED.fingerprint,
  item_count  = EXCLUDED.item_count,
  checked_at  = now(),
  changed_at  = CASE WHEN gold_ch.chm_source_fingerprint.fingerprint
                          IS DISTINCT FROM EXCLUDED.fingerprint
                     THEN now() ELSE gold_ch.chm_source_fingerprint.changed_at END;
""")

    print("\n".join(report))
    print(f"  => changed={str(changed).lower()}")

    today = date.today()
    version = f"chm-{today.year}-q{(today.month - 1)//3 + 1}"
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as fh:
            fh.write(f"changed={str(changed).lower()}\n")
            fh.write(f"version={version}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
