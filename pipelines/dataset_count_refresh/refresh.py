#!/usr/bin/env python3
"""
Refresh datasets.record_count on camelote_data with the REAL live row count of each
active parser's target table — so the monitoring dashboard shows real data permanently.

For each active dataset it looks up target_schema.target_table across all registry DBs
(prefers the dataset's own startup DB, falls back to any DB that has the table — this
auto-resolves cross-DB/legacy mislabels), counts it, and writes record_count back.

Registry (single source of truth for all DB creds) is read from REGISTRY_JSON
(default: ./supabase-registry/supabase-projects.json — clone it in CI with BACKUP_GH_PAT).
Counting is EXCEPTION-guarded per table (a missing/renamed table can't abort the batch).
"""
import os, json, sys
import psycopg2

REGISTRY_JSON = os.environ.get("REGISTRY_JSON", "./supabase-registry/supabase-projects.json")
REG = json.load(open(REGISTRY_JSON))

def uri(key):
    e = REG.get(key) or {}
    return e.get("session_pooler_uri")

# startup name -> registry key (preferred DB). Unlisted startups still resolve via fallback search.
STARTUP_DB = {
    "RE-LLM": "re-llm", "Lamap": "lamap-db", "Tinjob": "jobs-ch",
    "BillionairesList": "billionaires-list", "Camelote": "camelote-data", "XOXO": "xoxo",
}
# DBs to search for target tables (only those with a pooler URI in the registry)
SEARCH_DBS = [k for k in ["re-llm", "lamap-db", "camelote-data", "jobs-ch",
                          "billionaires-list", "xoxo"] if uri(k)]

CAM = uri("camelote-data")
if not CAM:
    sys.exit("FATAL: camelote-data session_pooler_uri missing from registry")

GUARDED_CNT = """
SET statement_timeout='240s';
CREATE OR REPLACE FUNCTION pg_temp.cnt(p text[]) RETURNS TABLE(tgt text, n bigint) AS $$
DECLARE x text; sch text; tb text; c bigint;
BEGIN
  FOREACH x IN ARRAY p LOOP
    sch := split_part(x,'|',1); tb := split_part(x,'|',2);
    BEGIN EXECUTE format('SELECT count(*) FROM %I.%I', sch, tb) INTO c;
    EXCEPTION WHEN OTHERS THEN c := -1; END;
    tgt := x; n := c; RETURN NEXT;
  END LOOP;
END$$ LANGUAGE plpgsql;
"""

def main():
    cam = psycopg2.connect(CAM); cam.autocommit = True
    cur = cam.cursor()
    cur.execute("""SELECT d.display_id, s.name, d.target_schema, d.target_table,
                          COALESCE(d.record_count,0)
                   FROM datasets d JOIN startups s ON s.id=d.startup_id
                   WHERE d.status='active'
                     AND d.target_schema IS NOT NULL AND d.target_table IS NOT NULL;""")
    rows = cur.fetchall()
    targets = sorted({(r[2], r[3]) for r in rows})
    arr = [f"{s}|{t}" for s, t in targets]

    # counts[db][schema.table] = n  (only where the table exists, n>=0)
    counts = {}
    for db in SEARCH_DBS:
        try:
            conn = psycopg2.connect(uri(db)); conn.autocommit = True
            c = conn.cursor()
            c.execute(GUARDED_CNT)
            c.execute("SELECT tgt, n FROM pg_temp.cnt(%s);", (arr,))
            m = {}
            for tgt, n in c.fetchall():
                if n is not None and n >= 0:
                    s, t = tgt.split("|", 1); m[f"{s}.{t}"] = n
            counts[db] = m
            conn.close()
            print(f"[{db}] tables found: {len(m)}")
        except Exception as e:
            counts[db] = {}
            print(f"[{db}] SKIPPED: {e}")

    def real_for(startup, key):
        pref = STARTUP_DB.get(startup)
        if pref and key in counts.get(pref, {}): return counts[pref][key]
        for db in SEARCH_DBS:
            if key in counts.get(db, {}): return counts[db][key]
        return None

    fixed = unresolved = 0
    for did, startup, sch, tbl, rc in rows:
        real = real_for(startup, f"{sch}.{tbl}")
        if real is None:
            unresolved += 1; continue
        if real != rc:
            cur.execute("UPDATE datasets SET record_count=%s, updated_at=now() WHERE display_id=%s;",
                        (real, did))
            fixed += 1
    print(f"\nrecord_count refreshed: {fixed} updated, {unresolved} unresolved (target not found in any DB), {len(rows)} active total")
    cam.close()

if __name__ == "__main__":
    main()
