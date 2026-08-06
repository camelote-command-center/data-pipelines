#!/usr/bin/env python3
"""
Emit content-hash DDL for one synced table. Run once per database.

Why trigger-maintained rather than GENERATED ... STORED: a stored generated
column requires an immutable expression and `timestamptz::text` is not immutable
(its output depends on the session TimeZone). A BEFORE trigger sidesteps the
whole class.

Timestamps are normalised to UTC with a fixed format regardless, because the
source hash is computed on re-LLM and the destination hash on lamap_db, in
different sessions whose TimeZone we do not control. The two hashes are only
comparable if they are computed identically.

Column list comes from the procedure's own SET clause, never pg_attribute of the
foreign table. updated_at is excluded: it is the churn column.

Usage:  gen_hash.py <table> <schema> < "col|type" lines
"""

import sys


def cast(col: str, typ: str, prefix: str) -> str:
    q = f'{prefix}"{col}"'
    if typ in ("timestamp with time zone", "timestamptz"):
        return f"to_char({q} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')"
    if typ == "timestamp without time zone":
        return f"to_char({q}, 'YYYY-MM-DD HH24:MI:SS.US')"
    return f"{q}::text"


def expr(cols, prefix: str) -> str:
    parts = ",\n           ".join(
        f"coalesce({cast(c, t, prefix)}, '\\x00NULL')" for c, t in cols
    )
    return f"md5(concat_ws('|',\n           {parts}))"


def emit(table: str, schema: str, cols) -> str:
    fn = f"{table}_content_hash"
    return f"""-- ---- {schema}.{table} : {len(cols)} columns hashed ----
ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS content_hash text;

CREATE OR REPLACE FUNCTION {schema}.{fn}() RETURNS trigger
LANGUAGE plpgsql
IMMUTABLE
AS $fn$
BEGIN
  NEW.content_hash := {expr(cols, 'NEW.')};
  RETURN NEW;
END $fn$;

DROP TRIGGER IF EXISTS {table}_content_hash_trg ON {schema}.{table};
CREATE TRIGGER {table}_content_hash_trg
  BEFORE INSERT OR UPDATE ON {schema}.{table}
  FOR EACH ROW EXECUTE FUNCTION {schema}.{fn}();

-- Backfill every existing row through the trigger.
UPDATE {schema}.{table} SET content_hash = NULL;

CREATE INDEX IF NOT EXISTS {table}_content_hash_idx ON {schema}.{table} (content_hash);
"""


if __name__ == "__main__":
    table, schema = sys.argv[1], sys.argv[2]
    cols = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        c, t = line.split("|", 1)
        cols.append((c.strip(), t.strip()))
    if not cols:
        raise SystemExit("no columns")
    sys.stderr.write(f"{schema}.{table}: hashing {len(cols)} columns\n")
    sys.stdout.write(emit(table, schema, cols))
