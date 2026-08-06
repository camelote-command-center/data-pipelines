#!/usr/bin/env python3
"""
Re-key a sync procedure from the surrogate `id` onto the business key
(no_comm, no_batiment), on BOTH branches, and replace the timestamp gate with a
full-column content comparison.

The INSERT branch matters as much as the UPDATE. `WHERE NOT EXISTS (... t.id =
s.id)` is what manufactured duplicate buildings: when a re-ingest renumbered a
building's id, the destination had no row with that id and a second copy of an
existing building was inserted. Re-keying only the UPDATE would leave that
generator in place.

`id` is dropped from the SET list as well. Copying the source surrogate over the
destination surrogate is what made the two databases' id spaces look
interchangeable in the first place; the destination keeps its own id, and the
business key is what identifies a row.
"""

import re
import sys

BK = ("no_comm", "no_batiment")
CHURN = {"updated_at"}


def set_columns(defn: str) -> list[str]:
    m = re.search(r"\bUPDATE\s+\S+\s+t\s+SET\b(.*?)\bFROM\b", defn, re.S | re.I)
    if not m:
        raise ValueError("no UPDATE ... t SET ... FROM")
    clause = m.group(1)
    cols = []
    for a in clause.split(","):
        lhs = a.split("=")[0].strip().strip('"')
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", lhs):
            cols.append(lhs)
    expected = len(re.findall(r"=\s*s\.", clause))
    if len(cols) != expected:
        raise ValueError(f"parsed {len(cols)} cols, clause has {expected} assignments")
    return cols


def rekey(defn: str, table: str) -> tuple[str, int]:
    cols = set_columns(defn)

    # The destination keeps its own surrogate; never copy the source's.
    set_cols = [c for c in cols if c != "id"]
    set_clause = ", ".join(f'{c}=s.{c}' for c in set_cols)

    compared = [c for c in set_cols if c not in CHURN]
    t_row = ", ".join(f't."{c}"::text' for c in compared)
    s_row = ", ".join(f's."{c}"::text' for c in compared)

    join = " AND ".join(f"t.{k}=s.{k}" for k in BK)
    new_update = (
        f"UPDATE lamap_db_foreign.{table} t SET {set_clause} "
        f"FROM bronze_ch.{table} s "
        f"WHERE {join} AND (({t_row}) IS DISTINCT FROM ({s_row}));"
    )

    # `id` is NOT NULL with no default on the destination, so a brand-new row
    # still needs one and the source value is as good as any. It stays OUT of
    # the UPDATE SET: an existing destination row must never be renumbered,
    # which is what made the two id spaces look interchangeable to begin with.
    insert_only = ["id"] + set_cols
    insert_cols = ", ".join(insert_only)
    src_cols = ", ".join(f"s.{c}" for c in insert_only)
    not_exists_join = " AND ".join(f"t.{k}=s.{k}" for k in BK)
    new_insert = (
        f"INSERT INTO lamap_db_foreign.{table} ({insert_cols}) "
        f"SELECT {src_cols} FROM bronze_ch.{table} s "
        f"WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.{table} t WHERE {not_exists_join});"
    )

    body = f"BEGIN\n  {new_update}\n  {new_insert}\nEND;"
    out = re.sub(r"BEGIN\b.*END;", body, defn, flags=re.S)
    if "t.id=s.id" in out or "t.id = s.id" in out:
        raise ValueError("surrogate join survived the rewrite")
    return out, len(set_cols)


if __name__ == "__main__":
    table = sys.argv[1]
    new, n = rekey(sys.stdin.read(), table)
    sys.stderr.write(f"{table}: {n} columns maintained, keyed on {BK}\n")
    sys.stdout.write(new)
