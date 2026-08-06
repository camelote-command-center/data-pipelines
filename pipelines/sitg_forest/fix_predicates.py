#!/usr/bin/env python3
"""
Replace the timestamp-gated change predicate in every gold_ch.sync_* procedure
with a content comparison built from that procedure's OWN explicit SET column
list.

Why not derive columns from pg_attribute: platform.standards
/sync_full_refresh_non_destructive_allowlist is explicit that the column set
must never come from the foreign table's catalog, because a column added on one
side then silently joins or leaves the contract. Each procedure's SET clause is
its contract, so the predicate is built from exactly those columns and no
others.

Why ::text on every column: the PostGIS = operator compares bounding boxes, so
two genuinely different geometries with the same envelope compare equal. A
geometry change would not fire the predicate. Casting to text compares the
actual WKB.

Why updated_at is excluded from the comparison but kept in SET: it is the churn
column. If the ingest stamps it on every run, including it in the predicate
rewrites every row on every sync. This mirrors the rule already applied to
sync_full_refresh Branch A.
"""

import re
import sys

TS_PRED = "(t.updated_at IS DISTINCT FROM s.updated_at)"
CHURN = {"updated_at"}


def columns_from_set(body: str) -> list[str]:
    """
    Pull the column names out of the UPDATE ... SET clause.

    Anchored on the UPDATE statement, NOT on the first SET in the definition:
    the procedure header carries `SET search_path TO ...` and `SET
    statement_timeout TO ...` before the body ever starts. Anchoring on the
    first SET swallowed the header text into the first comma-separated element
    and silently dropped the FIRST column of the real SET clause. That is the
    same class of silent omission this whole audit exists to remove, so the
    parse is now anchored and then verified against an independent count.
    """
    m = re.search(r"\bUPDATE\s+\S+\s+t\s+SET\b(.*?)\bFROM\b", body, re.S | re.I)
    if not m:
        raise ValueError("no UPDATE ... t SET ... FROM found")
    clause = m.group(1)

    cols = []
    for assign in clause.split(","):
        lhs = assign.split("=")[0].strip().strip('"')
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", lhs):
            cols.append(lhs)

    # Independent count: every assignment in the clause is "<lhs>=s.<rhs>".
    expected = len(re.findall(r"=\s*s\.", clause))
    if len(cols) != expected:
        raise ValueError(
            f"parsed {len(cols)} columns but the SET clause has {expected} "
            f"assignments; refusing to build a predicate over a partial column list"
        )
    if not cols:
        raise ValueError("no columns parsed from SET")
    return cols


def build_predicate(cols: list[str]) -> str:
    compared = [c for c in cols if c not in CHURN]
    if not compared:
        raise ValueError("every column was excluded as churn")
    t = ", ".join(f't."{c}"::text' for c in compared)
    s = ", ".join(f's."{c}"::text' for c in compared)
    return f"(({t}) IS DISTINCT FROM ({s}))"


def transform(defn: str) -> tuple[str, int]:
    """
    Both spellings occur in the wild: the predicate is parenthesised in most of
    the procedures and bare in sync_ge_cad_adresses. Match on the expression
    itself rather than on the punctuation around it.
    """
    pattern = re.compile(
        r"\(?\s*t\.updated_at\s+IS\s+DISTINCT\s+FROM\s+s\.updated_at\s*\)?",
        re.I,
    )
    if not pattern.search(defn):
        raise ValueError("timestamp predicate not found")

    cols = columns_from_set(defn)
    pred = build_predicate(cols)
    out, n = pattern.subn(pred, defn)
    if n != 1:
        raise ValueError(f"expected exactly 1 predicate, replaced {n}")
    return out, len(cols)


if __name__ == "__main__":
    src = sys.stdin.read()
    new, n = transform(src)
    sys.stderr.write(f"columns in SET: {n}\n")
    sys.stdout.write(new)
