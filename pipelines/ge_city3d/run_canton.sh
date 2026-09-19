#!/bin/bash
# Canton-wide 3D city: one tileset per commune (resumable: a commune with a tileset.json is skipped),
# 4 communes in parallel, one shared tree cache (detections are per cell, clipped per commune afterwards).
set -u
cd "$(dirname "$0")"
OUT=${1:-out/canton}
mkdir -p "$OUT/logs"
PG=${RE_LLM_PG_URI:?RE_LLM_PG_URI not set}
psql "$PG" -tAc "select distinct no_commune from bronze_ch.ge_communes_geo order by 1" > "$OUT/communes.txt"
export TREE_CACHE=${TREE_CACHE:-$OUT/../canton_trees}
mkdir -p "$TREE_CACHE"
build() {
  c=$1
  [ -f "$OUT/$c/tileset.json" ] && { echo "skip $c"; return; }
  python city_commune.py "$c" "$OUT/$c" > "$OUT/logs/$c.log" 2>&1 && echo "done $c" || echo "FAILED $c"
}
export -f build; export OUT
xargs -P 4 -I{} bash -c 'build {}' < "$OUT/communes.txt"
python make_root.py "$OUT"
echo CANTON_DONE
