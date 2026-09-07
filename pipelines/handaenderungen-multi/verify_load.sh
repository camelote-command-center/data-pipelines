#!/usr/bin/env bash
# Live load-path verification against re-LLM + lamap_db.
# Reads credentials from ~/supabase-registry (never hardcode). Requires: node, the
# pipeline's node_modules (npm install), and the _shared modules resolvable via NODE_PATH.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REG="${SUPABASE_REGISTRY:-$HOME/supabase-registry}/supabase-projects.json"

eval "$(python3 - "$REG" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
r, l = d['re-llm'], d['lamap-db']
print(f'export RE_LLM_SUPABASE_URL={r["url"]}')
print(f'export RE_LLM_SUPABASE_SERVICE_ROLE_KEY={r["service_role_key"]}')
print(f'export SUPABASE_URL={l["url"]}')
print(f'export SUPABASE_SERVICE_ROLE_KEY={l["service_role_key"]}')
print(f'export LAMAP_DB_URI={l["session_pooler_uri"]}')
PY
)"

cd "$DIR"
NODE_PATH="$DIR/node_modules" ./node_modules/.bin/tsx tests/load.live.test.ts
