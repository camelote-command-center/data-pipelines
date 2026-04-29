#!/usr/bin/env python3
"""
Parallel classification via the classify-row edge function.

USE CASE
  One-shot backfill of pending knowledge_ch documents through the production
  classify-row edge function (DeepSeek-primary + Sonnet-fallback routing).
  Use this instead of classify_existing.py when you want the new routing —
  classify_existing.py hits Anthropic directly with v1 schema and is obsolete.

USAGE
  export SUPABASE_RE_LLM_URL=https://znrvddgmczdqoucmykij.supabase.co
  export SUPABASE_RE_LLM_SERVICE_KEY=...
  python3 parallel_classify_via_edge.py <ids_file> [workers=10]

  ids_file: one row UUID per line. Get it from:
    psql "$DATABASE_URL" -A -t -c "
      SELECT id FROM knowledge_ch.documents
      WHERE categorization_status='pending' AND created_at >= '<date>';
    " > pending.txt

PROVENANCE
  Built ad-hoc on 2026-04-29 to clear the 27,336-doc spike from the
  court-decisions ingestion incident. Took 109min at 20 workers; cost ~$5
  total ($2.80 DeepSeek + $1.92 Sonnet fallback). Connection-reset bursts
  occurred ~2x during the run; transient and self-healed.
"""
import sys
import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib import request, error

URL = f"{os.environ['SUPABASE_RE_LLM_URL']}/functions/v1/classify-row"
SERVICE_KEY = os.environ['SUPABASE_RE_LLM_SERVICE_KEY']


def classify_one(row_id: str) -> dict:
    payload = json.dumps({
        "schema": "knowledge_ch",
        "table": "documents",
        "row_id": row_id,
    }).encode()
    req = request.Request(
        URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
            return {"id": row_id, **data}
    except error.HTTPError as e:
        return {"id": row_id, "ok": False, "error": f"HTTP {e.code}: {e.read().decode()[:100]}"}
    except Exception as e:
        return {"id": row_id, "ok": False, "error": f"{type(e).__name__}: {e}"}


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    ids_file = sys.argv[1]
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    with open(ids_file) as f:
        ids = [line.strip() for line in f if line.strip()]

    print(f"Processing {len(ids)} docs with {workers} workers", file=sys.stderr)
    start = time.time()
    counts = {
        "deepseek_auto": 0, "deepseek_review": 0,
        "sonnet_auto": 0, "sonnet_review": 0,
        "deferred": 0, "error": 0, "skipped": 0,
    }
    last_report = start

    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = {exe.submit(classify_one, rid): rid for rid in ids}
        for i, fut in enumerate(as_completed(futures), 1):
            r = fut.result()
            if r.get("ok"):
                if r.get("skipped"):
                    counts["skipped"] += 1
                else:
                    model = r.get("model", "?")
                    status = r.get("status", "?")
                    key = f"{'deepseek' if 'deepseek' in model else 'sonnet'}_{'auto' if status == 'auto' else 'review'}"
                    counts[key] = counts.get(key, 0) + 1
            elif r.get("deferred"):
                counts["deferred"] += 1
            else:
                counts["error"] += 1
                print(f"  ERR {r['id'][:8]}: {r.get('error', r.get('reason', '?'))}", file=sys.stderr)

            now = time.time()
            if now - last_report >= 10 or i == len(ids):
                rate = i / (now - start)
                eta_min = (len(ids) - i) / rate / 60 if rate > 0 else 0
                print(f"[{i}/{len(ids)} ({i*100//len(ids)}%) {rate:.1f}/s ETA {eta_min:.1f}min] {counts}", file=sys.stderr)
                last_report = now

    elapsed = time.time() - start
    print(f"\nDone in {elapsed/60:.1f}min. Final: {counts}", file=sys.stderr)


if __name__ == "__main__":
    main()
