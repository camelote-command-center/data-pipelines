# re-LLM classification backfill

Idempotent batch-classifies any rows in `knowledge_*.{entries,documents,chunks}` on
re-LLM with `categorization_status='pending'`. Catches:

- pg_net timeouts / queue overflows from the live `classify_on_insert` triggers
- Cross-axis trigger rejections (model put `retail` in `topics`, etc.)
- Anything bulk-imported under the bulk-import-discipline DISABLE/ENABLE pattern

Submits a single Anthropic Message Batch (Sonnet 4.6, 50% discount), polls until
done, ingests results. Re-runs are safe — only `pending` rows are picked up.

## Schedule
Daily 04:30 UTC via `.github/workflows/re-llm-classification-backfill.yml`.
Also `workflow_dispatch` for manual runs.

## Local dev
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill in real values from supabase-registry
python3 classify_existing.py
```

## Emergency
If the live triggers misbehave, apply `EMERGENCY_DISABLE.sql` against re-LLM
(disables all 5 `classify_on_insert` triggers in one transaction). Re-enable
by replacing `DISABLE` with `ENABLE`.

## See also
- camelote_data wiki: `operations / bulk-import-discipline`
- lamap_db `platform.standards`: `re_llm_bulk_import_discipline`
