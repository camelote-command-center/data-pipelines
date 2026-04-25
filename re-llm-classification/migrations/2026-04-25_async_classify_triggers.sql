-- ============================================================================
-- Async classification triggers for knowledge_*.{entries,documents,chunks}
--
-- Fires AFTER INSERT (only) via pg_net.http_post → /functions/v1/classify-row.
-- The edge function fetches the row, calls Anthropic, UPDATEs back. Failures
-- (API errors, trigger rejections) leave the row 'pending' for batch backfill.
--
-- ⚠️  BULK IMPORT DISCIPLINE  ⚠️
-- These triggers fire pg_net.http_post per row. A bulk INSERT of 8,000 rows
-- with the trigger active will queue 8,000 HTTP calls — saturates pg_net,
-- exhausts Anthropic rate limits, and (per camelote_data startup-rule)
-- "bulk simultaneous edge function calls on file upload can saturate DB
-- connections, leaving status permanently stuck at processing."
--
-- Before any bulk INSERT (>50 rows) into knowledge_ch.{entries,documents,chunks},
-- knowledge_global.entries, or knowledge_ae.entries:
--
--     ALTER TABLE <schema>.<table> DISABLE TRIGGER classify_on_insert;
--     -- ... bulk INSERT (rows land with categorization_status='pending') ...
--     ALTER TABLE <schema>.<table> ENABLE TRIGGER classify_on_insert;
--
-- Then run the batch script to classify the freshly imported rows:
--     cd ~/work/re-llm-classification && python3 classify_existing.py
--
-- Exclusions (no trigger): knowledge_ch.zone_rules, knowledge_ch.construction_costs,
-- knowledge_ch.pdcom_communes — file 01 hardcoded their topics/asset_classes by
-- table identity. They never need per-row classification.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- Shared trigger function
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION knowledge_global.fire_classify_row()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
BEGIN
  PERFORM net.http_post(
    url := 'https://znrvddgmczdqoucmykij.supabase.co/functions/v1/classify-row',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpucnZkZGdtY3pkcW91Y215a2lqIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzQwMjAzMywiZXhwIjoyMDg4OTc4MDMzfQ.9FcdsWV4dAtxy737QEgJYeoaduOHXhuAhKR90TelCTY'
    ),
    body := jsonb_build_object(
      'schema', TG_TABLE_SCHEMA,
      'table',  TG_TABLE_NAME,
      'row_id', NEW.id
    ),
    -- pg_net default is 5s, but classify-row takes ~6s (Anthropic ~3s + 2 DB UPDATEs).
    -- Without this, every successful call logs a fake timeout error.
    timeout_milliseconds := 30000
  );
  RETURN NEW;
END;
$$;

COMMENT ON FUNCTION knowledge_global.fire_classify_row() IS
  'Async classification dispatcher. Posts {schema,table,row_id} to /functions/v1/classify-row. '
  'SECURITY DEFINER so any inserter (anon, authenticated, service_role) can fire pg_net. '
  'BULK IMPORT DISCIPLINE: see comments on triggers using this function.';

-- ----------------------------------------------------------------------------
-- Triggers
-- AFTER INSERT only. WHEN guard skips bulk imports that explicitly set a
-- non-pending status (defense in depth alongside DISABLE TRIGGER).
-- ----------------------------------------------------------------------------

-- knowledge_ch.entries
DROP TRIGGER IF EXISTS classify_on_insert ON knowledge_ch.entries;
CREATE TRIGGER classify_on_insert
  AFTER INSERT ON knowledge_ch.entries
  FOR EACH ROW
  WHEN (NEW.categorization_status IS NULL OR NEW.categorization_status = 'pending')
  EXECUTE FUNCTION knowledge_global.fire_classify_row();

COMMENT ON TRIGGER classify_on_insert ON knowledge_ch.entries IS
  'Async classify via pg_net → /functions/v1/classify-row. '
  'BULK IMPORT DISCIPLINE: ALTER TABLE knowledge_ch.entries DISABLE TRIGGER classify_on_insert before bulk loads, '
  'then ENABLE and run classify_existing.py to backfill.';

-- knowledge_global.entries
DROP TRIGGER IF EXISTS classify_on_insert ON knowledge_global.entries;
CREATE TRIGGER classify_on_insert
  AFTER INSERT ON knowledge_global.entries
  FOR EACH ROW
  WHEN (NEW.categorization_status IS NULL OR NEW.categorization_status = 'pending')
  EXECUTE FUNCTION knowledge_global.fire_classify_row();

COMMENT ON TRIGGER classify_on_insert ON knowledge_global.entries IS
  'Async classify via pg_net → /functions/v1/classify-row. '
  'BULK IMPORT DISCIPLINE: ALTER TABLE knowledge_global.entries DISABLE TRIGGER classify_on_insert before bulk loads, '
  'then ENABLE and run classify_existing.py to backfill.';

-- knowledge_ae.entries
DROP TRIGGER IF EXISTS classify_on_insert ON knowledge_ae.entries;
CREATE TRIGGER classify_on_insert
  AFTER INSERT ON knowledge_ae.entries
  FOR EACH ROW
  WHEN (NEW.categorization_status IS NULL OR NEW.categorization_status = 'pending')
  EXECUTE FUNCTION knowledge_global.fire_classify_row();

COMMENT ON TRIGGER classify_on_insert ON knowledge_ae.entries IS
  'Async classify via pg_net → /functions/v1/classify-row. '
  'BULK IMPORT DISCIPLINE: ALTER TABLE knowledge_ae.entries DISABLE TRIGGER classify_on_insert before bulk loads, '
  'then ENABLE and run classify_existing.py to backfill.';

-- knowledge_ch.documents
DROP TRIGGER IF EXISTS classify_on_insert ON knowledge_ch.documents;
CREATE TRIGGER classify_on_insert
  AFTER INSERT ON knowledge_ch.documents
  FOR EACH ROW
  WHEN (NEW.categorization_status IS NULL OR NEW.categorization_status = 'pending')
  EXECUTE FUNCTION knowledge_global.fire_classify_row();

COMMENT ON TRIGGER classify_on_insert ON knowledge_ch.documents IS
  'Async classify via pg_net → /functions/v1/classify-row. '
  'BULK IMPORT DISCIPLINE: ALTER TABLE knowledge_ch.documents DISABLE TRIGGER classify_on_insert before bulk loads, '
  'then ENABLE and run classify_existing.py to backfill.';

-- knowledge_ch.chunks
DROP TRIGGER IF EXISTS classify_on_insert ON knowledge_ch.chunks;
CREATE TRIGGER classify_on_insert
  AFTER INSERT ON knowledge_ch.chunks
  FOR EACH ROW
  WHEN (NEW.categorization_status IS NULL OR NEW.categorization_status = 'pending')
  EXECUTE FUNCTION knowledge_global.fire_classify_row();

COMMENT ON TRIGGER classify_on_insert ON knowledge_ch.chunks IS
  'Async classify via pg_net → /functions/v1/classify-row. '
  'BULK IMPORT DISCIPLINE: ALTER TABLE knowledge_ch.chunks DISABLE TRIGGER classify_on_insert before bulk loads, '
  'then ENABLE and run classify_existing.py to backfill. '
  'A 100-chunk PDF ingest fires 100 pg_net calls — fine; an 8,000-chunk migration would saturate.';

COMMIT;

-- ----------------------------------------------------------------------------
-- Verification (run after applying)
-- ----------------------------------------------------------------------------
-- SELECT n.nspname, c.relname, t.tgname, pg_get_triggerdef(t.oid)
-- FROM pg_trigger t
-- JOIN pg_class c ON c.oid = t.tgrelid
-- JOIN pg_namespace n ON n.oid = c.relnamespace
-- WHERE t.tgname = 'classify_on_insert' AND NOT t.tgisinternal
-- ORDER BY 1, 2;
