-- Atomic dedup for the categorization review queue.
-- Prevents two concurrent pg_net retries of classify-row from both inserting
-- a 'pending' review row for the same source. INSERT ON CONFLICT (or 23505
-- swallow on raw INSERT) lets the loser cleanly no-op.
-- Apply BEFORE triggers.sql.

CREATE UNIQUE INDEX IF NOT EXISTS uniq_review_pending_per_source
  ON knowledge_global.categorization_review (source_schema, source_table, source_id)
  WHERE status = 'pending';
