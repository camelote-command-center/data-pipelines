-- EMERGENCY: disable all classify-on-insert triggers.
-- Use when classify-row is misbehaving (saturating pg_net, hitting Anthropic
-- rate limits, classification mass-rejecting on a schema bug, etc.).
-- Existing rows are untouched. Inserts after this still land 'pending' but
-- no async classification fires.

ALTER TABLE knowledge_ch.entries     DISABLE TRIGGER classify_on_insert;
ALTER TABLE knowledge_global.entries DISABLE TRIGGER classify_on_insert;
ALTER TABLE knowledge_ae.entries     DISABLE TRIGGER classify_on_insert;
ALTER TABLE knowledge_ch.documents   DISABLE TRIGGER classify_on_insert;
ALTER TABLE knowledge_ch.chunks      DISABLE TRIGGER classify_on_insert;

-- To re-enable: replace DISABLE with ENABLE on each line, then backfill any
-- 'pending' rows that landed during the outage:
--   cd ~/work/re-llm-classification && python3 classify_existing.py
