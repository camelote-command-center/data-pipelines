-- court-decisions — Swiss judicial decisions filtered to RE-relevant chambers.
-- Source: entscheidsuche.ch (Elasticsearch endpoint /_searchV2.php).
-- Articles land in knowledge_ch.documents; this index is for dedup only.
--
-- Rollback: DROP TABLE IF EXISTS bronze_ch.court_decisions_index;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.court_decisions_index (
  id                  BIGSERIAL PRIMARY KEY,
  decision_id         TEXT NOT NULL,        -- entscheidsuche stable ID, e.g. "GE_CJ_004_..._2024-03-15"
  canton              TEXT NOT NULL,        -- e.g. 'GE'
  court               TEXT NOT NULL,        -- e.g. 'GE_CJ' (cour de justice)
  kammer              TEXT NOT NULL,        -- e.g. 'GE_CJ_004' (chambre des baux et loyers)
  decision_date       DATE,
  document_id         UUID,                 -- → knowledge_ch.documents.id
  fetch_status        TEXT NOT NULL DEFAULT 'pending',
  fetch_error         TEXT,
  fetched_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT court_decisions_index_decision_unique UNIQUE (decision_id)
);

CREATE INDEX IF NOT EXISTS idx_court_idx_kammer ON bronze_ch.court_decisions_index (kammer);
CREATE INDEX IF NOT EXISTS idx_court_idx_canton ON bronze_ch.court_decisions_index (canton);
CREATE INDEX IF NOT EXISTS idx_court_idx_date ON bronze_ch.court_decisions_index (decision_date DESC);
CREATE INDEX IF NOT EXISTS idx_court_idx_doc ON bronze_ch.court_decisions_index (document_id) WHERE document_id IS NOT NULL;
