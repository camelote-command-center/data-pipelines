-- ============================================================
-- bronze_ch.fao_transactions_parse_errors — quarantine for rejected rows
-- ============================================================
-- Rows that fail validateParsedPrice() (implausible_price or future reasons)
-- land here instead of being silently dropped. Reviewed manually; once root
-- cause is fixed, can be re-parsed via reparse-recent.ts.

CREATE TABLE IF NOT EXISTS bronze_ch.fao_transactions_parse_errors (
  id              BIGSERIAL PRIMARY KEY,
  affaire_number  TEXT,
  reason          TEXT NOT NULL,
  parsed_price    TEXT,                 -- the price the LLM/parser produced (rejected)
  raw_regex_price TEXT,                 -- what the regex pulled from raw text, if any
  llm_payload     JSONB,                -- the full Claude response for debugging
  raw_text        TEXT NOT NULL,        -- the gazette text (untouched)
  warnings        TEXT[],               -- non-fatal warnings collected during parsing
  flagged_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE bronze_ch.fao_transactions_parse_errors IS
  'Quarantine for FAO transaction rows that failed validation before insert into bronze_ch.fao_transactions. Reviewed manually; reparse via pipelines/transactions-fao/reparse-recent.ts after root cause fix.';

CREATE INDEX IF NOT EXISTS ix_fao_tx_parse_errors_affaire
  ON bronze_ch.fao_transactions_parse_errors (affaire_number);
CREATE INDEX IF NOT EXISTS ix_fao_tx_parse_errors_reason
  ON bronze_ch.fao_transactions_parse_errors (reason);
CREATE INDEX IF NOT EXISTS ix_fao_tx_parse_errors_flagged_at
  ON bronze_ch.fao_transactions_parse_errors (flagged_at DESC);
