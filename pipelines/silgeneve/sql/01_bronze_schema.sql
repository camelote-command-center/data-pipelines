-- ============================================================================
-- SILGeneve pipeline — bronze_ch schema
-- Target DB: re-LLM (znrvddgmczdqoucmykij)
-- ============================================================================
-- Run via: apply_migration (NOT execute_sql — this is DDL)
-- After creation, run: NOTIFY pgrst, 'reload schema';
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS bronze_ch;

-- ---------------------------------------------------------------------------
-- Main laws table — one row per SILGeneve document
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.silgeneve_laws (
  law_rsge          TEXT        PRIMARY KEY,               -- 'L 5 20'
  url_slug          TEXT        NOT NULL,                  -- 'rsg_l5_20.htm'
  source_url        TEXT        NOT NULL,                  -- full URL
  short_name        TEXT,                                   -- 'LDTR'
  full_title        TEXT        NOT NULL,                  -- 'Loi sur les démolitions...'
  law_type          TEXT        NOT NULL,                  -- 'loi' | 'reglement' | 'arrete'
  domain            TEXT        NOT NULL,                  -- 'logement' | 'construction' | ...
  priority          INT         NOT NULL DEFAULT 3,        -- 1=critical, 2=high, 3=normal
  notes             TEXT,                                   -- editorial notes from registry
  adopted_date      DATE,                                   -- first adoption
  entry_in_force    DATE,                                   -- original entry in force
  last_modified     DATE,                                   -- 'Dernières modifications au...'
  content_html      TEXT,                                   -- raw cleaned HTML
  content_md        TEXT,                                   -- markdown for RAG ingestion
  content_hash      TEXT,                                   -- sha256 of content_md — change detection
  modifications_history JSONB,                              -- version history table
  article_count     INT         NOT NULL DEFAULT 0,
  word_count        INT         NOT NULL DEFAULT 0,
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_silgeneve_laws_domain   ON bronze_ch.silgeneve_laws (domain);
CREATE INDEX IF NOT EXISTS idx_silgeneve_laws_priority ON bronze_ch.silgeneve_laws (priority);
CREATE INDEX IF NOT EXISTS idx_silgeneve_laws_updated  ON bronze_ch.silgeneve_laws (updated_at DESC);

COMMENT ON TABLE bronze_ch.silgeneve_laws IS
  'Geneva cantonal real-estate-related laws, scraped weekly from silgeneve.ch. See camelote-command-center/data-pipelines/silgeneve.';


-- ---------------------------------------------------------------------------
-- Articles table — one row per article inside a law
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.silgeneve_articles (
  id                BIGSERIAL   PRIMARY KEY,
  law_rsge          TEXT        NOT NULL REFERENCES bronze_ch.silgeneve_laws(law_rsge) ON DELETE CASCADE,
  article_number    TEXT        NOT NULL,                  -- 'Art. 1', 'Art. 42A'
  article_order     INT         NOT NULL,                  -- ordering within law
  article_title     TEXT,                                   -- 'But'
  chapter           TEXT,                                   -- 'Chapitre I - Préambule'
  section           TEXT,                                   -- 'Section 1 - Buts et moyens'
  content           TEXT        NOT NULL,
  content_hash      TEXT        NOT NULL,
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (law_rsge, article_number)
);

CREATE INDEX IF NOT EXISTS idx_silgeneve_articles_law     ON bronze_ch.silgeneve_articles (law_rsge, article_order);
CREATE INDEX IF NOT EXISTS idx_silgeneve_articles_chapter ON bronze_ch.silgeneve_articles (law_rsge, chapter);

COMMENT ON TABLE bronze_ch.silgeneve_articles IS
  'Individual articles extracted from SILGeneve laws. Keyed by (law_rsge, article_number). Used for RAG chunk-level retrieval.';


-- ---------------------------------------------------------------------------
-- Fetch log — audit trail of every parser run
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_ch.silgeneve_fetch_log (
  id                BIGSERIAL   PRIMARY KEY,
  run_id            UUID        NOT NULL,                  -- groups all logs from one cron run
  law_rsge          TEXT,                                   -- NULL for run-level log
  status            TEXT        NOT NULL,                  -- 'success' | 'unchanged' | 'error' | 'run_start' | 'run_end'
  http_status       INT,
  content_changed   BOOLEAN,
  articles_parsed   INT,
  error_message     TEXT,
  duration_ms       INT,
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_silgeneve_log_run    ON bronze_ch.silgeneve_fetch_log (run_id);
CREATE INDEX IF NOT EXISTS idx_silgeneve_log_law    ON bronze_ch.silgeneve_fetch_log (law_rsge, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_silgeneve_log_status ON bronze_ch.silgeneve_fetch_log (status, fetched_at DESC);


-- ---------------------------------------------------------------------------
-- Trigger to maintain updated_at
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bronze_ch.silgeneve_laws_touch_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS silgeneve_laws_touch_updated_at ON bronze_ch.silgeneve_laws;
CREATE TRIGGER silgeneve_laws_touch_updated_at
  BEFORE UPDATE ON bronze_ch.silgeneve_laws
  FOR EACH ROW
  EXECUTE FUNCTION bronze_ch.silgeneve_laws_touch_updated_at();


-- ---------------------------------------------------------------------------
-- Reload PostgREST schema cache
-- ---------------------------------------------------------------------------
NOTIFY pgrst, 'reload schema';
