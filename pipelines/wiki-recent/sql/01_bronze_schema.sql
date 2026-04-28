-- wiki-recent — Wikipedia edit-velocity log for watched Geneva-RE entities.
-- One row per revision on a watched page (FR + DE). Tiny table by design.
--
-- Source: MediaWiki Action API on fr.wikipedia.org / de.wikipedia.org.
-- Watched titles come from bronze_ch.wikipedia_articles (already populated by
-- the wikipedia-ge pipeline).
--
-- Rollback: DROP TABLE IF EXISTS bronze_ch.wikipedia_edit_log;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.wikipedia_edit_log (
  id              BIGSERIAL PRIMARY KEY,
  qid             TEXT NOT NULL,         -- e.g. 'Q11917'
  language        TEXT NOT NULL,         -- 'fr' | 'de' | 'en' | 'it'
  page_title      TEXT NOT NULL,
  page_id         INTEGER,
  rev_id          BIGINT NOT NULL,
  parent_rev_id   BIGINT,
  user_name       TEXT,                  -- editor login or IP/temp identifier
  is_anonymous    BOOLEAN,
  comment         TEXT,
  edit_timestamp  TIMESTAMPTZ NOT NULL,
  size_bytes      INTEGER,
  tags            TEXT[],                -- mediawiki edit tags (visualeditor, mobile, etc.)
  fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT wikipedia_edit_log_rev_lang_unique UNIQUE (rev_id, language)
);

CREATE INDEX IF NOT EXISTS idx_wp_edit_qid ON bronze_ch.wikipedia_edit_log (qid);
CREATE INDEX IF NOT EXISTS idx_wp_edit_timestamp ON bronze_ch.wikipedia_edit_log (edit_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_wp_edit_lang ON bronze_ch.wikipedia_edit_log (language);
CREATE INDEX IF NOT EXISTS idx_wp_edit_user ON bronze_ch.wikipedia_edit_log (user_name);
