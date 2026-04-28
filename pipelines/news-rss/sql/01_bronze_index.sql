-- news-rss — URL-level dedup map for the news aggregator.
-- Tracks every URL we've seen across all feeds so we never re-fetch.
-- Articles themselves land in knowledge_ch.documents (per re-LLM v2 architecture).
--
-- Rollback: DROP TABLE IF EXISTS bronze_ch.news_index;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

CREATE TABLE IF NOT EXISTS bronze_ch.news_index (
  id                  BIGSERIAL PRIMARY KEY,
  feed_slug           TEXT NOT NULL,
  url                 TEXT NOT NULL,
  url_hash            TEXT NOT NULL,         -- md5(url) for cheap lookup
  document_id         UUID,                  -- → knowledge_ch.documents.id once inserted
  feed_title          TEXT,
  feed_published_at   TIMESTAMPTZ,
  fetch_status        TEXT NOT NULL DEFAULT 'pending',  -- pending | success | failed | skipped
  fetch_error         TEXT,
  retry_count         INT NOT NULL DEFAULT 0,
  language            TEXT,
  fetched_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT news_index_url_unique UNIQUE (url)
);

CREATE INDEX IF NOT EXISTS idx_news_index_feed ON bronze_ch.news_index (feed_slug);
CREATE INDEX IF NOT EXISTS idx_news_index_status ON bronze_ch.news_index (fetch_status);
CREATE INDEX IF NOT EXISTS idx_news_index_published ON bronze_ch.news_index (feed_published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_index_doc ON bronze_ch.news_index (document_id) WHERE document_id IS NOT NULL;

CREATE OR REPLACE FUNCTION bronze_ch._news_index_touch()
RETURNS TRIGGER AS $$ BEGIN NEW.last_attempt_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_news_index_touch ON bronze_ch.news_index;
CREATE TRIGGER trg_news_index_touch
  BEFORE UPDATE ON bronze_ch.news_index
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._news_index_touch();
