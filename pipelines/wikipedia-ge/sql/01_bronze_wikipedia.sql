-- Wikipedia Geneva — bronze tables on re-llm
--
-- Two tables:
--   * wikidata_entities — one row per Q-id; full Wikidata claims as JSONB
--   * wikipedia_articles — one row per (qid, language); REST page summary +
--                         page HTML + revision_id, fetched once per refresh
--
-- Source attribution baked into every row: source_url, fetched_at,
-- revision_id, license. No row can exist without a citable URL.
--
-- Phase-1 scope: Geneva real-estate (communes, quartiers, GE-specific RE laws,
-- major developments, RE companies, institutions). Future phases add `civic`,
-- `transport`, etc. by changing the seed list — table shape doesn't change.
--
-- Rollback:
--   DROP TABLE IF EXISTS bronze_ch.wikipedia_articles;
--   DROP TABLE IF EXISTS bronze_ch.wikidata_entities;

CREATE SCHEMA IF NOT EXISTS bronze_ch;

-- Wikidata: structured facts, CC0
CREATE TABLE IF NOT EXISTS bronze_ch.wikidata_entities (
  qid               TEXT PRIMARY KEY,                  -- e.g. 'Q11911' (canton of Geneva)
  labels            JSONB,                             -- { "fr": "...", "en": "...", ... }
  descriptions      JSONB,                             -- { "fr": "...", "en": "...", ... }
  claims            JSONB NOT NULL,                    -- raw Wikidata claims (P-properties)
  sitelinks         JSONB,                             -- { "frwiki": {title, url}, "enwiki": ... }
  domain            TEXT NOT NULL DEFAULT 'real_estate',
  category          TEXT,                              -- 'commune' | 'quartier' | 'law' | 'development' | 'company' | 'institution'
  source_url        TEXT NOT NULL,                     -- https://www.wikidata.org/wiki/<qid>
  license           TEXT NOT NULL DEFAULT 'CC0',
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_wikidata_entities_category
  ON bronze_ch.wikidata_entities (category);

CREATE INDEX IF NOT EXISTS idx_wikidata_entities_domain
  ON bronze_ch.wikidata_entities (domain);

CREATE INDEX IF NOT EXISTS idx_wikidata_entities_claims_gin
  ON bronze_ch.wikidata_entities USING GIN (claims);

-- Wikipedia: prose articles, CC BY-SA 4.0
CREATE TABLE IF NOT EXISTS bronze_ch.wikipedia_articles (
  id                BIGSERIAL PRIMARY KEY,
  qid               TEXT NOT NULL REFERENCES bronze_ch.wikidata_entities(qid) ON DELETE CASCADE,
  language          TEXT NOT NULL,                     -- 'fr', 'en', ...
  title             TEXT NOT NULL,
  revision_id       BIGINT NOT NULL,                   -- MediaWiki revision id at fetch time
  summary           TEXT,                              -- REST /page/summary plaintext extract
  description       TEXT,                              -- REST /page/summary "description" field
  html              TEXT NOT NULL,                     -- REST /page/html (full rendered HTML)
  categories        TEXT[],                            -- raw category names
  source_url        TEXT NOT NULL,                     -- https://<lang>.wikipedia.org/wiki/<title>
  license           TEXT NOT NULL DEFAULT 'CC BY-SA 4.0',
  attribution       TEXT NOT NULL,                     -- ready-to-render attribution string
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  domain            TEXT NOT NULL DEFAULT 'real_estate',
  category          TEXT,                              -- mirror of wikidata_entities.category
  CONSTRAINT wikipedia_articles_qid_lang_unique UNIQUE (qid, language)
);

CREATE INDEX IF NOT EXISTS idx_wikipedia_articles_qid
  ON bronze_ch.wikipedia_articles (qid);

CREATE INDEX IF NOT EXISTS idx_wikipedia_articles_language
  ON bronze_ch.wikipedia_articles (language);

CREATE INDEX IF NOT EXISTS idx_wikipedia_articles_category
  ON bronze_ch.wikipedia_articles (category);

CREATE INDEX IF NOT EXISTS idx_wikipedia_articles_categories_gin
  ON bronze_ch.wikipedia_articles USING GIN (categories);

-- Free-text search across summary (fast Lia preview lookups)
CREATE INDEX IF NOT EXISTS idx_wikipedia_articles_summary_trgm
  ON bronze_ch.wikipedia_articles USING GIN (summary gin_trgm_ops);

-- Auto-bump updated_at on wikidata_entities
CREATE OR REPLACE FUNCTION bronze_ch._wikidata_entities_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_wikidata_entities_touch_updated_at
  ON bronze_ch.wikidata_entities;
CREATE TRIGGER trg_wikidata_entities_touch_updated_at
  BEFORE UPDATE ON bronze_ch.wikidata_entities
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._wikidata_entities_touch_updated_at();
