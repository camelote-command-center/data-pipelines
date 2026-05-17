-- 2026-05-17 — Register 3 new Swiss news sources in camelote_data.
-- Parser code lives in pipelines/news-rss/ (extended in same commit).
-- Topic scope: business + RE + Swiss econ-policy. Languages: de/fr/en.
-- See data-pipelines commit + plan: read-my-first-aid-transient-hoare.md.
--
-- Run target: camelote_data Supabase project (dxugbpeacnorjunpljih).
-- Idempotent via ON CONFLICT.

BEGIN;

-- ── source_providers (3 new rows) ────────────────────────────────────────
WITH ins AS (
  INSERT INTO public.source_providers (name, base_url, country_code, notes)
  VALUES
    ('Blick',        'https://www.blick.ch', 'CH', 'Ringier flagship — DE tabloid. Wirtschaft+Politik+News sub-feeds, keyword-filtered to econ/RE/business.'),
    ('newsdata.io',  'https://newsdata.io',  NULL, 'Global news aggregator API. Free tier: 200 req/day, country=ch&category=business&language=de,fr,en.'),
    ('GNews',        'https://gnews.io',     NULL, 'Global news aggregator API. Free tier: 100 req/day, country=ch&category=business, ≤10 articles/call/lang.')
  ON CONFLICT (name) DO UPDATE SET base_url = EXCLUDED.base_url, notes = EXCLUDED.notes
  RETURNING id, name
)
SELECT * FROM ins;

-- ── datasets (3 new rows) ─────────────────────────────────────────────────
-- All under news_rss.yml (single workflow, per-feed try/catch isolates).
-- startup_id = re-LLM (look up by code='RE-LLM' on startups table).
WITH
  re_llm AS (SELECT id FROM public.startups WHERE code = 'RE-LLM' LIMIT 1),
  sp_blick     AS (SELECT id FROM public.source_providers WHERE name = 'Blick' LIMIT 1),
  sp_newsdata  AS (SELECT id FROM public.source_providers WHERE name = 'newsdata.io' LIMIT 1),
  sp_gnews     AS (SELECT id FROM public.source_providers WHERE name = 'GNews' LIMIT 1)
INSERT INTO public.datasets (
  code, label, description, startup_id, source_provider_id,
  workflow_file, target_schema, target_table,
  acquisition_method, frequency, expected_days_between_updates, delay_threshold_days
)
VALUES
  ('ch_news_blick',
   'Blick — Wirtschaft + Politik + News',
   'Blick.ch RSS sub-feeds (DE). 3 slugs: blick_wirtschaft, blick_politik, blick_news. Keyword-filtered to Swiss econ/RE/business + housing-policy items before write.',
   (SELECT id FROM re_llm), (SELECT id FROM sp_blick),
   'news_rss.yml', 'bronze_ch', 'news_index',
   'api', 'daily', 1, 2),
  ('ch_news_newsdata',
   'newsdata.io — Switzerland business',
   'newsdata.io /api/1/latest with country=ch&category=business&language=de,fr,en. Paginated via nextPage; capped at 100 items/run. Keyword-filtered post-fetch.',
   (SELECT id FROM re_llm), (SELECT id FROM sp_newsdata),
   'news_rss.yml', 'bronze_ch', 'news_index',
   'api', 'daily', 1, 2),
  ('ch_news_gnews',
   'GNews — Switzerland business',
   'GNews /api/v4/top-headlines with country=ch&category=business, one call per lang (de/fr/en), max=10 each. Keyword-filtered post-fetch.',
   (SELECT id FROM re_llm), (SELECT id FROM sp_gnews),
   'news_rss.yml', 'bronze_ch', 'news_index',
   'api', 'daily', 1, 2)
ON CONFLICT (code) DO UPDATE SET
  label              = EXCLUDED.label,
  description        = EXCLUDED.description,
  workflow_file      = EXCLUDED.workflow_file,
  target_schema      = EXCLUDED.target_schema,
  target_table       = EXCLUDED.target_table,
  acquisition_method = EXCLUDED.acquisition_method,
  frequency          = EXCLUDED.frequency,
  expected_days_between_updates = EXCLUDED.expected_days_between_updates,
  delay_threshold_days = EXCLUDED.delay_threshold_days;

-- ── change_log entry ──────────────────────────────────────────────────────
INSERT INTO public.change_log (startup_id, summary, details, tags)
SELECT
  (SELECT id FROM public.startups WHERE code = 'RE-LLM'),
  'News pipeline: added 3 Swiss news sources (Blick + newsdata.io + GNews)',
  'Extended pipelines/news-rss/ with 3 Blick RSS sub-feeds (Wirtschaft+Politik+News, DE) and 2 API adapters (newsdata.io + GNews, DE+FR+EN, country=ch&category=business). Topic gate: keyword filter for Swiss econ/RE/business + econ-policy keywords (LDTR, RDPPF, Bundesrat, etc). Single workflow news_rss.yml. GHA secrets NEWSDATA_API_KEY + GNEWS_API_KEY set. 3 new dataset rows under workflow_file=news_rss.yml.',
  ARRAY['news-rss', 'parsers', 'bronze_ch'];

COMMIT;

-- ── Verification queries ──────────────────────────────────────────────────
-- SELECT code, label, workflow_file FROM public.datasets WHERE code LIKE 'ch_news_%' ORDER BY code;
-- SELECT name, base_url FROM public.source_providers WHERE name IN ('Blick','newsdata.io','GNews');
