-- ============================================================================
-- transactions_national — make the load path load-ready (lamap_db fckdwddgtdbvhzloejni)
-- ============================================================================
-- Fixes the LOAD path for the LU/SZ Handänderungen parser (and the paused SG lane,
-- which shares this table). Verified against lamap_db before writing:
--   • ref.transactions_national + public.transactions_national_data each held 13,623
--     rows, both with ONLY PRIMARY KEY(id) — no UNIQUE(source_id,canton), so the
--     loader's ON CONFLICT (source_id, canton) threw on the first live write.
--   • Grain confirmed per-transaction-record: 0 dup (source_id,canton), 0 NULL
--     source_id on BOTH tables (the 7 FR/VD source_id string collisions are
--     cross-canton — the composite key is exactly right).
--   • ref is the canonical member (raw_data lives here; medallion_fully_retired
--     standard: canonical = ref.*, serving = public.*). public._data is the lean
--     serving twin that ask_lamap.transactions_national reads.
--
-- Coordinated shared shape with the SG lane (paused): same (source_id,canton) key,
-- same is_ownerless_event/egrid generated columns.
--
-- Rollback:
--   ALTER TABLE ref.transactions_national DROP CONSTRAINT transactions_national_source_canton_key;
--   ALTER TABLE ref.transactions_national DROP COLUMN is_ownerless_event;
--   ALTER TABLE ref.transactions_national DROP COLUMN egrid;
--   ALTER TABLE public.transactions_national_data DROP CONSTRAINT transactions_national_data_source_canton_key;
--   NOTIFY pgrst, 'reload schema';
-- ============================================================================

BEGIN;

-- Guard: refuse if any grain violation snuck in since verification.
DO $$
BEGIN
  IF EXISTS (SELECT source_id, canton FROM ref.transactions_national
             GROUP BY source_id, canton HAVING count(*) > 1) THEN
    RAISE EXCEPTION 'ref.transactions_national has (source_id,canton) duplicates — resolve grain first';
  END IF;
  IF EXISTS (SELECT source_id, canton FROM public.transactions_national_data
             GROUP BY source_id, canton HAVING count(*) > 1) THEN
    RAISE EXCEPTION 'public.transactions_national_data has (source_id,canton) duplicates — resolve grain first';
  END IF;
END $$;

-- 1. The upsert key (canonical write target).
ALTER TABLE ref.transactions_national
  ADD CONSTRAINT transactions_national_source_canton_key UNIQUE (source_id, canton);

-- 2. Queryable homes for the actionable outputs, derived from raw_data jsonb.
--    Only this loader ever sets these keys; existing rows lack them → NULL.
ALTER TABLE ref.transactions_national
  ADD COLUMN is_ownerless_event boolean
  GENERATED ALWAYS AS ((raw_data ->> 'is_ownerless_event')::boolean) STORED;

ALTER TABLE ref.transactions_national
  ADD COLUMN egrid text
  GENERATED ALWAYS AS (raw_data ->> 'egrid') STORED;

-- 3. Same key on the serving twin so the lean projection upserts idempotently.
ALTER TABLE public.transactions_national_data
  ADD CONSTRAINT transactions_national_data_source_canton_key UNIQUE (source_id, canton);

COMMIT;

NOTIFY pgrst, 'reload schema';
