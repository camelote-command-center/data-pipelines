-- ============================================================================
-- transactions_national — id sequence default (lamap_db)
-- ============================================================================
-- Both ref.transactions_national.id and public.transactions_national_data.id were
-- plain int with NO default/identity: ref.id is nullable (ref has NO PK — its real
-- key is now UNIQUE(source_id,canton)); public._data.id is NOT NULL PK. The legacy
-- xlsx/fo_fr_ch loader assigned ids by hand. Add a shared sequence default so the
-- streamed UPSERT can insert without managing ids. Additive; existing rows untouched.
-- ref.id and serving.id are NOT a join key (logical key is (source_id,canton)); new
-- rows may get different ids in each table — acceptable.
--
-- Rollback:
--   ALTER TABLE ref.transactions_national ALTER COLUMN id DROP DEFAULT;
--   ALTER TABLE public.transactions_national_data ALTER COLUMN id DROP DEFAULT;
--   DROP SEQUENCE public.transactions_national_id_seq;
-- ============================================================================
BEGIN;
CREATE SEQUENCE IF NOT EXISTS public.transactions_national_id_seq;
SELECT setval('public.transactions_national_id_seq', GREATEST(
  (SELECT COALESCE(max(id),0) FROM ref.transactions_national),
  (SELECT COALESCE(max(id),0) FROM public.transactions_national_data)), true);
ALTER TABLE ref.transactions_national         ALTER COLUMN id SET DEFAULT nextval('public.transactions_national_id_seq');
ALTER TABLE public.transactions_national_data ALTER COLUMN id SET DEFAULT nextval('public.transactions_national_id_seq');
COMMIT;
NOTIFY pgrst, 'reload schema';
