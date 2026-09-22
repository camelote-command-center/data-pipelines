-- Reuse the registered Lamap FDW identity and server.
DO $$ BEGIN
 IF to_regclass('lamap_db_foreign.vd_pdcom_review_site_scenarios') IS NULL THEN
  IMPORT FOREIGN SCHEMA ref LIMIT TO(vd_pdcom_review_site_scenarios)
   FROM SERVER lamap_db_server INTO lamap_db_foreign;
 END IF;
END $$;
REVOKE ALL ON lamap_db_foreign.vd_pdcom_review_site_scenarios FROM PUBLIC,anon,authenticated;

ALTER FOREIGN TABLE lamap_db_foreign.vd_pdcom_review_site_scenarios ALTER COLUMN site_name DROP NOT NULL;
