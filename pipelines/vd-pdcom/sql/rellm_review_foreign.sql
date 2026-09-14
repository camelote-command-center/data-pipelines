-- Registered existing receiver server; no new credentials or identity.
DO $$ BEGIN
 IF to_regclass('lamap_db_foreign.vd_pdcom_review_sectors') IS NULL THEN
  IMPORT FOREIGN SCHEMA ref LIMIT TO(vd_pdcom_review_sectors) FROM SERVER lamap_db_server INTO lamap_db_foreign;
 END IF;
 IF to_regclass('lamap_db_foreign.vd_pdcom_review_parcels') IS NULL THEN
  IMPORT FOREIGN SCHEMA ref LIMIT TO(vd_pdcom_review_parcels) FROM SERVER lamap_db_server INTO lamap_db_foreign;
 END IF;
END $$;
REVOKE ALL ON lamap_db_foreign.vd_pdcom_review_sectors,lamap_db_foreign.vd_pdcom_review_parcels FROM PUBLIC,anon,authenticated;
