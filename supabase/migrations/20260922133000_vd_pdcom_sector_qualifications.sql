-- Append-only, private qualification evidence; NOT a consumer release surface.
-- No existing candidate status, receiver or commune completion is changed.
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_sector_qualifications (
 id uuid PRIMARY KEY,
 sector_id uuid NOT NULL REFERENCES bronze_ch.vd_pdcom_sectors(id),
 document_id uuid NOT NULL REFERENCES bronze_ch.vd_pdcom_documents(id),
 source_sha256 text NOT NULL CHECK(source_sha256 ~ '^[0-9a-f]{64}$'),
 geometry_sha256 text NOT NULL CHECK(geometry_sha256 ~ '^[0-9a-f]{64}$'),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[0-9a-f]{64}$'),
 manifest jsonb NOT NULL CHECK(jsonb_typeof(manifest)='object'),
 recorded_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE bronze_ch.vd_pdcom_sector_qualifications ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON bronze_ch.vd_pdcom_sector_qualifications FROM PUBLIC,anon,authenticated;
CREATE OR REPLACE FUNCTION bronze_ch.vd_pdcom_qualification_immutable()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 RAISE EXCEPTION 'VD PDCom qualifications are immutable; retain history and submit a new reviewed manifest';
END $$;
DROP TRIGGER IF EXISTS vd_pdcom_qualification_immutable ON bronze_ch.vd_pdcom_sector_qualifications;
CREATE TRIGGER vd_pdcom_qualification_immutable BEFORE UPDATE OR DELETE OR TRUNCATE
 ON bronze_ch.vd_pdcom_sector_qualifications FOR EACH STATEMENT
 EXECUTE FUNCTION bronze_ch.vd_pdcom_qualification_immutable();
-- Rollback: stop qualification writes and retain evidence; do not drop history.
