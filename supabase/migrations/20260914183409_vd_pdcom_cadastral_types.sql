-- RE-LLM source only. Object kind does not establish planning rights or release.
ALTER TABLE bronze_ch.vd_pdcom_parcel_candidates
 ADD COLUMN IF NOT EXISTS cadastral_object_kind text NOT NULL DEFAULT 'unknown'
 CHECK(cadastral_object_kind IN ('unknown','bien_fonds','ddp_superficie','ddp_source')),
 ADD COLUMN IF NOT EXISTS cadastral_type_evidence jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE OR REPLACE VIEW gold_ch.v_vd_pdcom_review_parcels WITH (security_invoker=true) AS
SELECT p.sector_id,p.egrid,p.overlap_m2,p.parcel_area_m2,p.overlap_fraction,p.boundary_review_required,p.uncertainty_buffer_m,p.geom,
 s.review_status,'internal_review_only'::text AS publication_status,p.cadastral_object_kind,p.cadastral_type_evidence
FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN bronze_ch.vd_pdcom_sectors s ON s.id=p.sector_id;
REVOKE ALL ON gold_ch.v_vd_pdcom_review_parcels FROM PUBLIC,anon,authenticated;
