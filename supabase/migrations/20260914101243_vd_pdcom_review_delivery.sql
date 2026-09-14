CREATE OR REPLACE VIEW gold_ch.v_vd_pdcom_review_sectors WITH (security_invoker=true) AS
SELECT s.id,s.document_id,
 ARRAY(SELECT dc.commune_bfs FROM bronze_ch.vd_pdcom_document_communes dc WHERE dc.document_id=s.document_id ORDER BY dc.commune_bfs) AS commune_bfs,
 s.page_number,s.label,s.geom,s.alignment_rmse_m,s.source_precision_m,s.validation_evidence,s.validated_by,s.review_status,
 d.source_url,d.sha256 AS source_sha256,d.plan_status,
 'internal_review_only'::text AS publication_status
FROM bronze_ch.vd_pdcom_sectors s JOIN bronze_ch.vd_pdcom_documents d ON d.id=s.document_id;
CREATE OR REPLACE VIEW gold_ch.v_vd_pdcom_review_parcels WITH (security_invoker=true) AS
SELECT p.sector_id,p.egrid,p.overlap_m2,p.parcel_area_m2,p.overlap_fraction,p.boundary_review_required,p.uncertainty_buffer_m,p.geom,
 s.review_status,'internal_review_only'::text AS publication_status
FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN bronze_ch.vd_pdcom_sectors s ON s.id=p.sector_id;
REVOKE ALL ON gold_ch.v_vd_pdcom_review_sectors,gold_ch.v_vd_pdcom_review_parcels FROM anon,authenticated;
