-- A shared document's members are source scope, not every sector's location.
-- Retain only current declared members with positive-area interior overlap.
-- Compare topology in the stored sector CRS to avoid inverse-transform slivers.
-- Touching a boundary alone does not assign a commune; no fallback on no match.
CREATE OR REPLACE VIEW gold_ch.v_vd_pdcom_review_sectors WITH (security_invoker=true) AS
SELECT s.id,s.document_id,
 ARRAY(
   SELECT dc.commune_bfs
   FROM bronze_ch.vd_pdcom_document_communes dc
   JOIN bronze_ch.vd_pdcom_communes c ON c.commune_bfs=dc.commune_bfs AND c.is_current
   JOIN bronze_ch.swiss_communes_geo g ON g.bfs_nummer=dc.commune_bfs
   WHERE dc.document_id=s.document_id
     AND g.geometry && ST_Transform(s.geom,2056)
     AND ST_Relate(ST_Transform(g.geometry,4326),s.geom,'2********')
   ORDER BY dc.commune_bfs
 ) AS commune_bfs,
 s.page_number,s.label,s.geom,s.alignment_rmse_m,s.source_precision_m,s.validation_evidence,s.validated_by,s.review_status,
 d.source_url,d.sha256 AS source_sha256,d.plan_status,
 'internal_review_only'::text AS publication_status
FROM bronze_ch.vd_pdcom_sectors s JOIN bronze_ch.vd_pdcom_documents d ON d.id=s.document_id;
REVOKE ALL ON gold_ch.v_vd_pdcom_review_sectors FROM PUBLIC,anon,authenticated;
