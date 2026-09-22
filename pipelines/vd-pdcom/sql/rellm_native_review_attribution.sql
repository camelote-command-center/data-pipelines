-- Existing private route only. Polygon attribution remains unchanged.
-- Native features require an explicit kind and never imply parcel area/rights.
CREATE OR REPLACE VIEW gold_ch.v_vd_pdcom_review_sectors WITH (security_invoker=true) AS
SELECT s.id,s.document_id,
 CASE WHEN ST_Dimension(s.geom)=2 THEN ARRAY(
   SELECT dc.commune_bfs
   FROM bronze_ch.vd_pdcom_document_communes dc
   JOIN bronze_ch.vd_pdcom_communes c ON c.commune_bfs=dc.commune_bfs AND c.is_current
   JOIN bronze_ch.swiss_communes_geo g ON g.bfs_nummer=dc.commune_bfs
   WHERE dc.document_id=s.document_id
     AND g.geometry && ST_Transform(s.geom,2056)
     AND ST_Relate(ST_Transform(g.geometry,4326),s.geom,'2********')
   ORDER BY dc.commune_bfs
 ) ELSE native.communes END AS commune_bfs,
 s.page_number,s.label,s.geom,s.alignment_rmse_m,s.source_precision_m,
 CASE WHEN ST_Dimension(s.geom) IN (0,1) THEN s.validation_evidence || jsonb_build_object(
   'native_attribution',jsonb_build_object('method','positive_length_or_point_covers',
   'commune_bfs',native.communes,'multiple_communes',cardinality(native.communes)>1,
   'boundary_ambiguity',native.boundary_contact OR cardinality(native.communes)>1,
   'document_scope_is_not_geographic_attribution',true,'parcel_association',false))
 ELSE s.validation_evidence END AS validation_evidence,
 s.validated_by,s.review_status,d.source_url,d.sha256 AS source_sha256,d.plan_status,
 'internal_review_only'::text AS publication_status
FROM bronze_ch.vd_pdcom_sectors s
JOIN bronze_ch.vd_pdcom_documents d ON d.id=s.document_id
LEFT JOIN LATERAL (
 SELECT COALESCE(array_agg(c.commune_bfs ORDER BY c.commune_bfs),'{}'::integer[]) communes,
 COALESCE(bool_or(ST_Intersects(ST_Boundary(ST_Transform(g.geometry,4326)),s.geom)),false) boundary_contact
 FROM bronze_ch.vd_pdcom_communes c
 JOIN bronze_ch.swiss_communes_geo g ON g.bfs_nummer=c.commune_bfs
 WHERE c.is_current AND ST_Dimension(s.geom) IN (0,1)
 AND g.geometry && ST_Transform(s.geom,2056)
 AND CASE
 WHEN GeometryType(s.geom) IN ('LINESTRING','MULTILINESTRING')
  AND s.validation_evidence->>'feature_kind'='source_native_line'
 THEN ST_Length(ST_Intersection(ST_Transform(g.geometry,4326),s.geom))>0
 WHEN GeometryType(s.geom) IN ('POINT','MULTIPOINT')
  AND s.validation_evidence->>'feature_kind'='source_native_point'
 THEN EXISTS (SELECT 1 FROM ST_Dump(s.geom) pt WHERE ST_Covers(ST_Transform(g.geometry,4326),pt.geom))
 ELSE false END
) native ON true;
REVOKE ALL ON gold_ch.v_vd_pdcom_review_sectors FROM PUBLIC,anon,authenticated;
