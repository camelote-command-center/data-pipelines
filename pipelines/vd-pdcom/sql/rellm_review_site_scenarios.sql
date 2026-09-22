-- Exact reviewed source bytes only. Historical site scenarios, not geographic sectors.
CREATE OR REPLACE VIEW gold_ch.v_vd_pdcom_review_site_scenarios AS
SELECT d.id AS document_id, r->>'site_id' AS site_id,
 (r->>'scenario')::integer AS scenario, (r->>'commune_bfs')::integer AS commune_bfs,
 (r->>'source_page')::integer AS page_number, r->>'name' AS site_name,
 (r->>'area_ha')::numeric AS source_area_ha,
 (r->>'construction_fraction')::numeric AS construction_fraction,
 (r->>'saturation_fraction')::numeric AS saturation_fraction,
 (r->>'inhabitants_jobs_per_ha')::numeric AS inhabitants_jobs_per_ha,
 (r->>'stated_capacity')::numeric AS stated_inhabitants_jobs,
 r->>'notes' AS notes, d.source_url, d.sha256 AS source_sha256,
 jsonb_build_object('record',r,'source_vintage',m->'source_vintage',
 'limits',m->'limits','commune_sum_audits',m->'commune_sum_audits',
 'summary_source_values',m->'summary_source_values') AS source_evidence,
 'historical_scenario_not_current_capacity'::text AS review_status,
 'internal_review_only'::text AS publication_status
FROM bronze_ch.vd_pdcom_documents d
CROSS JOIN LATERAL jsonb_array_elements(d.inspection) i
CROSS JOIN LATERAL (SELECT i->'reviewed_operational_metrics' AS m) metadata
CROSS JOIN LATERAL jsonb_array_elements(m->'records') r
WHERE d.id='a2386483-8861-5e98-8631-9ea1adb6dca3'::uuid
 AND d.sha256='197829f74585f46c8637950f7651f94989d5cd54240ba695a3444b68cbc538bf'
 AND m->>'source_sha256'=d.sha256
 AND m->>'review_status'='four_source_table_pages_visually_reviewed';
REVOKE ALL ON gold_ch.v_vd_pdcom_review_site_scenarios FROM PUBLIC,anon,authenticated;
