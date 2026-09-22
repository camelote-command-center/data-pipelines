-- Run on the registered Lamap receiver. Additive private historical observations.
CREATE TABLE IF NOT EXISTS ref.vd_pdcom_review_site_scenarios (
 document_id uuid NOT NULL,site_id text NOT NULL,scenario integer NOT NULL,
 commune_bfs integer NOT NULL,page_number integer NOT NULL,site_name text,
 source_area_ha numeric,construction_fraction numeric,saturation_fraction numeric,
 inhabitants_jobs_per_ha numeric,stated_inhabitants_jobs numeric,notes text,
 source_url text NOT NULL,source_sha256 text NOT NULL,source_evidence jsonb NOT NULL,
 review_status text NOT NULL CHECK(review_status='historical_scenario_not_current_capacity'),
 publication_status text NOT NULL CHECK(publication_status='internal_review_only'),
 PRIMARY KEY(document_id,site_id,scenario)
);
ALTER TABLE ref.vd_pdcom_review_site_scenarios ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON ref.vd_pdcom_review_site_scenarios FROM PUBLIC,anon,authenticated;

-- Blank source names are retained as null, not inferred from another scenario.
ALTER TABLE ref.vd_pdcom_review_site_scenarios ALTER COLUMN site_name DROP NOT NULL;
