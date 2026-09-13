-- Additive VD PDCom inventory. No changes to GE extraction or consumer eligibility.
-- Rollback: stop vd_pdcom.yml scheduling and mark its Pixxels dataset paused;
-- retain these audit tables. No DROP/TRUNCATE required.
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_runs (
 id uuid PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT now(),
 completed_at timestamptz, status text NOT NULL CHECK(status IN ('running','success','failed')),
 roster_date date NOT NULL, roster_url text NOT NULL, roster_sha256 text,
 report jsonb NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_communes (
 commune_bfs integer PRIMARY KEY CHECK(commune_bfs BETWEEN 5400 AND 5999),
 commune_name text NOT NULL, historical_code integer NOT NULL,
 district text NOT NULL, roster_date date NOT NULL, roster_run_id uuid NOT NULL REFERENCES bronze_ch.vd_pdcom_runs,
 is_current boolean NOT NULL DEFAULT true, first_seen_at timestamptz NOT NULL DEFAULT now(),
 last_seen_at timestamptz NOT NULL DEFAULT now(), retired_at timestamptz,
 discovery_status text NOT NULL DEFAULT 'not_searched' CHECK(discovery_status IN
 ('not_searched','searching','sources_found','not_found','confirmed_no_plan','blocked')),
 extraction_status text NOT NULL DEFAULT 'pending' CHECK(extraction_status IN
 ('pending','downloaded','candidate_vectors','needs_ocr','failed','validated')),
 delivery_status text NOT NULL DEFAULT 'not_ready' CHECK(delivery_status IN ('not_ready','pending','verified','failed')),
 last_checked_at timestamptz, next_review_at timestamptz NOT NULL DEFAULT now(),
 evidence jsonb NOT NULL DEFAULT '[]', blocker text, review_note text,
 CHECK(discovery_status <> 'confirmed_no_plan' OR jsonb_array_length(evidence)>0),
 CHECK(delivery_status <> 'verified' OR extraction_status='validated')
);
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_documents (
 id uuid PRIMARY KEY, source_url text NOT NULL, landing_url text NOT NULL,
 sha256 text NOT NULL, title text NOT NULL, plan_status text NOT NULL,
 scope text NOT NULL CHECK(scope IN ('communal','intercommunal','regional')),
 status_evidence_url text NOT NULL, downloaded_at timestamptz NOT NULL DEFAULT now(),
 page_count integer NOT NULL, inspection jsonb NOT NULL,
 UNIQUE(source_url,sha256)
);
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_document_communes (
 document_id uuid REFERENCES bronze_ch.vd_pdcom_documents,
 commune_bfs integer REFERENCES bronze_ch.vd_pdcom_communes,
 coverage_extent text NOT NULL DEFAULT 'unverified' CHECK(coverage_extent IN ('unverified','whole_commune','partial')),
 evidence_url text NOT NULL,
 PRIMARY KEY(document_id,commune_bfs)
);
-- PDF coordinates are retained in inspection JSON, never passed off as geographic geometry.
-- Validated geographical output is a separate surface with mandatory GIST geom.
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_sectors (
 id uuid PRIMARY KEY, document_id uuid NOT NULL REFERENCES bronze_ch.vd_pdcom_documents,
 page_number integer NOT NULL CHECK(page_number>0), label text NOT NULL,
 geom geometry(Geometry,4326) NOT NULL,
 alignment_rmse_m double precision NOT NULL CHECK(alignment_rmse_m>=0),
 source_precision_m double precision NOT NULL CHECK(source_precision_m>0),
 validation_evidence jsonb NOT NULL, validated_by text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS vd_pdcom_sectors_geom_gist ON bronze_ch.vd_pdcom_sectors USING gist(geom);
CREATE OR REPLACE VIEW bronze_ch.vd_pdcom_coverage WITH(security_invoker=true) AS
 SELECT c.*, (SELECT count(*) FROM bronze_ch.vd_pdcom_document_communes d WHERE d.commune_bfs=c.commune_bfs) AS document_count,
 (c.discovery_status='confirmed_no_plan' OR (c.extraction_status='validated' AND c.delivery_status='verified')) AS resolved,
 (c.next_review_at<=now()) AS review_due
 FROM bronze_ch.vd_pdcom_communes c;
REVOKE ALL ON bronze_ch.vd_pdcom_runs,bronze_ch.vd_pdcom_communes,bronze_ch.vd_pdcom_documents,
 bronze_ch.vd_pdcom_document_communes,bronze_ch.vd_pdcom_sectors,bronze_ch.vd_pdcom_coverage FROM anon,authenticated;
