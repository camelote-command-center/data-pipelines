-- Target: RE-LLM. Additive document acquisition only; existing PDCom/geometries untouched.
BEGIN;
CREATE TABLE IF NOT EXISTS bronze_ch.planning_document_runs (
 id uuid PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT now(),
 completed_at timestamptz, status text NOT NULL DEFAULT 'running',
 scope jsonb NOT NULL, report jsonb NOT NULL DEFAULT '{}', workflow_url text
);
CREATE TABLE IF NOT EXISTS bronze_ch.planning_document_sources (
 id uuid PRIMARY KEY, source text NOT NULL, canton_code text NOT NULL,
 source_key text NOT NULL, title text NOT NULL, document_url text,
 commune_bfs integer, language text, legal_status text, document_type text,
 source_metadata jsonb NOT NULL, catalog_hash text NOT NULL,
 first_seen_at timestamptz NOT NULL DEFAULT now(), last_seen_at timestamptz NOT NULL DEFAULT now(),
 last_seen_run uuid REFERENCES bronze_ch.planning_document_runs(id),
 last_attempt_at timestamptz, last_success_at timestamptz,
 extraction_status text NOT NULL DEFAULT 'pending', error_code text,
 current_version_id uuid,
 UNIQUE(source,canton_code,source_key)
);
CREATE TABLE IF NOT EXISTS bronze_ch.planning_document_versions (
 id uuid PRIMARY KEY, source_id uuid NOT NULL REFERENCES bronze_ch.planning_document_sources(id),
 content_hash text NOT NULL, fetched_at timestamptz NOT NULL DEFAULT now(),
 final_url text NOT NULL, content_type text NOT NULL, byte_count integer NOT NULL,
 pages jsonb NOT NULL, extraction_status text NOT NULL, knowledge_document_id uuid,
 UNIQUE(source_id,content_hash)
);
CREATE INDEX IF NOT EXISTS planning_document_sources_canton_idx ON bronze_ch.planning_document_sources(canton_code,commune_bfs);
CREATE INDEX IF NOT EXISTS planning_document_sources_url_idx ON bronze_ch.planning_document_sources(document_url);
CREATE INDEX IF NOT EXISTS planning_document_sources_status_idx ON bronze_ch.planning_document_sources(extraction_status);
CREATE INDEX IF NOT EXISTS planning_document_versions_source_idx ON bronze_ch.planning_document_versions(source_id);
ALTER TABLE bronze_ch.planning_document_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE bronze_ch.planning_document_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE bronze_ch.planning_document_versions ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON bronze_ch.planning_document_runs,bronze_ch.planning_document_sources,bronze_ch.planning_document_versions FROM PUBLIC,anon,authenticated;
GRANT ALL ON bronze_ch.planning_document_runs,bronze_ch.planning_document_sources,bronze_ch.planning_document_versions TO service_role;
COMMENT ON TABLE bronze_ch.planning_document_sources IS 'Official CH planning document references. Counts are NOT commune coverage or proof of current binding rules. Missing URLs and source-provided legal status preserved. Pixxels dataset ch_planning_documents.';
COMMIT;
-- Rollback: disable ch_planning_documents.yml and mark its dataset paused;
-- preserve these additive audit tables and knowledge rows. Do not DROP or DELETE production records.
