CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_discovery_queue (
 commune_bfs integer PRIMARY KEY REFERENCES bronze_ch.vd_pdcom_communes(commune_bfs),
 directory_url text, website_url text,
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','candidates_found','manual_search_required','blocked')),
 attempt_count integer NOT NULL DEFAULT 0, last_attempt_at timestamptz,
 next_attempt_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_discovery_attempts (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), operation_id uuid NOT NULL,
 commune_bfs integer NOT NULL REFERENCES bronze_ch.vd_pdcom_communes(commune_bfs),
 attempted_at timestamptz NOT NULL DEFAULT now(),outcome text NOT NULL,evidence jsonb NOT NULL,
 UNIQUE(operation_id,commune_bfs)
);
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_source_candidates (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 commune_bfs integer NOT NULL REFERENCES bronze_ch.vd_pdcom_communes(commune_bfs),
 source_url text NOT NULL,evidence_url text NOT NULL,link_text text,kind text NOT NULL CHECK(kind IN ('landing','pdf')),
 review_status text NOT NULL DEFAULT 'pending' CHECK(review_status IN ('pending','accepted','rejected')),
 first_seen_at timestamptz NOT NULL DEFAULT now(),last_seen_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(commune_bfs,source_url)
);
ALTER TABLE bronze_ch.vd_pdcom_discovery_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE bronze_ch.vd_pdcom_discovery_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE bronze_ch.vd_pdcom_source_candidates ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON bronze_ch.vd_pdcom_discovery_queue,bronze_ch.vd_pdcom_discovery_attempts,bronze_ch.vd_pdcom_source_candidates FROM anon,authenticated;
CREATE INDEX IF NOT EXISTS vd_pdcom_discovery_due_idx ON bronze_ch.vd_pdcom_discovery_queue(next_attempt_at,attempt_count);
