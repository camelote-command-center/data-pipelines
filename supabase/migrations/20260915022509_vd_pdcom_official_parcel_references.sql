CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_parcel_reference_snapshots (
 id uuid PRIMARY KEY,
 document_id uuid NOT NULL REFERENCES bronze_ch.vd_pdcom_documents(id),
 commune_bfs integer NOT NULL REFERENCES bronze_ch.vd_pdcom_communes(commune_bfs),
 query_url text NOT NULL, count_url text NOT NULL,
 response_sha256 text NOT NULL CHECK (response_sha256 ~ '^[0-9a-f]{64}$'),
 feature_count integer NOT NULL CHECK (feature_count>0),
 bounds geometry(Polygon,2056) NOT NULL,
 fetched_at timestamptz NOT NULL DEFAULT now(),
 last_checked_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_parcel_references (
 snapshot_id uuid NOT NULL REFERENCES bronze_ch.vd_pdcom_parcel_reference_snapshots(id),
 egrid text NOT NULL CHECK (egrid ~ '^CH[0-9]{12}$'),
 commune_bfs integer NOT NULL REFERENCES bronze_ch.vd_pdcom_communes(commune_bfs),
 geom geometry(MultiPolygon,2056) NOT NULL CHECK (ST_IsValid(geom) AND NOT ST_IsEmpty(geom)),
 official_attributes jsonb NOT NULL,
 PRIMARY KEY(snapshot_id,egrid)
);
CREATE INDEX IF NOT EXISTS vd_pdcom_parcel_references_geom_gist ON bronze_ch.vd_pdcom_parcel_references USING gist(geom);
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_candidate_parcel_references (
 sector_id uuid NOT NULL, egrid text NOT NULL, snapshot_id uuid NOT NULL,
 PRIMARY KEY(sector_id,egrid),
 FOREIGN KEY(sector_id,egrid) REFERENCES bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid),
 FOREIGN KEY(snapshot_id,egrid) REFERENCES bronze_ch.vd_pdcom_parcel_references(snapshot_id,egrid)
);
ALTER TABLE bronze_ch.vd_pdcom_candidate_parcel_references ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON bronze_ch.vd_pdcom_candidate_parcel_references FROM PUBLIC,anon,authenticated;
ALTER TABLE bronze_ch.vd_pdcom_parcel_reference_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE bronze_ch.vd_pdcom_parcel_references ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON bronze_ch.vd_pdcom_parcel_reference_snapshots,bronze_ch.vd_pdcom_parcel_references FROM PUBLIC,anon,authenticated;
