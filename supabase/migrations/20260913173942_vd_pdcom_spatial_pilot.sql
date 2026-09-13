-- Distinguish geographic pilot candidates from released, fully reviewed sectors.
-- Rollback: leave pilot rows unreleased; no deletion or receiver changes.
ALTER TABLE bronze_ch.vd_pdcom_sectors ALTER COLUMN source_precision_m DROP NOT NULL;
ALTER TABLE bronze_ch.vd_pdcom_sectors ALTER COLUMN validated_by DROP NOT NULL;
ALTER TABLE bronze_ch.vd_pdcom_sectors ADD COLUMN IF NOT EXISTS review_status text NOT NULL DEFAULT 'review_required'
 CHECK(review_status IN ('review_required','validated','superseded'));
CREATE TABLE IF NOT EXISTS bronze_ch.vd_pdcom_parcel_candidates (
 sector_id uuid NOT NULL REFERENCES bronze_ch.vd_pdcom_sectors,
 egrid text NOT NULL, overlap_m2 double precision NOT NULL CHECK(overlap_m2>0),
 parcel_area_m2 double precision NOT NULL CHECK(parcel_area_m2>0),
 overlap_fraction double precision NOT NULL CHECK(overlap_fraction BETWEEN 0 AND 1),
 boundary_review_required boolean NOT NULL, uncertainty_buffer_m double precision NOT NULL,
 geom geometry(Geometry,4326) NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(sector_id,egrid)
);
CREATE INDEX IF NOT EXISTS vd_pdcom_parcel_candidates_geom_gist ON bronze_ch.vd_pdcom_parcel_candidates USING gist(geom);
REVOKE ALL ON bronze_ch.vd_pdcom_parcel_candidates FROM anon,authenticated;
