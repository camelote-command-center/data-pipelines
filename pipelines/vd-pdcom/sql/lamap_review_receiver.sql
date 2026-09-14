-- Run on the registered Lamap database only. Private review copies, not live PDCom layers.
CREATE TABLE IF NOT EXISTS ref.vd_pdcom_review_sectors (
 id uuid PRIMARY KEY,document_id uuid NOT NULL,commune_bfs integer[] NOT NULL,
 page_number integer NOT NULL,label text NOT NULL,geom geometry(Geometry,4326) NOT NULL,
 alignment_rmse_m double precision NOT NULL,source_precision_m double precision,
 validation_evidence jsonb NOT NULL,validated_by text,review_status text NOT NULL,
 source_url text NOT NULL,source_sha256 text NOT NULL,plan_status text,
 publication_status text NOT NULL CHECK(publication_status='internal_review_only')
);
CREATE TABLE IF NOT EXISTS ref.vd_pdcom_review_parcels (
 sector_id uuid NOT NULL,egrid text NOT NULL,overlap_m2 double precision NOT NULL,
 parcel_area_m2 double precision NOT NULL,overlap_fraction double precision NOT NULL,
 boundary_review_required boolean NOT NULL,uncertainty_buffer_m double precision,
 geom geometry(Geometry,4326) NOT NULL,review_status text NOT NULL,
 publication_status text NOT NULL CHECK(publication_status='internal_review_only'),PRIMARY KEY(sector_id,egrid)
);
CREATE INDEX IF NOT EXISTS vd_pdcom_review_sectors_geom_idx ON ref.vd_pdcom_review_sectors USING gist(geom);
CREATE INDEX IF NOT EXISTS vd_pdcom_review_parcels_geom_idx ON ref.vd_pdcom_review_parcels USING gist(geom);
ALTER TABLE ref.vd_pdcom_review_sectors ENABLE ROW LEVEL SECURITY;
ALTER TABLE ref.vd_pdcom_review_parcels ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON ref.vd_pdcom_review_sectors,ref.vd_pdcom_review_parcels FROM PUBLIC,anon,authenticated;
-- Additive cadastral object typing for existing and newly installed receivers.
ALTER TABLE ref.vd_pdcom_review_parcels
 ADD COLUMN IF NOT EXISTS cadastral_object_kind text NOT NULL DEFAULT 'unknown'
 CHECK(cadastral_object_kind IN ('unknown','bien_fonds','ddp_superficie','ddp_source')),
 ADD COLUMN IF NOT EXISTS cadastral_type_evidence jsonb NOT NULL DEFAULT '{}'::jsonb;
