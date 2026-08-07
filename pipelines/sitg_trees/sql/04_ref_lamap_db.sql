-- ═══════════════════════════════════════════════════════════════════════════
-- SITG tree cadastre — consumer table on lamap_db
--
-- APPLY ON lamap_db, not re-LLM.
--
-- PURE-REF: read by public.get_plot_trees_cadastre only. Nothing here reaches
-- into bronze, silver or gold.
--
-- This is a SEPARATE population from ref.plot_trees (1.59M LiDAR canopy
-- detections). plot_trees is geometry-only with an ESTIMATED crown radius and
-- no species or legal status; this is the surveyed official register with
-- MEASURED circumference, species and protection status, covering street, park
-- and inventoried trees only -- NOT every tree. They are cross-referenced by
-- proximity in the UI, never merged. get_plot_trees stays untouched.
--
-- geom_2056 travels alongside the 4326 geometry so metric work on lamap_db does
-- not have to reproject. Distances must never be computed in 4326.
--
-- _staging_ twin exists because gold_ch.sync_full_refresh Branch A stages
-- through it and then UPSERTs, so the live table is never truncated.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS ref;

CREATE TABLE IF NOT EXISTS ref.trees_cadastre (
  tree_id                        text PRIMARY KEY,
  source                         text,
  geom_2056                      geometry(Point, 2056),
  geometry                       geometry(Point, 4326),
  species                        text,
  circumference_cm               numeric,
  trunk_diameter_cm              numeric,
  crown_diameter_m               numeric,
  crown_radius_m                 numeric,
  height_total_m                 numeric,
  requires_felling_authorisation boolean,
  felling_flag_basis             text,
  is_remarquable                 boolean,
  remarquable_status             text,
  remarquable_reasons            text,
  position_status                text,
  position_precision_m           numeric,
  is_stump                       boolean,
  vitality                       text,
  development_stage              text,
  tree_class                     text,
  duplicate_of_tree_id           text,
  egrid                          text,
  commune_bfs                    integer,
  canton_code                    text,
  observed_at                    timestamptz,
  updated_at                     timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE ref.trees_cadastre IS
  'Official SITG tree register for Geneva: inventaire cantonal des arbres '
  'isolés + recensement des arbres remarquables. NOT the same population as '
  'ref.plot_trees, which is LiDAR canopy detection. Never merge the two: this '
  'one carries measured circumference, species and legal protection status and '
  'covers only inventoried trees; that one covers every detected crown with an '
  'estimated radius and no legal meaning.';

COMMENT ON COLUMN ref.trees_cadastre.requires_felling_authorisation IS
  'RCVA (rsGE L 4 05.04) art. 3 al. 2: authorisation required unless the tree '
  'is under 45 cm circumference at 1 m. TRI-STATE -- true = at/over threshold, '
  'false = under it, NULL = never measured (42% of the inventory). NULL is NOT '
  'false. And false means "below the size threshold", NOT "may be felled": '
  'authorisation stays mandatory regardless of size for trees under '
  'departmental directive, PLQ-protected vegetation, compensation plantings '
  'and art. 18A-funded plantings, none of which appear in this data.';

COMMENT ON COLUMN ref.trees_cadastre.is_remarquable IS
  'Legally remarkable, carried from source and never derived from size. '
  'Remarkability is a scored designation (six weighted criteria, >=12 of 20). '
  'Only etat = ''Approuvé remarquable'' counts: the source register also holds '
  'pending and rejected candidates.';

COMMENT ON COLUMN ref.trees_cadastre.egrid IS
  'Parcel the tree falls in. The GE cadastral layer covers public land too, so '
  'a street tree normally matches a domaine-public parcel: a match does NOT '
  'imply the tree is on private property. 239758 of 239761 match; NULL means '
  'the point fell outside every GE parcel polygon.';

COMMENT ON COLUMN ref.trees_cadastre.position_precision_m IS
  '25 m for Historique (1976 paper survey), 1 m for Relevé. NULL for Positionné '
  'and Nommé, which SITG does not document. Surface this in the UI: a 25 m '
  'point near a parcel boundary may be attributed to the wrong parcel.';

CREATE INDEX IF NOT EXISTS trees_cadastre_egrid_ix ON ref.trees_cadastre (egrid);
CREATE INDEX IF NOT EXISTS trees_cadastre_geom_gix ON ref.trees_cadastre USING GIST (geometry);
CREATE INDEX IF NOT EXISTS trees_cadastre_geom2056_gix ON ref.trees_cadastre USING GIST (geom_2056);

-- Staging twin for Branch A.
CREATE TABLE IF NOT EXISTS ref._staging_trees_cadastre
  (LIKE ref.trees_cadastre INCLUDING DEFAULTS);
