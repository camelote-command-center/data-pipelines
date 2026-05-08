-- ============================================================
-- bronze_ch.ge_cad_bati_projet — SITG planned/under-construction buildings
-- Source:    https://vector.sitg.ge.ch/arcgis/rest/services/Hosted/cad_bati_projet/FeatureServer/0
-- Cadence:   monthly (sitg_cadastral.yml schedule)
-- Soft-del:  last_seen_at refreshed every sync; deleted_at set when row absent from source
--            (per source page: rows are archived to ge_cad_batiments_histo after construction)
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze_ch.ge_cad_bati_projet (
  id                BIGSERIAL PRIMARY KEY,
  objectid          INTEGER NOT NULL,
  globalid          TEXT,
  egid              INTEGER,
  no_autor          TEXT,
  categorie_batpro  TEXT,
  nombat            TEXT,
  destination       TEXT,
  nomenclature      TEXT,
  nomen_classe      TEXT,
  niveaux_horsol    SMALLINT,
  niveaux_ssol      SMALLINT,
  hauteur           DOUBLE PRECISION,
  -- Raw epoch milliseconds from SITG (e.g. 1397212149000). Convert with to_timestamp(datedt/1000.0).
  datedt            BIGINT,
  type              TEXT,
  genre             TEXT,
  commune           TEXT,
  shape__length     DOUBLE PRECISION,
  shape__area       DOUBLE PRECISION,
  geometry          TEXT,
  geom              geometry(Geometry, 4326),
  first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at        TIMESTAMPTZ NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN bronze_ch.ge_cad_bati_projet.deleted_at IS
  'Set by parser when objectid is no longer present in the SITG source layer. Per the SITG description, these rows have been archived to bronze_ch.ge_cad_batiments_histo after construction. Soft-delete preserves history; never hard-delete.';
COMMENT ON COLUMN bronze_ch.ge_cad_bati_projet.last_seen_at IS
  'Touched on every successful UPSERT. Used to detect rows missing from source: post-sync, rows where last_seen_at < <sync_started_at> are flagged deleted_at=NOW().';

-- Conflict key
CREATE UNIQUE INDEX IF NOT EXISTS uq_ge_cad_bati_projet_objectid
  ON bronze_ch.ge_cad_bati_projet (objectid);

-- Lookup indexes
CREATE INDEX IF NOT EXISTS ix_ge_cad_bati_projet_commune
  ON bronze_ch.ge_cad_bati_projet (commune);
CREATE INDEX IF NOT EXISTS ix_ge_cad_bati_projet_type
  ON bronze_ch.ge_cad_bati_projet (type);
CREATE INDEX IF NOT EXISTS ix_ge_cad_bati_projet_no_autor
  ON bronze_ch.ge_cad_bati_projet (no_autor);
CREATE INDEX IF NOT EXISTS ix_ge_cad_bati_projet_egid
  ON bronze_ch.ge_cad_bati_projet (egid) WHERE egid IS NOT NULL;

-- Soft-delete + sync tracking
CREATE INDEX IF NOT EXISTS ix_ge_cad_bati_projet_last_seen_at
  ON bronze_ch.ge_cad_bati_projet (last_seen_at);
CREATE INDEX IF NOT EXISTS ix_ge_cad_bati_projet_deleted_at
  ON bronze_ch.ge_cad_bati_projet (deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_ge_cad_bati_projet_active
  ON bronze_ch.ge_cad_bati_projet (objectid) WHERE deleted_at IS NULL;

-- Spatial GIST (per ABSOLUTE RULE)
CREATE INDEX IF NOT EXISTS ix_ge_cad_bati_projet_geom_gist
  ON bronze_ch.ge_cad_bati_projet USING GIST (geom);

-- Auto-populate geom from raw ArcGIS JSON (per ABSOLUTE RULE)
DROP TRIGGER IF EXISTS trg_ge_cad_bati_projet_geom ON bronze_ch.ge_cad_bati_projet;
CREATE TRIGGER trg_ge_cad_bati_projet_geom
BEFORE INSERT OR UPDATE OF geometry ON bronze_ch.ge_cad_bati_projet
FOR EACH ROW EXECUTE FUNCTION bronze_ch._sync_geom_from_geometry();
