-- SITG Chantiers — bronze_ch tables on re-llm
-- Datasets:
--   1. INFOMOB_CHANTIER_POINT  — public-facing real-time high-impact construction sites (points)
--   2. PCMOB_CHANTIER_CONSULT  — full PCM coordination platform (planned + ongoing, polygons)
--
-- Field names use snake_case (produced by shared/sitg_arcgis.fetch_all_features).
-- All ArcGIS fields kept as TEXT for resilience (mirrors existing ge_sit_* tables).
-- geometry stored as TEXT (JSON string in WGS84/EPSG:4326).

CREATE SCHEMA IF NOT EXISTS bronze_ch;

-- ──────────────────────────────────────────────────────────────
-- 1. INFOMOB_CHANTIER_POINT (public-facing, points, ~85 rows)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze_ch.ge_infomob_chantier_point (
  id              BIGSERIAL PRIMARY KEY,
  objectid        TEXT NOT NULL UNIQUE,
  date_debut      TEXT,
  date_fin        TEXT,
  duree           TEXT,
  adresse         TEXT,
  type            TEXT,
  fiche_info      TEXT,
  hierarchie      TEXT,
  perturbation    TEXT,
  impact_global   TEXT,
  label_pcm       TEXT,
  moa             TEXT,
  date_statut     TEXT,
  origine_data    TEXT,
  zoom            TEXT,
  geometry        TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_infomob_chantier_point_date_debut
  ON bronze_ch.ge_infomob_chantier_point (date_debut);
CREATE INDEX IF NOT EXISTS idx_infomob_chantier_point_moa
  ON bronze_ch.ge_infomob_chantier_point (moa);

-- ──────────────────────────────────────────────────────────────
-- 2. PCMOB_CHANTIER_CONSULT (PCM coordination, polygons, ~201 rows)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze_ch.ge_pcmob_chantier_consult (
  id                 BIGSERIAL PRIMARY KEY,
  objectid           TEXT NOT NULL UNIQUE,
  nom                TEXT,
  type_chantier      TEXT,
  id_chantier_moa    TEXT,
  commune            TEXT,
  quartier           TEXT,
  voie_nom           TEXT,
  nature_travaux     TEXT,
  impact_remarque    TEXT,
  date_debut         TEXT,
  date_fin           TEXT,
  date_duree         TEXT,
  date_remarque      TEXT,
  horizon_temporel   TEXT,
  maitrise_ouvrage   TEXT,
  info_url           TEXT,
  numero_pcm         TEXT,
  remarque_chantier  TEXT,
  shape__area        TEXT,
  shape__length      TEXT,
  geometry           TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pcmob_chantier_consult_commune
  ON bronze_ch.ge_pcmob_chantier_consult (commune);
CREATE INDEX IF NOT EXISTS idx_pcmob_chantier_consult_date_debut
  ON bronze_ch.ge_pcmob_chantier_consult (date_debut);
CREATE INDEX IF NOT EXISTS idx_pcmob_chantier_consult_horizon
  ON bronze_ch.ge_pcmob_chantier_consult (horizon_temporel);

-- updated_at triggers (mirror pattern of other ge_sit_* tables)
CREATE OR REPLACE FUNCTION bronze_ch._touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_infomob_chantier_point_touch ON bronze_ch.ge_infomob_chantier_point;
CREATE TRIGGER trg_infomob_chantier_point_touch
  BEFORE UPDATE ON bronze_ch.ge_infomob_chantier_point
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._touch_updated_at();

DROP TRIGGER IF EXISTS trg_pcmob_chantier_consult_touch ON bronze_ch.ge_pcmob_chantier_consult;
CREATE TRIGGER trg_pcmob_chantier_consult_touch
  BEFORE UPDATE ON bronze_ch.ge_pcmob_chantier_consult
  FOR EACH ROW EXECUTE FUNCTION bronze_ch._touch_updated_at();
