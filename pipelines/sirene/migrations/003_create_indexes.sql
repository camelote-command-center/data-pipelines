-- pipelines/sirene/migrations/003_create_indexes.sql
-- RUN THIS AFTER THE BULK LOAD COMPLETES (not before).
-- Index churn during 400k+ inserts wastes ~30 min of CPU; building once at
-- the end is faster. NOTIFY pgrst at the end to refresh the schema cache.

BEGIN;

-- bronze_fr.companies
CREATE INDEX IF NOT EXISTS idx_fr_companies_naf
  ON bronze_fr.companies (activite_principale);
CREATE INDEX IF NOT EXISTS idx_fr_companies_etat
  ON bronze_fr.companies (etat_administratif);
CREATE INDEX IF NOT EXISTS idx_fr_companies_code_commune
  ON bronze_fr.companies (code_commune);
CREATE INDEX IF NOT EXISTS idx_fr_companies_denom_trgm
  ON bronze_fr.companies USING gin (denomination gin_trgm_ops);

-- bronze_fr.etablissements
CREATE INDEX IF NOT EXISTS idx_fr_etablissements_siren
  ON bronze_fr.etablissements (siren);
CREATE INDEX IF NOT EXISTS idx_fr_etablissements_code_postal
  ON bronze_fr.etablissements (code_postal);
CREATE INDEX IF NOT EXISTS idx_fr_etablissements_code_commune
  ON bronze_fr.etablissements (code_commune);
CREATE INDEX IF NOT EXISTS idx_fr_etablissements_activite
  ON bronze_fr.etablissements (activite_principale);
CREATE INDEX IF NOT EXISTS idx_fr_etablissements_etat
  ON bronze_fr.etablissements (etat_administratif);
CREATE INDEX IF NOT EXISTS idx_fr_etablissements_geo
  ON bronze_fr.etablissements (latitude, longitude)
  WHERE latitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fr_etablissements_siege
  ON bronze_fr.etablissements (siren)
  WHERE est_siege = true;

COMMIT;

NOTIFY pgrst, 'reload schema';
