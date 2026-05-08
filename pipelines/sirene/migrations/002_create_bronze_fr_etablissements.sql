-- pipelines/sirene/migrations/002_create_bronze_fr_etablissements.sql
-- Already applied on re-LLM 2026-05-08.

BEGIN;

CREATE TABLE IF NOT EXISTS bronze_fr.etablissements (
  id                              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  siret                           text NOT NULL UNIQUE,
  siren                           text NOT NULL,
  nic                             text,
  denomination                    text,
  enseigne                        text,
  activite_principale             text,
  activite_principale_libelle     text,
  activite_principale_naf25       text,
  numero_voie                     text,
  type_voie                       text,
  libelle_voie                    text,
  complement_adresse              text,
  code_postal                     text,
  commune                         text,
  code_commune                    text,
  departement                     text,
  region                          text,
  latitude                        numeric,
  longitude                       numeric,
  etat_administratif              text,
  date_creation                   date,
  date_debut_activite             date,
  date_fermeture                  date,
  est_siege                       boolean DEFAULT false,
  tranche_effectifs               text,
  tranche_effectifs_libelle       text,
  caractere_employeur             text,
  data_source                     text DEFAULT 'insee_sirene',
  raw_data                        jsonb,
  first_seen_at                   timestamptz DEFAULT now(),
  last_seen_at                    timestamptz DEFAULT now(),
  updated_at                      timestamptz DEFAULT now()
);

GRANT SELECT ON bronze_fr.etablissements TO authenticated, anon;

COMMIT;
