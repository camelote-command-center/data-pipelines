-- pipelines/sirene/migrations/001_create_bronze_fr_companies.sql
-- Already applied on re-LLM 2026-05-08 via the close-out session. Kept in repo
-- for reproducibility / fresh-deploy bootstrap.

BEGIN;

CREATE TABLE IF NOT EXISTS bronze_fr.companies (
  id                              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  siren                           text NOT NULL UNIQUE,
  siret_siege                     text,
  denomination                    text,
  denomination_usuelle            text,
  sigle                           text,
  forme_juridique_code            text,
  forme_juridique_libelle         text,
  activite_principale             text,
  activite_principale_libelle     text,
  activite_principale_naf25       text,
  capital_social                  numeric,
  date_immatriculation            date,
  date_creation                   date,
  date_radiation                  date,
  etat_administratif              text,
  adresse_ligne_1                 text,
  code_postal                     text,
  commune                         text,
  code_commune                    text,
  departement                     text,
  region                          text,
  latitude                        numeric,
  longitude                       numeric,
  nombre_etablissements           int,
  representants                   jsonb,
  data_source                     text DEFAULT 'insee_sirene',
  raw_data                        jsonb,
  first_seen_at                   timestamptz DEFAULT now(),
  last_seen_at                    timestamptz DEFAULT now(),
  updated_at                      timestamptz DEFAULT now()
);

GRANT SELECT ON bronze_fr.companies TO authenticated, anon;

COMMIT;
