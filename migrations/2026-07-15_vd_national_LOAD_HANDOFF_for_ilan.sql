-- ============================================================================
-- HANDOFF — NOT RUN BY CC. For Ilan / Claude-via-MCP to execute ON lamap_db.
-- ============================================================================
-- CC's brief scoped it to re-LLM: "do not create lamap_db objects", "No lamap_db
-- writes". So this file is a ready-to-run proposal, deliberately NOT executed.
--
-- Verified on lamap_db 2026-07-15 by CC (read-only):
--   * `dblink` 1.2 IS installed (schema public).
--   * There is NO FDW to re-LLM (only auth_sync_crm_server / auth_sync_lbi_server,
--     which point lamap_db -> crm/lbi). So option (b) = dblink pull.
--   * Neither ref.plot_zoning_national nor ref.rdppf_national exists yet.
--   * Payload: export_plot_zoning_national 59 MB / 281,129 rows;
--              export_rdppf_national 92 MB / 369,600 rows.
--
-- ── KEY DECISION BAKED IN: the join key is EGRID, not no_commune_no_parcelle ──
-- lamap_db ref.plots.no_commune_no_parcelle is populated for 0 of 284,015 VD
-- plots (GE-only convention, bug c655f036; VD fix gated on f50f2e08). egrid is
-- 284,015/284,015 and UNIQUE on both sides, and is what ref.ge_rdppf_synthese —
-- the GE table this mirrors — already keys on.
-- ⚠️ Do NOT join these tables to ref.plots on no_commune_no_parcelle: it returns
-- zero rows for VD, silently.
--
-- ⚠️ NAMING NOTE for Ilan: the 3 existing national tables (transactions_national,
-- sad_national, owners_national) use column `canton` (values 'VD','FR','NE','VS'),
-- NOT `canton_code`. The 2026-07-15 brief specified `canton_code` for these two,
-- which is also the re-LLM convention. Pick one and stay consistent — flagged,
-- not decided by CC.
-- ============================================================================


-- ── STEP 1: create the targets (Claude owns these) ─────────────────────────
CREATE TABLE IF NOT EXISTS ref.plot_zoning_national (
  canton_code          text        NOT NULL,
  egrid                text        NOT NULL,
  commune_bfs          integer,
  parcel_number        text,
  zone_affectation     text,          -- cantonal designation (detailed)
  zone_primaire        text,          -- federal 4-way supertype
  zone_synthetique     text,          -- harmonised federal 21-class
  ius                  numeric,
  cos                  numeric,
  spb                  numeric,
  cm                   numeric,
  igt                  numeric,
  statut_juridique     text,
  date_entree_vigueur  date,
  updated_at           timestamptz   NOT NULL DEFAULT now(),
  PRIMARY KEY (egrid)
);
CREATE INDEX IF NOT EXISTS plot_zoning_national_canton_idx   ON ref.plot_zoning_national (canton_code);
CREATE INDEX IF NOT EXISTS plot_zoning_national_commune_idx  ON ref.plot_zoning_national (commune_bfs);
CREATE INDEX IF NOT EXISTS plot_zoning_national_primaire_idx ON ref.plot_zoning_national (zone_primaire);
COMMENT ON TABLE ref.plot_zoning_national IS
  'Non-GE plot zoning, canton_code-keyed. VD first (2026-07-15); FR/NE/VS land here too. GE stays in ge_*. '
  'Joins ref.plots on EGRID — NOT on no_commune_no_parcelle (0% populated for non-GE, bug c655f036).';

CREATE TABLE IF NOT EXISTS ref.rdppf_national (
  canton_code          text        NOT NULL,
  egrid                text        NOT NULL,
  commune_bfs          integer,
  parcel_number        text,
  theme                text        NOT NULL,   -- zones_reservees | protection_eaux | sites_pollues
  sous_type            text,
  is_restrictive       boolean     NOT NULL,
  libelle              text,
  overlap_m2           numeric,
  statut_juridique     text,
  date_entree_vigueur  date,
  updated_at           timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS rdppf_national_grain_idx
  ON ref.rdppf_national (egrid, theme, sous_type) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS rdppf_national_canton_idx      ON ref.rdppf_national (canton_code);
CREATE INDEX IF NOT EXISTS rdppf_national_egrid_idx       ON ref.rdppf_national (egrid);
CREATE INDEX IF NOT EXISTS rdppf_national_theme_idx       ON ref.rdppf_national (theme);
CREATE INDEX IF NOT EXISTS rdppf_national_restrictive_idx ON ref.rdppf_national (is_restrictive) WHERE is_restrictive;
COMMENT ON TABLE ref.rdppf_national IS
  'Non-GE plot RDPPF restrictions, one row per plot x theme x sous_type. VD first (2026-07-15). '
  'ALWAYS filter is_restrictive=true for "does this parcel carry a restriction" — protection_eaux covers '
  '99.99% of VD via the secteur Au/uB CLASSIFICATION blanket; the real groundwater restriction is '
  'S1/S2/S3 = 15,323 distinct plots (5.40% of VD).';


-- ── STEP 2: dblink pull from re-LLM (UPSERT, additive, never TRUNCATE) ─────
-- Connection string: take re-llm `session_pooler_uri` from
--   ~/supabase-registry/supabase-projects.json
-- and pass it inline below. DO NOT hardcode it into any repo file.
-- Recommended: \set conn `jq -r '.["re-llm"].session_pooler_uri' ...` then :'conn'.

-- \set conn 'postgresql://postgres.znrvddgmczdqoucmykij:<pw>@aws-<n>-<region>.pooler.supabase.com:5432/postgres'

BEGIN;
SET LOCAL statement_timeout = '1800s';

INSERT INTO ref.plot_zoning_national (
  canton_code, egrid, commune_bfs, parcel_number, zone_affectation, zone_primaire,
  zone_synthetique, ius, cos, spb, cm, igt, statut_juridique, date_entree_vigueur, updated_at)
SELECT * FROM dblink(:'conn', $q$
  SELECT canton_code, egrid, commune_bfs, parcel_number, zone_affectation, zone_primaire,
         zone_synthetique, ius, cos, spb, cm, igt, statut_juridique, date_entree_vigueur, updated_at
  FROM gold_ch.export_plot_zoning_national
$q$) AS t(canton_code text, egrid text, commune_bfs integer, parcel_number text,
          zone_affectation text, zone_primaire text, zone_synthetique text,
          ius numeric, cos numeric, spb numeric, cm numeric, igt numeric,
          statut_juridique text, date_entree_vigueur date, updated_at timestamptz)
ON CONFLICT (egrid) DO UPDATE SET
  canton_code=EXCLUDED.canton_code, commune_bfs=EXCLUDED.commune_bfs,
  parcel_number=EXCLUDED.parcel_number, zone_affectation=EXCLUDED.zone_affectation,
  zone_primaire=EXCLUDED.zone_primaire, zone_synthetique=EXCLUDED.zone_synthetique,
  ius=EXCLUDED.ius, cos=EXCLUDED.cos, spb=EXCLUDED.spb, cm=EXCLUDED.cm, igt=EXCLUDED.igt,
  statut_juridique=EXCLUDED.statut_juridique, date_entree_vigueur=EXCLUDED.date_entree_vigueur,
  updated_at=EXCLUDED.updated_at;

INSERT INTO ref.rdppf_national (
  canton_code, egrid, commune_bfs, parcel_number, theme, sous_type, is_restrictive,
  libelle, overlap_m2, statut_juridique, date_entree_vigueur, updated_at)
SELECT * FROM dblink(:'conn', $q$
  SELECT canton_code, egrid, commune_bfs, parcel_number, theme, sous_type, is_restrictive,
         libelle, overlap_m2, statut_juridique, date_entree_vigueur, updated_at
  FROM gold_ch.export_rdppf_national
$q$) AS t(canton_code text, egrid text, commune_bfs integer, parcel_number text,
          theme text, sous_type text, is_restrictive boolean, libelle text,
          overlap_m2 numeric, statut_juridique text, date_entree_vigueur date,
          updated_at timestamptz)
ON CONFLICT (egrid, theme, sous_type) DO UPDATE SET
  canton_code=EXCLUDED.canton_code, commune_bfs=EXCLUDED.commune_bfs,
  parcel_number=EXCLUDED.parcel_number, is_restrictive=EXCLUDED.is_restrictive,
  libelle=EXCLUDED.libelle, overlap_m2=EXCLUDED.overlap_m2,
  statut_juridique=EXCLUDED.statut_juridique, date_entree_vigueur=EXCLUDED.date_entree_vigueur,
  updated_at=EXCLUDED.updated_at;

COMMIT;


-- ── STEP 3: expected counts (assert these after the load) ──────────────────
--   ref.plot_zoning_national                                  = 281,129
--     zone à bâtir 159,592 | agricole 68,189 | autres 45,275 | à protéger 8,073
--   ref.rdppf_national                                        = 369,600
--     protection_eaux 341,253 rows / 283,989 plots (20,843 restrictive rows)
--     zones_reservees  17,664 rows /  17,444 plots (all restrictive)
--     sites_pollues    10,683 rows /  10,299 plots (all restrictive)
--   is_restrictive=true, protection_eaux, DISTINCT plots      = 15,323  (5.40% of VD)
--     S1 1,532 | S2 6,499 | S3 12,812   (a plot can be in >1 S-zone; rows 20,843 > plots 15,323)
--   is_restrictive=false blanket: üB 183,784 | Au 112,498 | S 17,620 | Zu 3,701 | Périmètre 2,807
--
-- ⚠️ RPC canton-branching: GE -> ge_* objects; everything else -> *_national by
-- canton_code. VD is the first non-GE canton in these two tables.
-- ⚠️ Noise + servitudes are ABSENT (still 0 rows upstream, gated on f50f2e08) —
-- the frontend must not imply a full RDPPF extract for VD yet.
-- ============================================================================
