-- ============================================================================
-- 2026-07-15 — VD zoning + RDPPF shaped for the `*_national` consumer convention
-- ============================================================================
-- Two flat, consumer-ready gold_ch exports, canton_code-stamped 'VD'. These are
-- what lamap_db loads into ref.plot_zoning_national / ref.rdppf_national.
-- FR/NE/VS land in the SAME tables later, keyed by canton_code.
-- GE stays in its own ge_* objects — untouched.
--
-- ── JOIN KEY = egrid (decided 2026-07-15, deviates from the brief) ──────────
-- The brief specified `no_commune_no_parcelle` as "the plot key used by lamap_db
-- join". It cannot work:
--   * lamap_db ref.plots.no_commune_no_parcelle is populated for 0 of 284,015 VD
--     plots — and NULL for EVERY non-GE canton. It is a GE-only convention
--     (bug c655f036); synthesising a value here yields a key that joins to nothing.
--   * Populating it for VD needs a silver cadastral_plots body change → gated on
--     f50f2e08 → explicitly out of scope.
--   * ref.ge_rdppf_synthese — the GE table this mirrors — itself keys on `egrid`
--     and has no no_commune_no_parcelle column at all.
-- egrid is 284,015/284,015 and UNIQUE on both re-LLM and lamap_db. It is the key.
-- commune_bfs + parcel_number ride along for the national canton+commune+parcel
-- convention. NOTE commune_name is NOT emitted: it is 0.44% populated on VD plots,
-- and re-LLM's ref.communes cannot fix that (its `ofs_number` is NOT the BFS —
-- VD rows run 5862-13188 and Lausanne 5586 is absent). Resolve names on lamap_db,
-- whose ref.communes IS correct (300 VD communes, 5401-5939).
--
-- Gold reads bronze_ch for the harmonised code_ch/designation_ch: established
-- practice here (7 of 21 gold_ch matviews already do, incl. core_plots_ext_vd);
-- silver_ch.cadastral_zones_vd does not carry the MGDM supertype.
--
-- ROLLBACK at the bottom of this file.
-- ============================================================================

BEGIN;
SET LOCAL statement_timeout = '1800s';
SET LOCAL lock_timeout = '60s';

-- ---------------------------------------------------------------------------
-- 1. gold_ch.export_plot_zoning_national
--    Grain: ONE row per plot, dominant affectation.
--    zone_type='affectation' AND is_dominant yields exactly 1 row per plot
--    (verified: 281,129 plots, all with n=1) — the parking overlay is
--    zone_type='stationnement_sector' and drops out here, which is how the
--    multi-dominant case (affectation + parking) resolves to the affectation row.
--    2,886 of 284,015 VD plots have no dominant affectation and are absent.
--    h_max and foret are deliberately NOT propagated — both dead upstream
--    (bug 41a643b8: h_max 0/83,671; vd_limite_foret 2,607 rows / 0 geometry, so
--    it contributes no zone_type='foret' rows at all).
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_plot_zoning_national RESTRICT;
CREATE MATERIALIZED VIEW gold_ch.export_plot_zoning_national AS
SELECT
  'VD'::text                       AS canton_code,
  l.egrid,
  cp.commune_bfs,
  cp.parcel_number,
  za.designation_vd_n2             AS zone_affectation,   -- cantonal designation (detailed)
  CASE za.code_ch / 10                                    -- MGDM 73.1 supertype = 1st digit
    WHEN 1 THEN 'zone à bâtir'
    WHEN 2 THEN 'agricole'
    WHEN 3 THEN 'à protéger'
    WHEN 4 THEN 'autres'
  END                              AS zone_primaire,
  za.designation_ch                AS zone_synthetique,   -- harmonised federal 21-class
  l.ius, l.cos, l.spb, l.cm, l.igt,
  za.statut_juridique,
  za.date_entree_vigueur::date     AS date_entree_vigueur,
  now()                            AS updated_at
FROM silver_ch.link_plot_zones_vd l
JOIN silver_ch.cadastral_plots cp
  ON cp.egrid = l.egrid AND cp.canton_code = 'VD'
JOIN bronze_ch.vd_zone_affectation za
  ON 'za_' || za.arcgis_objectid::text = l.zone_id
 AND za.deleted_at IS NULL
WHERE l.zone_type = 'affectation'
  AND l.is_dominant;

CREATE UNIQUE INDEX export_plot_zoning_national_egrid_idx
  ON gold_ch.export_plot_zoning_national (egrid);
CREATE INDEX export_plot_zoning_national_canton_idx
  ON gold_ch.export_plot_zoning_national (canton_code);
CREATE INDEX export_plot_zoning_national_commune_idx
  ON gold_ch.export_plot_zoning_national (commune_bfs);
CREATE INDEX export_plot_zoning_national_primaire_idx
  ON gold_ch.export_plot_zoning_national (zone_primaire);
COMMENT ON MATERIALIZED VIEW gold_ch.export_plot_zoning_national IS
  'Consumer-ready flat export → lamap_db ref.plot_zoning_national. Grain: one row per plot, dominant '
  'affectation. Keyed on EGRID (no_commune_no_parcelle is 0% populated for VD — GE-only convention, '
  'bug c655f036). canton_code-stamped; FR/NE/VS join the same table later. 3 zoning tiers: '
  'zone_affectation = cantonal detail, zone_synthetique = harmonised federal 21-class (designation_ch), '
  'zone_primaire = federal 4-way supertype (code_ch first digit). h_max/foret excluded — dead upstream (41a643b8).';
COMMENT ON COLUMN gold_ch.export_plot_zoning_national.zone_primaire IS
  'MGDM 73.1 supertype from code_ch first digit: 1x=zone à bâtir, 2x=agricole, 3x=à protéger, 4x=autres.';

-- ---------------------------------------------------------------------------
-- 2. gold_ch.export_rdppf_national
--    Grain: ONE row per plot × theme × sous_type (link rows are per polygon, so
--    several polygons of the same sous_type on one plot are aggregated:
--    overlap_m2 = SUM, libelle/statut = value from the LARGEST-overlap polygon).
--
--    is_restrictive semantics — read before changing:
--      The ONLY non-restrictive rows are the groundwater secteur/aire blanket.
--      protection_eaux `secteur Au` (112,498 plots) + `üB` (183,784) cover the
--      canton BY CONSTRUCTION — that is what drives the 99.99% headline. It is a
--      CLASSIFICATION, not a restriction. is_restrictive=true only for the real
--      zones S1/S2/S3 → 1,532 / 6,499 / 12,812 plots, exactly the numbers the
--      frontend must surface.
--      zones_reservees (LAT art.27 building freeze) and sites_pollues (KbS legal
--      encumbrance) have no blanket — every row is a genuine restriction ⇒ true.
--    ⚠️ FLAGGED, deliberately left is_restrictive=false per brief: `zone/Périmètre`
--      (107 polygons, 2,807 plots) is the Grundwasserschutzareal / périmètre de
--      protection des eaux souterraines — arguably a genuine building restriction
--      (land reserved for future water capture), NOT part of the Au/üB blanket.
--      `secteur/S` (17,620 plots) is the envelope around the S1/S2/S3 groups.
--      Both are candidates for a later is_restrictive review.
--    Join key note: eaux + sites carry NULL commune_bfs at bronze (NO_COMMUNE ≠ BFS,
--    bug 90c20178) — so commune_bfs here comes from the PLOT, never the theme.
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_rdppf_national RESTRICT;
CREATE MATERIALIZED VIEW gold_ch.export_rdppf_national AS
WITH unioned AS (
  -- zones réservées (LAT art.27 planning freeze) — always restrictive
  SELECT l.egrid,
         'zones_reservees'::text            AS theme,
         nullif(l.zone_kind, '')            AS sous_type,   -- 29% populated; NULL not faked
         true                               AS is_restrictive,
         l.zone_name                        AS libelle,
         l.overlap_m2,
         l.statut_juridique,
         l.date_entree_vigueur::date        AS date_entree_vigueur
  FROM silver_ch.link_plot_zones_reservees_vd l

  UNION ALL

  -- protection des eaux souterraines — restrictive ONLY for zones S1/S2/S3
  SELECT l.egrid,
         'protection_eaux',
         l.indice_protection,                              -- S1/S2/S3 | Périmètre | Au/S/üB | Zu
         (l.protection_kind = 'zone' AND l.indice_protection IN ('S1','S2','S3')),
         CASE l.protection_kind
           WHEN 'zone'    THEN 'Zone de protection des eaux '    || l.indice_protection
           WHEN 'secteur' THEN 'Secteur de protection des eaux '  || l.indice_protection
           WHEN 'aire'    THEN 'Aire d''alimentation '            || l.indice_protection
         END,
         l.overlap_m2,
         NULL::text,                                       -- source carries no statut juridique
         l.date_acceptation::date
  FROM silver_ch.link_plot_protection_eaux_vd l

  UNION ALL

  -- sites pollués (KbS) — every entry is a legal encumbrance ⇒ always restrictive
  SELECT l.egrid,
         'sites_pollues',
         l.type_site,                                      -- Décharge/Remblai | Aire d'exploitation | ...
         true,
         coalesce(nullif(l.nom_site,''), nullif(s.activite,''), l.type_site),
         l.overlap_m2,
         l.nom_phase,                                      -- the KbS legal status
         NULL::date                                        -- source carries no date
  FROM silver_ch.link_plot_sites_pollues_vd l
  JOIN silver_ch.sites_pollues_vd s ON s.id = l.site_pollue_id
)
SELECT
  'VD'::text                                   AS canton_code,
  u.egrid,
  cp.commune_bfs,
  cp.parcel_number,
  u.theme,
  u.sous_type,
  bool_or(u.is_restrictive)                    AS is_restrictive,
  (array_agg(u.libelle ORDER BY u.overlap_m2 DESC NULLS LAST))[1]          AS libelle,
  sum(u.overlap_m2)::numeric                   AS overlap_m2,
  (array_agg(u.statut_juridique ORDER BY u.overlap_m2 DESC NULLS LAST))[1] AS statut_juridique,
  max(u.date_entree_vigueur)                   AS date_entree_vigueur,
  now()                                        AS updated_at
FROM unioned u
JOIN silver_ch.cadastral_plots cp
  ON cp.egrid = u.egrid AND cp.canton_code = 'VD'
GROUP BY u.egrid, cp.commune_bfs, cp.parcel_number, u.theme, u.sous_type;

-- NULLS NOT DISTINCT: sous_type is legitimately NULL (zones_reservees zone_kind is
-- 71% empty) and the grain treats those as ONE group — the index must agree.
CREATE UNIQUE INDEX export_rdppf_national_grain_idx
  ON gold_ch.export_rdppf_national (egrid, theme, sous_type) NULLS NOT DISTINCT;
CREATE INDEX export_rdppf_national_canton_idx      ON gold_ch.export_rdppf_national (canton_code);
CREATE INDEX export_rdppf_national_egrid_idx       ON gold_ch.export_rdppf_national (egrid);
CREATE INDEX export_rdppf_national_theme_idx       ON gold_ch.export_rdppf_national (theme);
CREATE INDEX export_rdppf_national_restrictive_idx ON gold_ch.export_rdppf_national (is_restrictive) WHERE is_restrictive;
CREATE INDEX export_rdppf_national_commune_idx     ON gold_ch.export_rdppf_national (commune_bfs);
COMMENT ON MATERIALIZED VIEW gold_ch.export_rdppf_national IS
  'Consumer-ready flat export → lamap_db ref.rdppf_national. Grain: one row per plot × theme × sous_type '
  '(overlap_m2 SUMmed across polygons of the same sous_type). Keyed on EGRID. '
  '⚠️ ALWAYS filter is_restrictive=true for "does this parcel carry a restriction". The 99.99% '
  'protection_eaux coverage is the secteur Au/üB CLASSIFICATION blanket, not a restriction; the real '
  'groundwater restriction is S1/S2/S3 = 1,532/6,499/12,812 plots. Noise + servitudes absent — gated on f50f2e08.';
COMMENT ON COLUMN gold_ch.export_rdppf_national.is_restrictive IS
  'true = genuine restriction on this parcel. false ONLY for the protection_eaux secteur/aire blanket '
  '(Au/üB/S/Zu) and zone/Périmètre. NOTE: zone/Périmètre (Grundwasserschutzareal, 2,807 plots) is '
  'arguably restrictive and is false only because the 2026-07-15 brief scoped is_restrictive to S1/S2/S3 — '
  'flagged for review.';

GRANT SELECT ON gold_ch.export_plot_zoning_national, gold_ch.export_rdppf_national
  TO anon, authenticated, service_role;

COMMIT;

-- ============================================================================
-- ROLLBACK
-- ============================================================================
-- BEGIN;
--   DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_plot_zoning_national RESTRICT;
--   DROP MATERIALIZED VIEW IF EXISTS gold_ch.export_rdppf_national       RESTRICT;
--   DELETE FROM supabase_migrations.schema_migrations WHERE version = '20260715000003';
-- COMMIT;
-- ============================================================================
