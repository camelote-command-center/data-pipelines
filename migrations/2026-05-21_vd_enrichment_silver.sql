-- ============================================================================
-- VD/Lausanne enrichment — SILVER migration
-- ============================================================================
-- Target DB: re-LLM
-- Scope    : 10 _vd matviews + 3 national new matviews
-- Pattern  : Mirror existing silver_ch.cadastral_*_ge / national shapes. canton_code
--            mandatory. Unique index built before any REFRESH CONCURRENTLY.
-- Pre-reqs : 2026-05-21_vd_enrichment_bronze.sql applied AND parsers have ingested
--            at least one batch into bronze (matview WITH NO DATA otherwise creates
--            empty matview that REFRESH CONCURRENTLY will fail on if columns are NULL).
--            => Two-phase apply: this migration creates matviews WITH NO DATA;
--               first REFRESH happens AFTER bronze backfill.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. silver_ch.cadastral_buildings_vd
--    Mirror of cadastral_buildings_ge shape; EGID-keyed aggregation.
--    Delta vs _ge: ADDS heating cols (full chauf1/2 + eau1/2 sys+nrg), SRE m2,
--                  abri_pci, categorie_txt (raw, normalization deferred).
--    Source: bronze_ch.vd_batiment_rcb (filtered statut_txt != 'demoli' etc.)
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_buildings_vd AS
SELECT
  b.egid,
  -- Aggregations (one row per egid, since RCB exposes one feature per building)
  sum(b.surface_m2 * COALESCE(NULLIF(b.nb_niv_tot, 0), 1))::numeric AS total_floor_area_m2,
  NULL::numeric                                                     AS total_volume_m3,         -- RCB doesn't expose volume
  count(*)::integer                                                  AS buildings_count_with_sbp_status,
  min(b.cons_annee) FILTER (WHERE b.cons_annee >= 1000 AND b.cons_annee <= extract(year FROM CURRENT_DATE)::int) AS oldest_construction_year,
  max(b.cons_annee) FILTER (WHERE b.cons_annee >= 1000 AND b.cons_annee <= extract(year FROM CURRENT_DATE)::int) AS newest_construction_year,
  mode() WITHIN GROUP (ORDER BY b.chauf1_sys_txt) AS predominant_heating_type,
  mode() WITHIN GROUP (ORDER BY b.chauf1_nrg_txt) AS predominant_heating_energy,
  mode() WITHIN GROUP (ORDER BY b.classe_txt)    AS predominant_building_class,
  mode() WITHIN GROUP (ORDER BY b.categorie_txt) AS predominant_building_category,
  -- VD-only enrichment columns
  sum(b.sre_m2)::numeric                          AS total_sre_m2,
  mode() WITHIN GROUP (ORDER BY b.eau1_nrg_txt)   AS predominant_water_heating_energy,
  bool_or(b.abri_pci IS NOT NULL AND b.abri_pci <> '') AS has_pci_shelter,
  'VD'::text                                       AS canton_code,
  now()                                            AS updated_at
FROM bronze_ch.vd_batiment_rcb b
WHERE b.deleted_at IS NULL
  AND b.egid IS NOT NULL AND b.egid <> ''
  AND COALESCE(b.statut_txt, '') NOT IN ('demoli','demolished','projet')   -- exclude non-existing buildings
GROUP BY b.egid
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_buildings_vd IS
  'VD canton-wide buildings per EGID. Source: bronze_ch.vd_batiment_rcb. '
  'Schema delta vs cadastral_buildings_ge: ADDS total_sre_m2, predominant_water_heating_energy, '
  'has_pci_shelter, predominant_building_category. RCB richer than RegBL.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_buildings_vd_egid_idx
  ON silver_ch.cadastral_buildings_vd (egid);


-- ----------------------------------------------------------------------------
-- 2. silver_ch.cadastral_zones_vd
--    UNION of: vd_zone_affectation (zone_type='affectation'),
--              vd_limite_foret    (zone_type='foret'),
--              vd_lausanne_parking_sectors (zone_type='stationnement_sector')
--    Mirror of cadastral_zones shape. canton_code='VD'.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_zones_vd AS
SELECT
  ('za_' || za.arcgis_objectid::text) AS id,
  za.code_vd_n2                       AS zone_code,
  za.designation_vd_n2                AS zone_name,
  'affectation'::text                  AS zone_type,
  st_transform(za.geometry, 2056)      AS geometry,
  'VD'::text                           AS canton_code,
  jsonb_build_object(
    'ius', za.ius, 'cos', za.cos, 'spb', za.spb, 'cm', za.cm, 'igt', za.igt,
    'h_max', za.h_max, 'statut_juridique', za.statut_juridique,
    'date_entree_vigueur', za.date_entree_vigueur,
    'commune', za.designation_com, 'sous_theme', za.sous_theme,
    'surface_m2', za.surface_m2, 'perimetre_m', za.perimetre_m
  ) AS raw_data,
  now() AS updated_at
FROM bronze_ch.vd_zone_affectation za
WHERE za.deleted_at IS NULL
UNION ALL
SELECT
  ('lf_' || lf.arcgis_objectid::text) AS id,
  NULL::text                           AS zone_code,
  COALESCE((lf.raw_data->>'nom_zone'), 'Forêt') AS zone_name,
  'foret'::text                        AS zone_type,
  lf.geometry,
  'VD'::text                           AS canton_code,
  lf.raw_data,
  now() AS updated_at
FROM bronze_ch.vd_limite_foret lf
WHERE lf.deleted_at IS NULL
UNION ALL
SELECT
  ('ps_' || ps.source_pk)              AS id,
  ps.secteur                           AS zone_code,
  ('Secteur stationnement ' || ps.secteur) AS zone_name,
  'stationnement_sector'::text         AS zone_type,
  ps.geometry,
  'VD'::text                           AS canton_code,
  jsonb_build_object('source','lausanne','secteur', ps.secteur) AS raw_data,
  now() AS updated_at
FROM bronze_ch.vd_lausanne_parking_sectors ps
WHERE ps.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_zones_vd IS
  'VD canton-wide zones unified: PGA affectation (with IUS/COS/SPB in raw_data), forest limits, '
  'and Lausanne parking sectors. Mirror of cadastral_zones shape. Discriminator: zone_type.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_zones_vd_id_idx ON silver_ch.cadastral_zones_vd (id);
CREATE INDEX        IF NOT EXISTS cadastral_zones_vd_geom_idx ON silver_ch.cadastral_zones_vd USING GIST (geometry);
CREATE INDEX        IF NOT EXISTS cadastral_zones_vd_zonetype_idx ON silver_ch.cadastral_zones_vd (zone_type);


-- ----------------------------------------------------------------------------
-- 3. silver_ch.cadastral_chantiers_vd
--    From vd_batiment_projete. Mirror of cadastral_chantiers shape.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_chantiers_vd AS
SELECT
  bp.arcgis_objectid::integer          AS id,
  COALESCE((bp.raw_data->>'description'), 'Bâtiment projeté') AS description,
  NULL::date                            AS date_debut,
  NULL::date                            AS date_fin,
  'VD'::text                            AS canton_code,
  bp.geometry,
  now() AS updated_at
FROM bronze_ch.vd_batiment_projete bp
WHERE bp.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_chantiers_vd IS
  'VD canton-wide projected/in-study buildings. Source: bronze_ch.vd_batiment_projete.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_chantiers_vd_id_idx ON silver_ch.cadastral_chantiers_vd (id);
CREATE INDEX        IF NOT EXISTS cadastral_chantiers_vd_geom_idx ON silver_ch.cadastral_chantiers_vd USING GIST (geometry);


-- ----------------------------------------------------------------------------
-- 4. silver_ch.cadastral_ddp_vd
--    UNION of vd_ddp_pts (canton multipoint) + vd_lausanne_ddp (Lausanne polygon).
--    Mirror of cadastral_ddp + cadastral_ddp_geo concept.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_ddp_vd AS
SELECT
  ('pt_' || dp.arcgis_objectid::text) AS id,
  dp.egris_egrid                      AS egrid,
  dp.numero                           AS no_ddp,
  NULL::text                           AS no_commune_no_parcelle,
  dp.genre_txt                        AS genre,
  NULL::numeric                        AS surface_ddp_m2,
  NULL::jsonb                          AS ddp_owners,
  NULL::jsonb                          AS batiments,
  'valid'::text                        AS validite,
  dp.geometry                          AS geometry,
  'point'::text                        AS source_geometry_kind,
  'VD'::text                           AS canton_code,
  now() AS updated_at
FROM bronze_ch.vd_ddp_pts dp
WHERE dp.deleted_at IS NULL
UNION ALL
SELECT
  ('lau_' || ld.source_pk)             AS id,
  NULL::text                           AS egrid,
  ld.no_parc                           AS no_ddp,
  (ld.no_commune::text || '/' || ld.no_parc) AS no_commune_no_parcelle,
  ld.type_txt                          AS genre,
  ld.surface_m2                        AS surface_ddp_m2,
  CASE WHEN ld.proprietaire IS NOT NULL
       THEN jsonb_build_array(jsonb_build_object('name', ld.proprietaire))
       ELSE NULL::jsonb END             AS ddp_owners,
  NULL::jsonb                          AS batiments,
  'valid'::text                        AS validite,
  ld.geometry                          AS geometry,
  'polygon'::text                      AS source_geometry_kind,
  'VD'::text                           AS canton_code,
  now() AS updated_at
FROM bronze_ch.vd_lausanne_ddp ld
WHERE ld.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_ddp_vd IS
  'VD DDP records — canton multipoint (egrid-keyed) UNION Lausanne polygons (parcel-keyed). '
  'Mirrors cadastral_ddp / cadastral_ddp_geo combined.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_ddp_vd_id_idx ON silver_ch.cadastral_ddp_vd (id);
CREATE INDEX        IF NOT EXISTS cadastral_ddp_vd_egrid_idx ON silver_ch.cadastral_ddp_vd (egrid);
CREATE INDEX        IF NOT EXISTS cadastral_ddp_vd_ncnp_idx  ON silver_ch.cadastral_ddp_vd (no_commune_no_parcelle);
CREATE INDEX        IF NOT EXISTS cadastral_ddp_vd_geom_idx  ON silver_ch.cadastral_ddp_vd USING GIST (geometry);


-- ----------------------------------------------------------------------------
-- 5. silver_ch.cadastral_plots_vd
--    DP-only complement to federal_cadastral_parcels (which excludes DP).
--    Mirror of cadastral_plots shape, but VD-DP-only subset.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_plots_vd AS
SELECT
  ('dp_' || ldp.source_pk)             AS egrid,    -- DP plots don't have EGRID; use synthetic
  'VD'::text                            AS canton_code,
  'Vaud'::text                          AS canton_name,
  ldp.no_commune                        AS commune_bfs,
  ldp.commune_name                      AS commune_name,
  ldp.no_parc                           AS parcel_number,
  ldp.no_commune::text                  AS no_commune,
  ldp.no_parc                           AS no_parcelle,
  (ldp.no_commune::text || '/' || ldp.no_parc) AS no_commune_no_parcelle,
  ldp.surface_m2                        AS surface_m2,
  ldp.geometry                          AS geometry,
  st_centroid(ldp.geometry)             AS centroid,
  st_x(st_transform(st_centroid(ldp.geometry), 2056))::double precision AS lv95_e,
  st_y(st_transform(st_centroid(ldp.geometry), 2056))::double precision AS lv95_n,
  st_y(st_transform(st_centroid(ldp.geometry), 4326))::double precision AS wgs84_lat,
  st_x(st_transform(st_centroid(ldp.geometry), 4326))::double precision AS wgs84_lng,
  'valid'::text                         AS validite,
  'DP'::text                            AS type_propriete,
  NULL::text                            AS ideddp,
  NULL::text                            AS extrait_rdppf_pdf,
  'vd_lausanne_dp'::text                AS source_table,
  now() AS updated_at
FROM bronze_ch.vd_lausanne_dp ldp
WHERE ldp.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_plots_vd IS
  'VD Domaine Public parcels (federal cadastre excludes DP). egrid is synthesized as '
  '''dp_'' || source_pk because DP parcels have no federal EGRID. Consumers expecting '
  'real federal EGRIDs MUST filter type_propriete = ''DP'' to exclude these rows.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_plots_vd_egrid_idx ON silver_ch.cadastral_plots_vd (egrid);
CREATE INDEX        IF NOT EXISTS cadastral_plots_vd_geom_idx  ON silver_ch.cadastral_plots_vd USING GIST (geometry);
CREATE INDEX        IF NOT EXISTS cadastral_plots_vd_ncnp_idx  ON silver_ch.cadastral_plots_vd (no_commune_no_parcelle);


-- ----------------------------------------------------------------------------
-- 6. silver_ch.cadastral_servitudes_vd
--    Genre derived from source_layer prefix.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_servitudes_vd AS
SELECT
  row_number() OVER (ORDER BY s.source_layer, s.source_pk)::integer AS id,
  NULL::text                            AS no_commune_no_parcelle,    -- not in source
  s.id_rf                               AS id_servitude,
  s.id_rf                               AS register_number,
  CASE
    WHEN s.source_layer LIKE 'bdcad_servitudes_passages_pub_%'
         THEN 'passage_public'
    WHEN s.source_layer LIKE 'bdcad_servitudes_passages_canalisations_pub_%'
         THEN 'canalisation_publique'
    WHEN s.source_layer LIKE 'bdcad_servitudes_usage_pub_%'
         THEN 'usage_public'
  END                                   AS genre,
  s.geom_kind                           AS classe,    -- 'line' | 'point' | 'surf'
  s.nom                                 AS texte1,
  s.type_txt                            AS texte_complementaire,
  'valid'::text                         AS validite,
  CASE WHEN s.geom_kind = 'surf'
       THEN st_area(s.geometry)::numeric
       ELSE NULL END                     AS area_m2,
  'VD'::text                             AS canton_code,
  now() AS updated_at
FROM bronze_ch.vd_lausanne_servitudes s
WHERE s.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_servitudes_vd IS
  'VD public servitudes (Lausanne-only data today). genre derived from source_layer prefix. '
  'Mirror of cadastral_servitudes shape. classe field uses geom_kind discriminator.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_servitudes_vd_id_idx ON silver_ch.cadastral_servitudes_vd (id);
CREATE INDEX        IF NOT EXISTS cadastral_servitudes_vd_genre_idx ON silver_ch.cadastral_servitudes_vd (genre);
CREATE INDEX        IF NOT EXISTS cadastral_servitudes_vd_idrf_idx  ON silver_ch.cadastral_servitudes_vd (register_number);


-- ----------------------------------------------------------------------------
-- 7. silver_ch.cadastral_patrimoine_classe_vd
--    From vd_classement (both layers consolidated).
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_patrimoine_classe_vd AS
SELECT
  c.arcgis_objectid::integer            AS id,
  COALESCE(c.description, c.commune, 'Classement '||c.arcgis_objectid::text) AS designation,
  c.date_arrete::date                   AS date_classement,
  'VD'::text                            AS canton_code,
  c.geometry,
  jsonb_build_object(
    'source_layer',     c.source_layer,
    'fiche',            c.fiche,
    'url_recens',       c.url_recens,
    'numero_arrete',    c.numero_arrete,
    'type_protection',  c.type_protection
  ) AS raw_data,
  now() AS updated_at
FROM bronze_ch.vd_classement c
WHERE c.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_patrimoine_classe_vd IS
  'VD heritage classement (vd.plan_classement + vd.arrete_decision_classement_perimetre). '
  'Mirror of cadastral_patrimoine_classe + raw_data carries source-specific fields.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_patrimoine_classe_vd_id_idx ON silver_ch.cadastral_patrimoine_classe_vd (id);
CREATE INDEX        IF NOT EXISTS cadastral_patrimoine_classe_vd_geom_idx ON silver_ch.cadastral_patrimoine_classe_vd USING GIST (geometry);


-- ----------------------------------------------------------------------------
-- 8. silver_ch.cadastral_patrimoine_inventaire_vd
--    From vd_jardin_historique + vd_isos (sites + perimeters).
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_patrimoine_inventaire_vd AS
SELECT
  ('jh_' || jh.arcgis_objectid::text)::text AS id,    -- text to mix with isos ids
  COALESCE(jh.designation, 'Jardin historique '||jh.arcgis_objectid::text) AS designation,
  'jardin_historique'::text             AS valeur,
  'VD'::text                            AS canton_code,
  jh.geometry,
  jsonb_build_object('source_layer', jh.source_layer, 'date_classement', jh.date_classement) AS raw_data,
  now() AS updated_at
FROM bronze_ch.vd_jardin_historique jh
WHERE jh.deleted_at IS NULL
UNION ALL
SELECT
  ('isos_' || i.source_layer || '_' || i.arcgis_objectid::text) AS id,
  COALESCE(i.designation, i.isos_categorie, 'ISOS '||i.arcgis_objectid::text) AS designation,
  CASE WHEN i.source_layer = 'vd.site_fonde_sur_isos'      THEN 'isos_site'
       WHEN i.source_layer = 'vd.perimetre_fonde_sur_isos' THEN 'isos_perimetre' END AS valeur,
  'VD'::text                            AS canton_code,
  i.geometry,
  jsonb_build_object('source_layer', i.source_layer, 'isos_categorie', i.isos_categorie) AS raw_data,
  now() AS updated_at
FROM bronze_ch.vd_isos i
WHERE i.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_patrimoine_inventaire_vd IS
  'VD heritage inventory: historic gardens (valeur=''jardin_historique'') + ISOS sites/perimeters '
  '(valeur=''isos_site''/''isos_perimetre''). NOTE: id is text (vs integer in _ge equivalent) due to mixed sources.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_patrimoine_inventaire_vd_id_idx
  ON silver_ch.cadastral_patrimoine_inventaire_vd (id);
CREATE INDEX        IF NOT EXISTS cadastral_patrimoine_inventaire_vd_geom_idx
  ON silver_ch.cadastral_patrimoine_inventaire_vd USING GIST (geometry);
CREATE INDEX        IF NOT EXISTS cadastral_patrimoine_inventaire_vd_valeur_idx
  ON silver_ch.cadastral_patrimoine_inventaire_vd (valeur);


-- ----------------------------------------------------------------------------
-- 9. silver_ch.cadastral_densification_vd
--    From vd_lausanne_pa_etude. Mirror of cadastral_densification shape.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_densification_vd AS
SELECT
  ('pae_' || pae.source_pk)::text       AS id,
  NULL::text                            AS egrid,
  NULL::text                            AS no_commune_no_parcelle,
  pae.nom_projet                        AS sector_name,
  'Lausanne'::text                      AS commune,
  'plan_affectation_etude'::text        AS densification_type,
  NULL::text                            AS administrative_practice_url,
  'VD'::text                            AS canton_code,
  pae.geometry                          AS geometry,
  now() AS updated_at
FROM bronze_ch.vd_lausanne_pa_etude pae
WHERE pae.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_densification_vd IS
  'VD densification candidates (Lausanne PA à l''étude). Mirror of cadastral_densification + geometry column.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_densification_vd_id_idx ON silver_ch.cadastral_densification_vd (id);
CREATE INDEX        IF NOT EXISTS cadastral_densification_vd_geom_idx ON silver_ch.cadastral_densification_vd USING GIST (geometry);


-- ----------------------------------------------------------------------------
-- 10. silver_ch.cadastral_energy_vd
--     From vd_lausanne_energie_cad_bati (per-building) + heating fields from vd_batiment_rcb.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_energy_vd AS
WITH lausanne AS (
  SELECT
    e.egid,
    e.besoins_kwh                       AS energy_demand_kwh,
    e.solution_heat                     AS heating_solution_target,
    e.horizon_heat                      AS heating_horizon,
    e.vecteur_actuel                    AS energy_source_current,
    e.detail_solution_heat              AS heating_solution_detail
  FROM bronze_ch.vd_lausanne_energie_cad_bati e
  WHERE e.deleted_at IS NULL AND e.egid IS NOT NULL AND e.egid <> ''
), rcb AS (
  SELECT
    b.egid,
    mode() WITHIN GROUP (ORDER BY b.chauf1_nrg_txt) AS rcb_predominant_heating_energy,
    mode() WITHIN GROUP (ORDER BY b.chauf1_sys_txt) AS rcb_predominant_heating_system,
    sum(b.sre_m2)::numeric                          AS rcb_total_sre_m2
  FROM bronze_ch.vd_batiment_rcb b
  WHERE b.deleted_at IS NULL AND b.egid IS NOT NULL AND b.egid <> ''
  GROUP BY b.egid
)
SELECT
  COALESCE(l.egid, r.egid)::character varying       AS egid,
  COALESCE(l.energy_source_current, r.rcb_predominant_heating_energy) AS energy_source,
  'VD'::text                                         AS canton_code,
  NULL::text                                          AS idc_status,
  NULL::text                                          AS idc_detail,
  r.rcb_total_sre_m2                                  AS idc_sre_m2,
  NULL::numeric                                       AS idc_last_value,
  NULL::integer                                       AS idc_last_year,
  NULL::numeric                                       AS idc_prev_value,
  NULL::integer                                       AS idc_prev_year,
  NULL::numeric                                       AS idc_third_value,
  NULL::integer                                       AS idc_third_year,
  NULL::numeric                                       AS idc_avg_3years,
  NULL::text                                          AS idc_bracket_3years,
  NULL::text                                          AS idc_bracket_1year,
  NULL::text                                          AS minergie_certificate,
  NULL::text                                          AS minergie_standard,
  NULL::integer                                       AS minergie_ebf_m2,
  -- VD-only enrichment columns (additive vs _ge)
  l.energy_demand_kwh,
  l.heating_solution_target,
  l.heating_horizon,
  l.heating_solution_detail,
  r.rcb_predominant_heating_system,
  now() AS updated_at
FROM lausanne l
FULL OUTER JOIN rcb r USING (egid)
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_energy_vd IS
  'VD per-building energy. EGID-keyed FULL OUTER JOIN of Lausanne energie_cad_bati '
  '(canton-VD has no per-building source) and vd_batiment_rcb heating cols. IDC fields '
  'left NULL — Lausanne source provides demand not consumption. Schema delta vs cadastral_energy: '
  'ADDS energy_demand_kwh, heating_solution_target, heating_horizon, heating_solution_detail, '
  'rcb_predominant_heating_system.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_energy_vd_egid_idx ON silver_ch.cadastral_energy_vd (egid);


-- ============================================================================
-- 3 NATIONAL new silver matviews (canton-pluggable from day one)
-- ============================================================================

-- 11. silver_ch.cadastral_noise_sensitivity  (NATIONAL)
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_noise_sensitivity AS
SELECT
  row_number() OVER (ORDER BY b.canton_code, b.arcgis_objectid)::integer AS id,
  b.ds_degre                            AS sensitivity_degree,    -- DS I..IV
  b.source_plan                         AS source_plan,
  b.description                         AS description,
  b.canton_code,
  b.geometry,
  now() AS updated_at
FROM bronze_ch.vd_degre_sensibilite_bruit b
WHERE b.deleted_at IS NULL
-- Additional canton bronzes UNION ALL here as they come online (ZH, BE, ...)
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_noise_sensitivity IS
  'National OPB noise sensitivity degrees (DS I–IV). Canton-pluggable: each canton''s bronze '
  'UNIONs in as ingest comes online. Today: VD only.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_noise_sensitivity_id_idx ON silver_ch.cadastral_noise_sensitivity (id);
CREATE INDEX        IF NOT EXISTS cadastral_noise_sensitivity_geom_idx ON silver_ch.cadastral_noise_sensitivity USING GIST (geometry);
CREATE INDEX        IF NOT EXISTS cadastral_noise_sensitivity_canton_idx ON silver_ch.cadastral_noise_sensitivity (canton_code);


-- 12. silver_ch.cadastral_archaeology  (NATIONAL)
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.cadastral_archaeology AS
SELECT
  ('vdreg_' || r.arcgis_objectid::text) AS id,
  'region'::text                         AS record_type,
  NULL::text                             AS record_subtype,
  r.nom_region                           AS designation,
  NULL::text                             AS description,
  NULL::text                             AS url_fiche,
  NULL::text                             AS url_carte,
  'VD'::text                             AS canton_code,
  r.geometry,
  now() AS updated_at
FROM bronze_ch.vd_region_archeologique r
WHERE r.deleted_at IS NULL
UNION ALL
SELECT
  ('vdlau_' || la.source_layer || '_' || la.source_pk) AS id,
  la.record_type,
  la.record_subtype,
  la.description                         AS designation,
  COALESCE(la.note_detail, la.note_carto) AS description,
  la.url_fiche,
  la.url_carte,
  'VD'::text                             AS canton_code,
  la.geometry,
  now() AS updated_at
FROM bronze_ch.vd_lausanne_archeologie la
WHERE la.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.cadastral_archaeology IS
  'National archaeology: canton-wide regions + city-level sites/measures. record_type ∈ '
  '(region,site,mesure). record_subtype ∈ (notes,mesures,pbc,plan_class) for site/mesure rows. '
  'Today: VD only.';
CREATE UNIQUE INDEX IF NOT EXISTS cadastral_archaeology_id_idx       ON silver_ch.cadastral_archaeology (id);
CREATE INDEX        IF NOT EXISTS cadastral_archaeology_canton_idx   ON silver_ch.cadastral_archaeology (canton_code);
CREATE INDEX        IF NOT EXISTS cadastral_archaeology_type_idx     ON silver_ch.cadastral_archaeology (record_type);
CREATE INDEX        IF NOT EXISTS cadastral_archaeology_geom_idx     ON silver_ch.cadastral_archaeology USING GIST (geometry);


-- 13. silver_ch.transit_stops  (NATIONAL)
CREATE MATERIALIZED VIEW IF NOT EXISTS silver_ch.transit_stops AS
SELECT
  t.didok,
  t.name                                 AS stop_name,
  t.abbr                                  AS abbreviation,
  t.mode                                  AS transport_mode,
  t.accessibility                         AS accessibility,
  t.gtfs_stop_id                          AS gtfs_stop_id,
  t.validity_from,
  t.validity_to,
  t.canton_code,
  t.geometry,
  now() AS updated_at
FROM bronze_ch.federal_bav_transit t
WHERE t.deleted_at IS NULL
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW silver_ch.transit_stops IS
  'National BAV public-transit stops (~31K). canton_code populated in bronze via spatial join '
  'to federal_communes during ingest. Authoritative federal source for nearest_transport_m '
  'computation in future plot_intel_* refresh.';
CREATE UNIQUE INDEX IF NOT EXISTS transit_stops_didok_idx  ON silver_ch.transit_stops (didok);
CREATE INDEX        IF NOT EXISTS transit_stops_canton_idx ON silver_ch.transit_stops (canton_code);
CREATE INDEX        IF NOT EXISTS transit_stops_mode_idx   ON silver_ch.transit_stops (transport_mode);
CREATE INDEX        IF NOT EXISTS transit_stops_geom_idx   ON silver_ch.transit_stops USING GIST (geometry);


COMMIT;

-- ============================================================================
-- Post-apply (separate transaction, AFTER bronze parsers have run a backfill):
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_buildings_vd;          -- not CONCURRENTLY first time
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_zones_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_chantiers_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_ddp_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_plots_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_servitudes_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_patrimoine_classe_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_patrimoine_inventaire_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_densification_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_energy_vd;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_noise_sensitivity;
--   REFRESH MATERIALIZED VIEW silver_ch.cadastral_archaeology;
--   REFRESH MATERIALIZED VIEW silver_ch.transit_stops;
-- Subsequent refreshes: use CONCURRENTLY (unique indexes already in place).
-- ============================================================================
NOTIFY pgrst, 'reload schema';
