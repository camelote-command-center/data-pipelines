-- ============================================================================
-- camelote_data.public.datasets — 19 registration rows for vd_enrichment
-- ============================================================================
-- Target DB: camelote-data  (project dxugbpeacnorjunpljih)
-- Pre-req  : 2026-05-21_camelote_datasets_host.sql applied (adds `host` column).
-- ----------------------------------------------------------------------------
-- All rows scoped to startup_id = (SELECT id FROM startups WHERE code='lamap').
-- All have acquisition_method='api' (ArcGIS REST / WFS / GeoAdmin all qualify).
-- frequency='monthly' except federal_bav_transit which is 'quarterly'.
-- ----------------------------------------------------------------------------
BEGIN;

WITH lamap AS (SELECT id FROM startups WHERE code='lamap')
INSERT INTO public.datasets (
  startup_id, code, name, data_category, acquisition_method, frequency,
  expected_days_between_updates, delay_threshold_days,
  target_schema, target_table, parser_name, has_geometry, is_automated,
  priority, status, host, source_url, notes
)
SELECT lamap.id, v.*
FROM lamap, (VALUES
  -- code, name, data_category, acq_method, freq, exp_days, threshold, target_schema, target_table, parser_name, has_geometry, is_automated, priority, status, host, source_url, notes
  ('vd_batiment_rcb',                 'VD building cadastre (RCB)',                        'building',       'api','monthly',31, 7,'bronze_ch','vd_batiment_rcb',                 'pipelines.vd_enrichment.vd_batiment_rcb.run',                 true, true,'high',    'needs_setup','vps-145.223.82.190','https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/39','Per-building EGID + heating + SRE.'),
  ('vd_zone_affectation',             'VD PGA zones (canton-wide, with IUS/COS/SPB)',      'geographic',     'api','monthly',31, 7,'bronze_ch','vd_zone_affectation',             'pipelines.vd_enrichment.vd_zone_affectation.run',             true, true,'high',    'needs_setup','vps-31.97.122.135', 'https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/36','Includes IUS, COS, SPB, CM, IGT, H_MAX.'),
  ('vd_limite_foret',                 'VD forest limits',                                  'geographic',     'api','monthly',31, 7,'bronze_ch','vd_limite_foret',                 'pipelines.vd_enrichment.vd_limite_foret.run',                 true, true,'medium',  'needs_setup','vps-31.97.122.135', 'https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/490', ''),
  ('vd_batiment_projete',             'VD projected/in-study buildings',                   'building',       'api','monthly',31, 7,'bronze_ch','vd_batiment_projete',             'pipelines.vd_enrichment.vd_batiment_projete.run',             true, true,'medium',  'needs_setup','vps-145.223.82.190','https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/274', ''),
  ('vd_degre_sensibilite_bruit',      'VD OPB noise sensitivity (DS I–IV)',                'geographic',     'api','monthly',31, 7,'bronze_ch','vd_degre_sensibilite_bruit',      'pipelines.vd_enrichment.vd_degre_sensibilite_bruit.run',      true, true,'medium',  'needs_setup','vps-31.97.122.135', 'https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/461', 'Feeds NATIONAL cadastral_noise_sensitivity.'),
  ('vd_classement',                   'VD heritage classement (consolidated 2 layers)',    'legal',          'api','monthly',31, 7,'bronze_ch','vd_classement',                   'pipelines.vd_enrichment.vd_classement.run',                   true, true,'medium',  'needs_setup','vps-31.97.122.135', 'multi: agsgc /398 + /161', 'Multi-layer; discriminator source_layer.'),
  ('vd_jardin_historique',            'VD historic gardens',                               'geographic',     'api','monthly',31, 7,'bronze_ch','vd_jardin_historique',            'pipelines.vd_enrichment.vd_jardin_historique.run',            true, true,'low',     'needs_setup','vps-31.97.122.135', 'https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/403', ''),
  ('vd_isos',                         'VD ISOS sites + perimeters (consolidated 2 layers)','geographic',     'api','monthly',31, 7,'bronze_ch','vd_isos',                         'pipelines.vd_enrichment.vd_isos.run',                         true, true,'medium',  'needs_setup','vps-31.97.122.135', 'multi: agsgc /481 + /404', 'Federally-derived; canton publishes.'),
  ('vd_region_archeologique',         'VD archaeological regions',                         'geographic',     'api','monthly',31, 7,'bronze_ch','vd_region_archeologique',         'pipelines.vd_enrichment.vd_region_archeologique.run',         true, true,'low',     'needs_setup','vps-31.97.122.135', 'https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/320', ''),
  ('vd_ddp_pts',                      'VD DDP centroids (multipoint)',                     'legal',          'api','monthly',31, 7,'bronze_ch','vd_ddp_pts',                      'pipelines.vd_enrichment.vd_ddp_pts.run',                      true, true,'medium',  'needs_setup','vps-145.223.82.190','https://agsgc.map.vd.ch/agsgc/rest/services/OGC/wmsVD/MapServer/430', 'Polygon via vd_lausanne_ddp.'),
  ('vd_lausanne_ddp',                 'Lausanne DDP polygons',                             'legal',          'api','monthly',31, 7,'bronze_ch','vd_lausanne_ddp',                 'pipelines.vd_enrichment.vd_lausanne_ddp.run',                 true, true,'high',    'needs_setup','vps-145.223.82.190','https://map.lausanne.ch/mapserv_proxy (bdcad_bf_parc_pol_ddp)', ''),
  ('vd_lausanne_dp',                  'Lausanne Domaine Public parcels',                   'legal',          'api','monthly',31, 7,'bronze_ch','vd_lausanne_dp',                  'pipelines.vd_enrichment.vd_lausanne_dp.run',                  true, true,'high',    'needs_setup','vps-145.223.82.190','https://map.lausanne.ch/mapserv_proxy (bdcad_bf_parc_pol_dp)', 'DP excluded by federal_cadastral_parcels.'),
  ('vd_lausanne_servitudes',          'Lausanne public servitudes (8 layers consolidated)','legal',          'api','monthly',31, 7,'bronze_ch','vd_lausanne_servitudes',          'pipelines.vd_enrichment.vd_lausanne_servitudes.run',          true, true,'high',    'needs_setup','vps-145.223.82.190','https://map.lausanne.ch/mapserv_proxy (8 layers)', 'Canton-VD has zero servitudes layers.'),
  ('vd_lausanne_pa_etude',            'Lausanne plans d''affectation à l''étude',           'geographic',     'api','monthly',31, 7,'bronze_ch','vd_lausanne_pa_etude',            'pipelines.vd_enrichment.vd_lausanne_pa_etude.run',            true, true,'medium',  'needs_setup','vps-31.97.122.135', 'https://map.lausanne.ch/mapserv_proxy (amenagement_pga_pa_etude)', ''),
  ('vd_lausanne_parking_sectors',     'Lausanne parking policy sectors',                   'administrative', 'api','monthly',31, 7,'bronze_ch','vd_lausanne_parking_sectors',     'pipelines.vd_enrichment.vd_lausanne_parking_sectors.run',     true, true,'low',     'needs_setup','vps-31.97.122.135', 'https://map.lausanne.ch/mapserv_proxy (amenagement_pga_sect_stationnement)', ''),
  ('vd_lausanne_archeologie',         'Lausanne archaeology (8 layers consolidated)',      'geographic',     'api','monthly',31, 7,'bronze_ch','vd_lausanne_archeologie',         'pipelines.vd_enrichment.vd_lausanne_archeologie.run',         true, true,'medium',  'needs_setup','vps-31.97.122.135', 'https://map.lausanne.ch/mapserv_proxy (8 layers)', 'Feeds NATIONAL cadastral_archaeology.'),
  ('vd_lausanne_energie_cad_bati',    'Lausanne per-building energy',                      'building',       'api','monthly',31, 7,'bronze_ch','vd_lausanne_energie_cad_bati',    'pipelines.vd_enrichment.vd_lausanne_energie_cad_bati.run',    true, true,'medium',  'needs_setup','vps-46.202.153.114','https://map.lausanne.ch/mapserv_proxy (energie_cad_bati)', 'Canton only has hectare aggregates.'),
  ('federal_bav_transit',             'Federal BAV public transit stops (national)',       'administrative', 'api','quarterly',92, 14,'bronze_ch','federal_bav_transit',             'pipelines.vd_enrichment.federal_bav_transit.run',             true, true,'high',    'needs_setup','vps-46.202.153.114','https://api3.geo.admin.ch (ch.bav.haltestellen-oev)', '~31K national stops. canton_code via spatial join.')
) AS v(
  code, name, data_category, acquisition_method, frequency,
  expected_days_between_updates, delay_threshold_days,
  target_schema, target_table, parser_name, has_geometry, is_automated,
  priority, status, host, source_url, notes
)
ON CONFLICT (startup_id, code) DO NOTHING;

COMMIT;
