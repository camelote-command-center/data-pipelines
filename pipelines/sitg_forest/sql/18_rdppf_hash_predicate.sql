-- ============================================================================
-- ge_rdppf_synthese: compare a stored content hash, not 54 columns (Step 2)
-- ============================================================================
-- The full-column text comparison was correct but could not finish: 54 columns
-- across a 74'436-row FDW join exceeded the procedure's 600s statement_timeout
-- on every attempt. The predicate is now a single indexed column.
--
-- content_hash is trigger-maintained on BOTH databases from the same 54-column
-- expression (the procedure's own SET list, updated_at excluded), with every
-- timestamp normalised to UTC in a fixed format because the two hashes are
-- computed in different sessions whose TimeZone we do not control. A stored
-- GENERATED column was rejected: timestamptz::text is not immutable.
--
-- The timeout stays in the CREATE PROCEDURE definition clause (proconfig), not
-- SET LOCAL in the body: pg_cron sessions inherit the database-level timeout
-- and SET LOCAL would be overridden.
-- ============================================================================

CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_rdppf_synthese()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.ge_rdppf_synthese t SET "id160_reserves_forestieres"=s."id160_reserves_forestieres", "numero"=s."numero", "id73_zones_affect_superp"=s."id73_zones_affect_superp", "id73_plcp"=s."id73_plcp", "globalid"=s."globalid", "id73_pnp"=s."id73_pnp", "id131_zones_protec_eaux_sout"=s."id131_zones_protec_eaux_sout", "id132_perim_protec_eaux_sout"=s."id132_perim_protec_eaux_sout", "id190_ere"=s."id190_ere", "id73_zp"=s."id73_zp", "id73_plq"=s."id73_plq", "id103_ch_zr_instal_aero"=s."id103_ch_zr_instal_aero", "id117_ch_cad_site_pollues_mili"=s."id117_ch_cad_site_pollues_mili", "egrid"=s."egrid", "id157_limites_foret_statiques"=s."id157_limites_foret_statiques", "genre"=s."genre", "id96_ch_zr_instal_ferrov"=s."id96_ch_zr_instal_ferrov", "id118_ch_cad_site_pollues_aero"=s."id118_ch_cad_site_pollues_aero", "shape__area"=s."shape__area", "id73_pdzi"=s."id73_pdzi", "id87_zr_rn"=s."id87_zr_rn", "id88_ch_align_rn"=s."id88_ch_align_rn", "surface"=s."surface", "id104_ch_align_instal_aero"=s."id104_ch_align_instal_aero", "id73_pdzam"=s."id73_pdzam", "id73_pus"=s."id73_pus", "extrait_rdppf_pdf"=s."extrait_rdppf_pdf", "idxxge_cadastre_foret"=s."idxxge_cadastre_foret", "id73_si"=s."id73_si", "id159_distances_foret"=s."id159_distances_foret", "id76_zr"=s."id76_zr", "id108_ch_plan_zone_securite"=s."id108_ch_plan_zone_securite", "id218_ch_align_instal_elec"=s."id218_ch_align_instal_elec", "id116_cadastre_site_pollues"=s."id116_cadastre_site_pollues", "id73_zones_affect_prim"=s."id73_zones_affect_prim", "id73_ps"=s."id73_ps", "id97_ch_align_instal_ferrov"=s."id97_ch_align_instal_ferrov", "id119_ch_cad_site_pollues_tp"=s."id119_ch_cad_site_pollues_tp", "id73_extract_grav"=s."id73_extract_grav", "id217_ch_zr_ltn220"=s."id217_ch_zr_ltn220", "id73_rs"=s."id73_rs", "id145_dsopb"=s."id145_dsopb", "idxxge_lim_const_lroutes"=s."idxxge_lim_const_lroutes", "shape__length"=s."shape__length", "id73_pla"=s."id73_pla", "objectid"=s."objectid", "id73_zones_affect_synt"=s."id73_zones_affect_synt", "lien_www"=s."lien_www", "iteration"=s."iteration", "created_at"=s."created_at", "updated_at"=s."updated_at", "idge_xxxx_align_const_lroutes"=s."idge_xxxx_align_const_lroutes", "idge_1175_ouvrages_souterrains"=s."idge_1175_ouvrages_souterrains", "idge_1079_cadastre_foret"=s."idge_1079_cadastre_foret", "geometry"=s."geometry"
  FROM bronze_ch.ge_rdppf_synthese s
  WHERE t.id=s.id AND t.content_hash IS DISTINCT FROM s.content_hash;
  INSERT INTO lamap_db_foreign.ge_rdppf_synthese ("id", "id160_reserves_forestieres", "numero", "id73_zones_affect_superp", "id73_plcp", "globalid", "id73_pnp", "id131_zones_protec_eaux_sout", "id132_perim_protec_eaux_sout", "id190_ere", "id73_zp", "id73_plq", "id103_ch_zr_instal_aero", "id117_ch_cad_site_pollues_mili", "egrid", "id157_limites_foret_statiques", "genre", "id96_ch_zr_instal_ferrov", "id118_ch_cad_site_pollues_aero", "shape__area", "id73_pdzi", "id87_zr_rn", "id88_ch_align_rn", "surface", "id104_ch_align_instal_aero", "id73_pdzam", "id73_pus", "extrait_rdppf_pdf", "idxxge_cadastre_foret", "id73_si", "id159_distances_foret", "id76_zr", "id108_ch_plan_zone_securite", "id218_ch_align_instal_elec", "id116_cadastre_site_pollues", "id73_zones_affect_prim", "id73_ps", "id97_ch_align_instal_ferrov", "id119_ch_cad_site_pollues_tp", "id73_extract_grav", "id217_ch_zr_ltn220", "id73_rs", "id145_dsopb", "idxxge_lim_const_lroutes", "shape__length", "id73_pla", "objectid", "id73_zones_affect_synt", "lien_www", "iteration", "created_at", "updated_at", "idge_xxxx_align_const_lroutes", "idge_1175_ouvrages_souterrains", "idge_1079_cadastre_foret", "geometry")
  SELECT "id", "id160_reserves_forestieres", "numero", "id73_zones_affect_superp", "id73_plcp", "globalid", "id73_pnp", "id131_zones_protec_eaux_sout", "id132_perim_protec_eaux_sout", "id190_ere", "id73_zp", "id73_plq", "id103_ch_zr_instal_aero", "id117_ch_cad_site_pollues_mili", "egrid", "id157_limites_foret_statiques", "genre", "id96_ch_zr_instal_ferrov", "id118_ch_cad_site_pollues_aero", "shape__area", "id73_pdzi", "id87_zr_rn", "id88_ch_align_rn", "surface", "id104_ch_align_instal_aero", "id73_pdzam", "id73_pus", "extrait_rdppf_pdf", "idxxge_cadastre_foret", "id73_si", "id159_distances_foret", "id76_zr", "id108_ch_plan_zone_securite", "id218_ch_align_instal_elec", "id116_cadastre_site_pollues", "id73_zones_affect_prim", "id73_ps", "id97_ch_align_instal_ferrov", "id119_ch_cad_site_pollues_tp", "id73_extract_grav", "id217_ch_zr_ltn220", "id73_rs", "id145_dsopb", "idxxge_lim_const_lroutes", "shape__length", "id73_pla", "objectid", "id73_zones_affect_synt", "lien_www", "iteration", "created_at", "updated_at", "idge_xxxx_align_const_lroutes", "idge_1175_ouvrages_souterrains", "idge_1079_cadastre_foret", "geometry" FROM bronze_ch.ge_rdppf_synthese s
  WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.ge_rdppf_synthese t WHERE t.id=s.id);
END;$procedure$

;
