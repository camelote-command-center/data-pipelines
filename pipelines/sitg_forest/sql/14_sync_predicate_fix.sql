-- ============================================================================
-- Replace timestamp gating with content comparison in the sync procedures
-- ============================================================================
-- Bug 177de4c5. Each procedure keeps its own explicit SET column list; only the
-- change predicate is replaced, with a full-column comparison over exactly
-- those columns, every value cast to text (the PostGIS = operator is
-- bbox-based, so an uncast geometry comparison never fires on a real shape
-- change). updated_at stays in the SET but is excluded from the comparison so
-- ingest churn does not rewrite every row.
--
-- DELIBERATELY EXCLUDED FROM THIS FILE:
--   gold_ch.sync_ge_cad_batiments
--   gold_ch.sync_ge_cad_batiments_souterrains
-- Both join on `id`, an ingest-assigned surrogate that is MISALIGNED between
-- re-LLM and lamap_db: joined on id, 82'423 of 83'018 buildings carry a
-- different egid on the two sides and the sampled geometries are different
-- buildings entirely. Their broken predicate is currently the only thing
-- preventing mass corruption. Fixing it here would make the next sync overwrite
-- ~82'000 buildings with another building's attributes. Those two need their
-- join key moved to the business key (no_comm, no_batiment) FIRST, as a
-- separate reviewed change.
--
-- NOTE ON sync_ge_cad_adresses: it maintains only 7 of the table's 33 columns.
-- The other 26 are written on INSERT and never updated, so drift on geometry,
-- e, n, type and the symbole columns is by design and this fix does not change
-- that. Whether that narrow update is still intended is a separate question.
-- ============================================================================

-- ---- sync_fao_ldtr ----
CREATE OR REPLACE PROCEDURE gold_ch.sync_fao_ldtr()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.fao_ldtr t SET type=s.type, date_de_parution_au_rf=s.date_de_parution_au_rf, transaction_date=s.transaction_date, transaction=s.transaction, affaire=s.affaire, vendeur=s.vendeur, commune=s.commune, lot_key=s.lot_key, acheteur=s.acheteur, prix=s.prix, created_at=s.created_at, updated_at=s.updated_at, address=s.address, acheteur_list=s.acheteur_list, vendeur_list=s.vendeur_list FROM bronze_ch.fao_ldtr s WHERE t.id=s.id AND ((t."type"::text, t."date_de_parution_au_rf"::text, t."transaction_date"::text, t."transaction"::text, t."affaire"::text, t."vendeur"::text, t."commune"::text, t."lot_key"::text, t."acheteur"::text, t."prix"::text, t."created_at"::text, t."address"::text, t."acheteur_list"::text, t."vendeur_list"::text) IS DISTINCT FROM (s."type"::text, s."date_de_parution_au_rf"::text, s."transaction_date"::text, s."transaction"::text, s."affaire"::text, s."vendeur"::text, s."commune"::text, s."lot_key"::text, s."acheteur"::text, s."prix"::text, s."created_at"::text, s."address"::text, s."acheteur_list"::text, s."vendeur_list"::text));
  INSERT INTO lamap_db_foreign.fao_ldtr SELECT s.* FROM bronze_ch.fao_ldtr s WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.fao_ldtr t WHERE t.id=s.id);
END;$procedure$

;

-- ---- sync_ge_cad_adresses ----
CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_cad_adresses()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.ge_cad_adresses t
  SET adresse=s.adresse, egid=s.egid, code_voie=s.code_voie, id_girec=s.id_girec, no_batiment=s.no_batiment, nom_npa=s.nom_npa, updated_at=s.updated_at
  FROM bronze_ch.ge_cad_adresses s WHERE t.idpadr=s.idpadr AND((t."adresse"::text, t."egid"::text, t."code_voie"::text, t."id_girec"::text, t."no_batiment"::text, t."nom_npa"::text) IS DISTINCT FROM (s."adresse"::text, s."egid"::text, s."code_voie"::text, s."id_girec"::text, s."no_batiment"::text, s."nom_npa"::text));
  INSERT INTO lamap_db_foreign.ge_cad_adresses SELECT s.* FROM bronze_ch.ge_cad_adresses s
  WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.ge_cad_adresses t WHERE t.idpadr=s.idpadr);
END;$procedure$

;

-- ---- sync_ge_cad_communes ----
CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_cad_communes()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.ge_cad_communes t SET objectid=s.objectid, commune=s.commune, no_comm=s.no_comm, abreviation=s.abreviation, no_com_federal=s.no_com_federal, lien_www=s.lien_www, shape_area=s.shape_area, shape_len=s.shape_len, iteration=s.iteration, created_at=s.created_at, updated_at=s.updated_at, geometry=s.geometry FROM bronze_ch.ge_cad_communes s WHERE t.id=s.id AND ((t."objectid"::text, t."commune"::text, t."no_comm"::text, t."abreviation"::text, t."no_com_federal"::text, t."lien_www"::text, t."shape_area"::text, t."shape_len"::text, t."iteration"::text, t."created_at"::text, t."geometry"::text) IS DISTINCT FROM (s."objectid"::text, s."commune"::text, s."no_comm"::text, s."abreviation"::text, s."no_com_federal"::text, s."lien_www"::text, s."shape_area"::text, s."shape_len"::text, s."iteration"::text, s."created_at"::text, s."geometry"::text));
  INSERT INTO lamap_db_foreign.ge_cad_communes SELECT s.* FROM bronze_ch.ge_cad_communes s WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.ge_cad_communes t WHERE t.id=s.id);
END;$procedure$

;

-- ---- sync_ge_cad_ddp_rich ----
CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_cad_ddp_rich()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  -- Existing rows: refresh RICH attributes only (geo/identity cols intentionally absent from SET → preserved).
  UPDATE lamap_db_foreign.ddp t SET
    genre=s.genre, validite=s.validite, lien_www=s.lien_www,
    extrait_rdppf_pdf=s.extrait_rdppf_pdf, batiments=s.batiments,
    mutnum=s.mutnum, mutori=s.mutori, no_ddp=s.no_ddp,
    surface_officielle_m2=s.surface_officielle_m2,
    source_updated_at=s.source_updated_at, updated_at=s.updated_at
  FROM gold_ch.v_ddp_full s
  WHERE t.egrid=s.egrid AND ((t."genre"::text, t."validite"::text, t."lien_www"::text, t."extrait_rdppf_pdf"::text, t."batiments"::text, t."mutnum"::text, t."mutori"::text, t."no_ddp"::text, t."surface_officielle_m2"::text, t."source_updated_at"::text) IS DISTINCT FROM (s."genre"::text, s."validite"::text, s."lien_www"::text, s."extrait_rdppf_pdf"::text, s."batiments"::text, s."mutnum"::text, s."mutori"::text, s."no_ddp"::text, s."surface_officielle_m2"::text, s."source_updated_at"::text));

  -- New egrids only: full insert (geo from bronze; id via lamap gen_random_uuid() default).
  INSERT INTO lamap_db_foreign.ddp
    (egrid, no_commune_no_parcelle, commune_name, no_comm, surface_ddp_m2, shape_area, shape_len,
     genre, validite, lien_www, extrait_rdppf_pdf, batiments, mutnum, mutori, no_ddp,
     surface_officielle_m2, source_updated_at, updated_at)
  SELECT egrid, no_commune_no_parcelle, commune_name, no_comm, surface_ddp_m2, shape_area, shape_len,
     genre, validite, lien_www, extrait_rdppf_pdf, batiments, mutnum, mutori, no_ddp,
     surface_officielle_m2, source_updated_at, updated_at
  FROM gold_ch.v_ddp_full s
  WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.ddp t WHERE t.egrid=s.egrid);
END;$procedure$

;

-- ---- sync_ge_gol_sites_pollues ----
CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_gol_sites_pollues()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.ge_gol_sites_pollues t SET iteration=s.iteration, created_at=s.created_at, updated_at=s.updated_at, objectid=s.objectid, quantite_polluant_ecoulee=s.quantite_polluant_ecoulee, no_officiel=s.no_officiel, globalid=s.globalid, assainissement_realise=s.assainissement_realise, type_activite_ou_type_remblai=s.type_activite_ou_type_remblai, atteinte_constatee=s.atteinte_constatee, atteinte_env_possible=s.atteinte_env_possible, documentation_existante=s.documentation_existante, date_derniere_mention=s.date_derniere_mention, autre_statut=s.autre_statut, type_site=s.type_site, evenement_particulier=s.evenement_particulier, air_menace=s.air_menace, raison_sociale=s.raison_sociale, projet_assainissement_realise=s.projet_assainissement_realise, volume_dechet=s.volume_dechet, shape__area=s.shape__area, quantite_polluant_recuperee=s.quantite_polluant_recuperee, type_pollution=s.type_pollution, eau_souterraine_menacee=s.eau_souterraine_menacee, meta_information=s.meta_information, sol_menace=s.sol_menace, eau_surface_menacee=s.eau_surface_menacee, no_gsipol=s.no_gsipol, investigation_techn_realisee=s.investigation_techn_realisee, investigation_histo_realisee=s.investigation_histo_realisee, libelle_noga=s.libelle_noga, type_polluant=s.type_polluant, investigation=s.investigation, date_premiere_mention=s.date_premiere_mention, investigation_detail_realisee=s.investigation_detail_realisee, shape__length=s.shape__length, code_noga=s.code_noga, date_accident=s.date_accident, no_commune_no_parcelle=s.no_commune_no_parcelle, geometry=s.geometry
  FROM bronze_ch.ge_gol_sites_pollues s WHERE t.id=s.id AND ((t."iteration"::text, t."created_at"::text, t."objectid"::text, t."quantite_polluant_ecoulee"::text, t."no_officiel"::text, t."globalid"::text, t."assainissement_realise"::text, t."type_activite_ou_type_remblai"::text, t."atteinte_constatee"::text, t."atteinte_env_possible"::text, t."documentation_existante"::text, t."date_derniere_mention"::text, t."autre_statut"::text, t."type_site"::text, t."evenement_particulier"::text, t."air_menace"::text, t."raison_sociale"::text, t."projet_assainissement_realise"::text, t."volume_dechet"::text, t."shape__area"::text, t."quantite_polluant_recuperee"::text, t."type_pollution"::text, t."eau_souterraine_menacee"::text, t."meta_information"::text, t."sol_menace"::text, t."eau_surface_menacee"::text, t."no_gsipol"::text, t."investigation_techn_realisee"::text, t."investigation_histo_realisee"::text, t."libelle_noga"::text, t."type_polluant"::text, t."investigation"::text, t."date_premiere_mention"::text, t."investigation_detail_realisee"::text, t."shape__length"::text, t."code_noga"::text, t."date_accident"::text, t."no_commune_no_parcelle"::text, t."geometry"::text) IS DISTINCT FROM (s."iteration"::text, s."created_at"::text, s."objectid"::text, s."quantite_polluant_ecoulee"::text, s."no_officiel"::text, s."globalid"::text, s."assainissement_realise"::text, s."type_activite_ou_type_remblai"::text, s."atteinte_constatee"::text, s."atteinte_env_possible"::text, s."documentation_existante"::text, s."date_derniere_mention"::text, s."autre_statut"::text, s."type_site"::text, s."evenement_particulier"::text, s."air_menace"::text, s."raison_sociale"::text, s."projet_assainissement_realise"::text, s."volume_dechet"::text, s."shape__area"::text, s."quantite_polluant_recuperee"::text, s."type_pollution"::text, s."eau_souterraine_menacee"::text, s."meta_information"::text, s."sol_menace"::text, s."eau_surface_menacee"::text, s."no_gsipol"::text, s."investigation_techn_realisee"::text, s."investigation_histo_realisee"::text, s."libelle_noga"::text, s."type_polluant"::text, s."investigation"::text, s."date_premiere_mention"::text, s."investigation_detail_realisee"::text, s."shape__length"::text, s."code_noga"::text, s."date_accident"::text, s."no_commune_no_parcelle"::text, s."geometry"::text));
  INSERT INTO lamap_db_foreign.ge_gol_sites_pollues
  SELECT s.* FROM bronze_ch.ge_gol_sites_pollues s
  WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.ge_gol_sites_pollues t WHERE t.id=s.id);
END;$procedure$

;

-- ---- sync_ge_rdppf_synthese ----
CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_rdppf_synthese()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.ge_rdppf_synthese t SET "id160_reserves_forestieres"=s."id160_reserves_forestieres", "numero"=s."numero", "id73_zones_affect_superp"=s."id73_zones_affect_superp", "id73_plcp"=s."id73_plcp", "globalid"=s."globalid", "id73_pnp"=s."id73_pnp", "id131_zones_protec_eaux_sout"=s."id131_zones_protec_eaux_sout", "id132_perim_protec_eaux_sout"=s."id132_perim_protec_eaux_sout", "id190_ere"=s."id190_ere", "id73_zp"=s."id73_zp", "id73_plq"=s."id73_plq", "id103_ch_zr_instal_aero"=s."id103_ch_zr_instal_aero", "id117_ch_cad_site_pollues_mili"=s."id117_ch_cad_site_pollues_mili", "egrid"=s."egrid", "id157_limites_foret_statiques"=s."id157_limites_foret_statiques", "genre"=s."genre", "id96_ch_zr_instal_ferrov"=s."id96_ch_zr_instal_ferrov", "id118_ch_cad_site_pollues_aero"=s."id118_ch_cad_site_pollues_aero", "shape__area"=s."shape__area", "id73_pdzi"=s."id73_pdzi", "id87_zr_rn"=s."id87_zr_rn", "id88_ch_align_rn"=s."id88_ch_align_rn", "surface"=s."surface", "id104_ch_align_instal_aero"=s."id104_ch_align_instal_aero", "id73_pdzam"=s."id73_pdzam", "id73_pus"=s."id73_pus", "extrait_rdppf_pdf"=s."extrait_rdppf_pdf", "idxxge_cadastre_foret"=s."idxxge_cadastre_foret", "id73_si"=s."id73_si", "id159_distances_foret"=s."id159_distances_foret", "id76_zr"=s."id76_zr", "id108_ch_plan_zone_securite"=s."id108_ch_plan_zone_securite", "id218_ch_align_instal_elec"=s."id218_ch_align_instal_elec", "id116_cadastre_site_pollues"=s."id116_cadastre_site_pollues", "id73_zones_affect_prim"=s."id73_zones_affect_prim", "id73_ps"=s."id73_ps", "id97_ch_align_instal_ferrov"=s."id97_ch_align_instal_ferrov", "id119_ch_cad_site_pollues_tp"=s."id119_ch_cad_site_pollues_tp", "id73_extract_grav"=s."id73_extract_grav", "id217_ch_zr_ltn220"=s."id217_ch_zr_ltn220", "id73_rs"=s."id73_rs", "id145_dsopb"=s."id145_dsopb", "idxxge_lim_const_lroutes"=s."idxxge_lim_const_lroutes", "shape__length"=s."shape__length", "id73_pla"=s."id73_pla", "objectid"=s."objectid", "id73_zones_affect_synt"=s."id73_zones_affect_synt", "lien_www"=s."lien_www", "iteration"=s."iteration", "created_at"=s."created_at", "updated_at"=s."updated_at", "idge_xxxx_align_const_lroutes"=s."idge_xxxx_align_const_lroutes", "idge_1175_ouvrages_souterrains"=s."idge_1175_ouvrages_souterrains", "idge_1079_cadastre_foret"=s."idge_1079_cadastre_foret", "geometry"=s."geometry"
  FROM bronze_ch.ge_rdppf_synthese s
  WHERE t.id=s.id AND ((t."id160_reserves_forestieres"::text, t."numero"::text, t."id73_zones_affect_superp"::text, t."id73_plcp"::text, t."globalid"::text, t."id73_pnp"::text, t."id131_zones_protec_eaux_sout"::text, t."id132_perim_protec_eaux_sout"::text, t."id190_ere"::text, t."id73_zp"::text, t."id73_plq"::text, t."id103_ch_zr_instal_aero"::text, t."id117_ch_cad_site_pollues_mili"::text, t."egrid"::text, t."id157_limites_foret_statiques"::text, t."genre"::text, t."id96_ch_zr_instal_ferrov"::text, t."id118_ch_cad_site_pollues_aero"::text, t."shape__area"::text, t."id73_pdzi"::text, t."id87_zr_rn"::text, t."id88_ch_align_rn"::text, t."surface"::text, t."id104_ch_align_instal_aero"::text, t."id73_pdzam"::text, t."id73_pus"::text, t."extrait_rdppf_pdf"::text, t."idxxge_cadastre_foret"::text, t."id73_si"::text, t."id159_distances_foret"::text, t."id76_zr"::text, t."id108_ch_plan_zone_securite"::text, t."id218_ch_align_instal_elec"::text, t."id116_cadastre_site_pollues"::text, t."id73_zones_affect_prim"::text, t."id73_ps"::text, t."id97_ch_align_instal_ferrov"::text, t."id119_ch_cad_site_pollues_tp"::text, t."id73_extract_grav"::text, t."id217_ch_zr_ltn220"::text, t."id73_rs"::text, t."id145_dsopb"::text, t."idxxge_lim_const_lroutes"::text, t."shape__length"::text, t."id73_pla"::text, t."objectid"::text, t."id73_zones_affect_synt"::text, t."lien_www"::text, t."iteration"::text, t."created_at"::text, t."idge_xxxx_align_const_lroutes"::text, t."idge_1175_ouvrages_souterrains"::text, t."idge_1079_cadastre_foret"::text, t."geometry"::text) IS DISTINCT FROM (s."id160_reserves_forestieres"::text, s."numero"::text, s."id73_zones_affect_superp"::text, s."id73_plcp"::text, s."globalid"::text, s."id73_pnp"::text, s."id131_zones_protec_eaux_sout"::text, s."id132_perim_protec_eaux_sout"::text, s."id190_ere"::text, s."id73_zp"::text, s."id73_plq"::text, s."id103_ch_zr_instal_aero"::text, s."id117_ch_cad_site_pollues_mili"::text, s."egrid"::text, s."id157_limites_foret_statiques"::text, s."genre"::text, s."id96_ch_zr_instal_ferrov"::text, s."id118_ch_cad_site_pollues_aero"::text, s."shape__area"::text, s."id73_pdzi"::text, s."id87_zr_rn"::text, s."id88_ch_align_rn"::text, s."surface"::text, s."id104_ch_align_instal_aero"::text, s."id73_pdzam"::text, s."id73_pus"::text, s."extrait_rdppf_pdf"::text, s."idxxge_cadastre_foret"::text, s."id73_si"::text, s."id159_distances_foret"::text, s."id76_zr"::text, s."id108_ch_plan_zone_securite"::text, s."id218_ch_align_instal_elec"::text, s."id116_cadastre_site_pollues"::text, s."id73_zones_affect_prim"::text, s."id73_ps"::text, s."id97_ch_align_instal_ferrov"::text, s."id119_ch_cad_site_pollues_tp"::text, s."id73_extract_grav"::text, s."id217_ch_zr_ltn220"::text, s."id73_rs"::text, s."id145_dsopb"::text, s."idxxge_lim_const_lroutes"::text, s."shape__length"::text, s."id73_pla"::text, s."objectid"::text, s."id73_zones_affect_synt"::text, s."lien_www"::text, s."iteration"::text, s."created_at"::text, s."idge_xxxx_align_const_lroutes"::text, s."idge_1175_ouvrages_souterrains"::text, s."idge_1079_cadastre_foret"::text, s."geometry"::text));
  INSERT INTO lamap_db_foreign.ge_rdppf_synthese ("id", "id160_reserves_forestieres", "numero", "id73_zones_affect_superp", "id73_plcp", "globalid", "id73_pnp", "id131_zones_protec_eaux_sout", "id132_perim_protec_eaux_sout", "id190_ere", "id73_zp", "id73_plq", "id103_ch_zr_instal_aero", "id117_ch_cad_site_pollues_mili", "egrid", "id157_limites_foret_statiques", "genre", "id96_ch_zr_instal_ferrov", "id118_ch_cad_site_pollues_aero", "shape__area", "id73_pdzi", "id87_zr_rn", "id88_ch_align_rn", "surface", "id104_ch_align_instal_aero", "id73_pdzam", "id73_pus", "extrait_rdppf_pdf", "idxxge_cadastre_foret", "id73_si", "id159_distances_foret", "id76_zr", "id108_ch_plan_zone_securite", "id218_ch_align_instal_elec", "id116_cadastre_site_pollues", "id73_zones_affect_prim", "id73_ps", "id97_ch_align_instal_ferrov", "id119_ch_cad_site_pollues_tp", "id73_extract_grav", "id217_ch_zr_ltn220", "id73_rs", "id145_dsopb", "idxxge_lim_const_lroutes", "shape__length", "id73_pla", "objectid", "id73_zones_affect_synt", "lien_www", "iteration", "created_at", "updated_at", "idge_xxxx_align_const_lroutes", "idge_1175_ouvrages_souterrains", "idge_1079_cadastre_foret", "geometry")
  SELECT "id", "id160_reserves_forestieres", "numero", "id73_zones_affect_superp", "id73_plcp", "globalid", "id73_pnp", "id131_zones_protec_eaux_sout", "id132_perim_protec_eaux_sout", "id190_ere", "id73_zp", "id73_plq", "id103_ch_zr_instal_aero", "id117_ch_cad_site_pollues_mili", "egrid", "id157_limites_foret_statiques", "genre", "id96_ch_zr_instal_ferrov", "id118_ch_cad_site_pollues_aero", "shape__area", "id73_pdzi", "id87_zr_rn", "id88_ch_align_rn", "surface", "id104_ch_align_instal_aero", "id73_pdzam", "id73_pus", "extrait_rdppf_pdf", "idxxge_cadastre_foret", "id73_si", "id159_distances_foret", "id76_zr", "id108_ch_plan_zone_securite", "id218_ch_align_instal_elec", "id116_cadastre_site_pollues", "id73_zones_affect_prim", "id73_ps", "id97_ch_align_instal_ferrov", "id119_ch_cad_site_pollues_tp", "id73_extract_grav", "id217_ch_zr_ltn220", "id73_rs", "id145_dsopb", "idxxge_lim_const_lroutes", "shape__length", "id73_pla", "objectid", "id73_zones_affect_synt", "lien_www", "iteration", "created_at", "updated_at", "idge_xxxx_align_const_lroutes", "idge_1175_ouvrages_souterrains", "idge_1079_cadastre_foret", "geometry" FROM bronze_ch.ge_rdppf_synthese s
  WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.ge_rdppf_synthese t WHERE t.id=s.id);
END;$procedure$

;

-- ---- sync_ge_sit_surelevation ----
CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_sit_surelevation()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.ge_sit_surelevation t SET
    egid=s.egid, lien_carte=s.lien_carte, commune=s.commune, destination=s.destination,
    globalid=s.globalid, shape__length=s.shape__length, objectid=s.objectid, remarque=s.remarque,
    shape__area=s.shape__area, iteration=s.iteration, created_at=s.created_at,
    updated_at=s.updated_at, geometry=s.geometry
  FROM bronze_ch.ge_sit_surelevation s
  WHERE t.id=s.id AND ((t."egid"::text, t."lien_carte"::text, t."commune"::text, t."destination"::text, t."globalid"::text, t."shape__length"::text, t."objectid"::text, t."remarque"::text, t."shape__area"::text, t."iteration"::text, t."created_at"::text, t."geometry"::text) IS DISTINCT FROM (s."egid"::text, s."lien_carte"::text, s."commune"::text, s."destination"::text, s."globalid"::text, s."shape__length"::text, s."objectid"::text, s."remarque"::text, s."shape__area"::text, s."iteration"::text, s."created_at"::text, s."geometry"::text));

  INSERT INTO lamap_db_foreign.ge_sit_surelevation
    (id, egid, lien_carte, commune, destination, globalid, shape__length, objectid,
     remarque, shape__area, iteration, created_at, updated_at, geometry)
  SELECT s.id, s.egid, s.lien_carte, s.commune, s.destination, s.globalid, s.shape__length,
     s.objectid, s.remarque, s.shape__area, s.iteration, s.created_at, s.updated_at, s.geometry
  FROM bronze_ch.ge_sit_surelevation s
  WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.ge_sit_surelevation t WHERE t.id=s.id);
END;$procedure$

;

-- ---- sync_uspi_knowledge ----
CREATE OR REPLACE PROCEDURE gold_ch.sync_uspi_knowledge()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.uspi_knowledge t SET module=s.module, topic=s.topic, subtopic=s.subtopic, content=s.content, key_points=s.key_points, legal_references=s.legal_references, practical_tips=s.practical_tips, common_mistakes=s.common_mistakes, related_topics=s.related_topics, source=s.source, inserted_at=s.inserted_at, updated_at=s.updated_at
  FROM bronze_ch.uspi_knowledge s WHERE t.id=s.id AND ((t."module"::text, t."topic"::text, t."subtopic"::text, t."content"::text, t."key_points"::text, t."legal_references"::text, t."practical_tips"::text, t."common_mistakes"::text, t."related_topics"::text, t."source"::text, t."inserted_at"::text) IS DISTINCT FROM (s."module"::text, s."topic"::text, s."subtopic"::text, s."content"::text, s."key_points"::text, s."legal_references"::text, s."practical_tips"::text, s."common_mistakes"::text, s."related_topics"::text, s."source"::text, s."inserted_at"::text));
  INSERT INTO lamap_db_foreign.uspi_knowledge
  SELECT s.* FROM bronze_ch.uspi_knowledge s
  WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.uspi_knowledge t WHERE t.id=s.id);
END;$procedure$

;

