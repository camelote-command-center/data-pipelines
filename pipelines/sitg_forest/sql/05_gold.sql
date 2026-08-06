-- ============================================================================
-- Geneva forest layers — gold views on re-LLM
-- ============================================================================
-- Per platform.standards / silver_promotion_pattern_ge_overlays step 4: gold is
-- a thin pass-through of silver with an explicit column list, consumed by
-- gold_ch.sync_full_refresh.
--
-- feature_key: gold_ch.sync_full_refresh Branch A builds
-- "ON CONFLICT (<pk_column>)" from a SINGLE sync_registry.pk_column. Three of
-- these layers are keyed on a composite in bronze ((erebid, geom_hash),
-- (id_dossier_key, geom_hash)), which that contract cannot express. Rather than
-- weaken the bronze keys, gold projects a single deterministic text key. The
-- component columns travel alongside it so nothing downstream has to parse the
-- key apart.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Cadastre forestier
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_forest_cadastre_full AS
SELECT
  s.geom_hash                                   AS feature_key,
  s.geom_hash,
  s.objectid,
  s.remarque,
  s.area_m2,
  s.geom_2056,
  s.geometry,
  s.canton_code,
  s.raw_data,
  s.updated_at
FROM silver_ch.cadastral_forest_cadastre s;

-- ---------------------------------------------------------------------------
-- 2. RDPPF distances forêt — surface
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_forest_distance_s_full AS
SELECT
  s.erebid::text || ':' || s.geom_hash          AS feature_key,
  s.erebid,
  s.geom_hash,
  s.objectid,
  s.commune_bfs,
  s.commune_name,
  s.statut_juridique,
  s.entree_en_force_date,
  s.lien_document,
  s.lien_plan,
  s.date_maj,
  s.area_m2,
  s.geom_2056,
  s.geometry,
  s.canton_code,
  s.raw_data,
  s.updated_at
FROM silver_ch.cadastral_forest_distance_s s;

-- ---------------------------------------------------------------------------
-- 3. RDPPF distances forêt — ligne
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_forest_distance_l_full AS
SELECT
  s.erebid::text || ':' || s.geom_hash          AS feature_key,
  s.erebid,
  s.geom_hash,
  s.objectid,
  s.commune_bfs,
  s.commune_name,
  s.statut_juridique,
  s.entree_en_force_date,
  s.lien_document,
  s.lien_plan,
  s.date_maj,
  s.length_m,
  s.geom_2056,
  s.geometry,
  s.canton_code,
  s.raw_data,
  s.updated_at
FROM silver_ch.cadastral_forest_distance_l s;

-- ---------------------------------------------------------------------------
-- 4. Lisières forestières
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_forest_lisieres_full AS
SELECT
  s.id_dossier_key || ':' || s.geom_hash        AS feature_key,
  s.id_dossier_key,
  s.geom_hash,
  s.id_dossier,
  s.objectid,
  s.type_procedure,
  s.commune_raw,
  s.commune_bfs,
  s.commune_resolution,
  s.parcelles_raw,
  s.num_autor,
  s.mz_plq,
  s.relev_etat,
  s.relev_date,
  s.etape_procedure,
  s.etat_dossier,
  s.statut_juridique,
  s.dec_natfor,
  s.dec_natfor_bool,
  s.fao_requete_date,
  s.fao_decision_date,
  s.entree_en_force_date,
  s.in_force,
  s.recours,
  s.rdppf_statut,
  s.lien_document,
  s.length_m,
  s.geom_2056,
  s.geometry,
  s.canton_code,
  s.raw_data,
  s.updated_at
FROM silver_ch.cadastral_forest_lisieres s;

-- ---------------------------------------------------------------------------
-- 5. Lisières — parcelles (child, no geometry of its own)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_forest_lisieres_parcelles_full AS
SELECT
  s.id_dossier_key || ':' || s.geom_hash || ':' || s.token_ordinal::text AS feature_key,
  s.id_dossier_key,
  s.geom_hash,
  s.id_dossier,
  s.token_ordinal,
  s.parcelles_raw,
  s.annotation,
  s.token,
  s.no_parcelle,
  s.parcelle_suffix,
  s.is_domaine_public,
  s.no_commune,
  s.commune_resolution,
  s.parse_status,
  s.canton_code,
  s.updated_at
FROM silver_ch.cadastral_forest_lisieres_parcelles s;

-- ---------------------------------------------------------------------------
-- 6. Fonction plan directeur forestier
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_forest_fonction_full AS
SELECT
  s.geom_hash                                   AS feature_key,
  s.geom_hash,
  s.objectid,
  s.fonction_type,
  s.area_m2,
  s.geom_2056,
  s.geometry,
  s.canton_code,
  s.raw_data,
  s.updated_at
FROM silver_ch.cadastral_forest_fonction s;
