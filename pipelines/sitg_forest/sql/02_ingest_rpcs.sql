-- ============================================================================
-- Geneva forest layers — bronze ingest RPCs on re-LLM
-- ============================================================================
-- Batch-upsert RPCs following the established
-- public.upsert_federal_cadastral_parcels_batch pattern: the parser POSTs a
-- JSONB array to a SECURITY DEFINER function in `public`, and all geometry and
-- key derivation happens server-side. This keeps the parser free of PostGIS
-- and means CI needs only RE_LLM_SUPABASE_SERVICE_ROLE_KEY, never a DB URI.
--
-- Payload shape, one element per feature:
--     {"attrs": {<raw ArcGIS attributes, original field names>},
--      "geom":  {<GeoJSON geometry in LV95 coordinates>} | null}
--
-- The GeoJSON carries LV95 coordinates because the fetch sets outSR=2056, but
-- GeoJSON has no CRS member, so ST_GeomFromGeoJSON yields SRID 0 and we stamp
-- 2056 with ST_SetSRID. Do NOT use ST_Transform here: the coordinates are
-- already LV95 and transforming would move every feature.
--
-- Batches are de-duplicated on the primary key with DISTINCT ON before the
-- INSERT, because ON CONFLICT DO UPDATE cannot touch the same row twice in one
-- statement. The count of rows collapsed this way is returned as `collapsed`
-- so a real key collision surfaces in the run log instead of failing the load.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------------------------

-- ArcGIS f=json returns dates as epoch milliseconds. NULL and 0 both mean
-- "no date" in this dataset.
CREATE OR REPLACE FUNCTION public.ge_forest_epoch_ms_to_date(p_ms double precision)
RETURNS date
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT CASE
           WHEN p_ms IS NULL OR p_ms = 0 THEN NULL
           ELSE (to_timestamp(p_ms / 1000.0) AT TIME ZONE 'UTC')::date
         END;
$$;

COMMENT ON FUNCTION public.ge_forest_epoch_ms_to_date(double precision) IS
  'SITG ArcGIS f=json epoch-milliseconds to date. 0 and NULL both map to NULL.';

-- The deterministic geometry surrogate used as the key on every forest layer.
-- Precision reduced to 1 cm before hashing so that coordinate rounding at
-- source does not read as a delete plus an insert on the next quarterly run.
--
-- Two failure modes were measured on the live layers on 2026-08-06 and both
-- are handled here rather than allowed to corrupt the load:
--
-- 1. ST_ReducePrecision THROWS on topologically invalid input. Three polygons
--    in FFP_CADASTRE_FORET have ring self-intersections, and one polygon in
--    RDPPF_DISTANCES_FORET_S raised
--      "TopologyException: side location conflict at 2496003.88 1114568.55".
--    That single row aborted the whole 856-row layer. ST_MakeValid is applied
--    first, with a full-precision fallback if GEOS still refuses.
--
-- 2. ST_ReducePrecision COLLAPSES degenerate slivers to EMPTY, and every empty
--    geometry hashes identically. On the first load that merged a genuine
--    sliver in FFP_CADASTRE_FORET with the layer's one zero-area feature
--    (OBJECTID 156, Shape__Area = 0 at source) and lost the sliver. When the
--    reduced geometry is empty or null we now fall back to hashing the geometry
--    at full precision, which keeps slivers distinct. OBJECTID 156 itself has
--    no geometry to keep and is excluded by the no-geometry guard, taking
--    FFP_CADASTRE_FORET from 839 source features to 838 usable polygons.
--
-- The repaired geometry is used ONLY to derive the key. Bronze stores the
-- source geometry verbatim; validity repair belongs to silver, where it is
-- counted and reported.
CREATE OR REPLACE FUNCTION public.ge_forest_geom_hash(p_geom geometry)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
  v_reduced geometry;
BEGIN
  IF p_geom IS NULL OR ST_IsEmpty(p_geom) THEN
    RETURN NULL;
  END IF;

  BEGIN
    v_reduced := ST_ReducePrecision(ST_MakeValid(p_geom), 0.01);
  EXCEPTION WHEN OTHERS THEN
    v_reduced := NULL;
  END;

  -- Degenerate or unrepairable: key on full precision so slivers stay distinct.
  IF v_reduced IS NULL OR ST_IsEmpty(v_reduced) THEN
    RETURN md5(ST_AsBinary(p_geom));
  END IF;

  RETURN md5(ST_AsBinary(v_reduced));
END;
$$;

COMMENT ON FUNCTION public.ge_forest_geom_hash(geometry) IS
  'Deterministic surrogate key for SITG forest layers. SITG states OBJECTID must not be used as a permanent identifier, and EREBID / ID_DOSSIER are both non-unique in practice.';

-- Parse a GeoJSON geometry that is already in LV95 into SRID 2056.
CREATE OR REPLACE FUNCTION public.ge_forest_geom_from_geojson(p_geom jsonb)
RETURNS geometry
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
  v_geom geometry;
BEGIN
  IF p_geom IS NULL OR jsonb_typeof(p_geom) = 'null' THEN
    RETURN NULL;
  END IF;
  BEGIN
    v_geom := ST_SetSRID(ST_GeomFromGeoJSON(p_geom::text), 2056);
  EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
  END;
  RETURN v_geom;
END;
$$;

-- ---------------------------------------------------------------------------
-- Return signature carries skipped_no_geom, so the drops are required: a
-- CREATE OR REPLACE cannot change a function's return type.
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.upsert_ge_ffp_cadastre_foret_batch(jsonb, uuid);
DROP FUNCTION IF EXISTS public.upsert_ge_rdppf_distances_foret_s_batch(jsonb, uuid);
DROP FUNCTION IF EXISTS public.upsert_ge_rdppf_distances_foret_l_batch(jsonb, uuid);
DROP FUNCTION IF EXISTS public.upsert_ge_ffp_lisieres_forestieres_batch(jsonb, uuid);
DROP FUNCTION IF EXISTS public.upsert_ge_ffp_fonction_pdf_batch(jsonb, uuid);

-- ---------------------------------------------------------------------------
-- 1. FFP_CADASTRE_FORET
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.upsert_ge_ffp_cadastre_foret_batch(
  p_rows jsonb,
  p_run_id uuid
)
RETURNS TABLE (received integer, skipped_no_geom integer, collapsed integer, affected integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'bronze_ch', 'pg_catalog'
SET statement_timeout TO '300s'
AS $$
DECLARE
  v_received  integer;
  v_withgeom  integer;
  v_distinct  integer;
  v_affected  integer;
BEGIN
  CREATE TEMP TABLE _stage ON COMMIT DROP AS
  SELECT
    (e->'attrs')                                            AS attrs,
    public.ge_forest_geom_from_geojson(e->'geom')            AS geometry
  FROM jsonb_array_elements(p_rows) AS e;

  SELECT count(*) INTO v_received FROM _stage;

  DELETE FROM _stage WHERE geometry IS NULL OR ST_IsEmpty(geometry);
  SELECT count(*) INTO v_withgeom FROM _stage;

  CREATE TEMP TABLE _keyed ON COMMIT DROP AS
  SELECT DISTINCT ON (public.ge_forest_geom_hash(geometry))
    public.ge_forest_geom_hash(geometry)          AS geom_hash,
    (attrs->>'OBJECTID')::integer                 AS objectid,
    attrs->>'REMARQUE'                            AS remarque,
    (attrs->>'Shape__Area')::double precision     AS shape_area,
    (attrs->>'Shape__Length')::double precision   AS shape_length,
    geometry,
    attrs                                         AS raw_data
  FROM _stage
  ORDER BY public.ge_forest_geom_hash(geometry), (attrs->>'OBJECTID')::integer;

  SELECT count(*) INTO v_distinct FROM _keyed;

  INSERT INTO bronze_ch.ge_ffp_cadastre_foret AS t
    (geom_hash, objectid, remarque, shape_area, shape_length, geometry,
     raw_data, run_id, ingested_at, first_seen_at, last_seen_at)
  SELECT geom_hash, objectid, remarque, shape_area, shape_length, geometry,
         raw_data, p_run_id, now(), now(), now()
  FROM _keyed
  ON CONFLICT (geom_hash) DO UPDATE SET
    objectid     = EXCLUDED.objectid,
    remarque     = EXCLUDED.remarque,
    shape_area   = EXCLUDED.shape_area,
    shape_length = EXCLUDED.shape_length,
    geometry     = EXCLUDED.geometry,
    raw_data     = EXCLUDED.raw_data,
    run_id       = EXCLUDED.run_id,
    ingested_at  = EXCLUDED.ingested_at,
    last_seen_at = EXCLUDED.last_seen_at,
    deleted_at   = NULL;

  GET DIAGNOSTICS v_affected = ROW_COUNT;
  RETURN QUERY SELECT v_received, v_received - v_withgeom, v_withgeom - v_distinct, v_affected;
END;
$$;

-- ---------------------------------------------------------------------------
-- 2 + 3. RDPPF_DISTANCES_FORET_S / _L
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.upsert_ge_rdppf_distances_foret_s_batch(
  p_rows jsonb,
  p_run_id uuid
)
RETURNS TABLE (received integer, skipped_no_geom integer, collapsed integer, affected integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'bronze_ch', 'pg_catalog'
SET statement_timeout TO '300s'
AS $$
DECLARE
  v_received integer;
  v_withgeom integer;
  v_distinct integer;
  v_affected integer;
BEGIN
  CREATE TEMP TABLE _stage ON COMMIT DROP AS
  SELECT (e->'attrs') AS attrs,
         public.ge_forest_geom_from_geojson(e->'geom') AS geometry
  FROM jsonb_array_elements(p_rows) AS e;

  SELECT count(*) INTO v_received FROM _stage;

  DELETE FROM _stage WHERE geometry IS NULL OR ST_IsEmpty(geometry);
  SELECT count(*) INTO v_withgeom FROM _stage;

  CREATE TEMP TABLE _keyed ON COMMIT DROP AS
  SELECT DISTINCT ON ((attrs->>'EREBID')::integer, public.ge_forest_geom_hash(geometry))
    (attrs->>'EREBID')::integer                   AS erebid,
    public.ge_forest_geom_hash(geometry)          AS geom_hash,
    (attrs->>'OBJECTID')::integer                 AS objectid,
    (attrs->>'COMMUNE')::smallint                 AS commune,
    attrs->>'STATUT_JURIDIQUE'                    AS statut_juridique,
    public.ge_forest_epoch_ms_to_date((attrs->>'ENTREE_EN_FORCE_DATE')::double precision) AS entree_en_force_date,
    attrs->>'LIEN_DOCUMENT'                       AS lien_document,
    attrs->>'LIEN_PLAN'                           AS lien_plan,
    public.ge_forest_epoch_ms_to_date((attrs->>'DATE_MAJ')::double precision) AS date_maj,
    (attrs->>'Shape__Area')::double precision     AS shape_area,
    (attrs->>'Shape__Length')::double precision   AS shape_length,
    geometry,
    attrs                                         AS raw_data
  FROM _stage
  WHERE attrs->>'EREBID' IS NOT NULL
  ORDER BY (attrs->>'EREBID')::integer, public.ge_forest_geom_hash(geometry), (attrs->>'OBJECTID')::integer;

  SELECT count(*) INTO v_distinct FROM _keyed;

  INSERT INTO bronze_ch.ge_rdppf_distances_foret_s AS t
    (erebid, geom_hash, objectid, commune, statut_juridique, entree_en_force_date,
     lien_document, lien_plan, date_maj, shape_area, shape_length, geometry,
     raw_data, run_id, ingested_at, first_seen_at, last_seen_at)
  SELECT erebid, geom_hash, objectid, commune, statut_juridique, entree_en_force_date,
         lien_document, lien_plan, date_maj, shape_area, shape_length, geometry,
         raw_data, p_run_id, now(), now(), now()
  FROM _keyed
  ON CONFLICT (erebid, geom_hash) DO UPDATE SET
    objectid             = EXCLUDED.objectid,
    commune              = EXCLUDED.commune,
    statut_juridique     = EXCLUDED.statut_juridique,
    entree_en_force_date = EXCLUDED.entree_en_force_date,
    lien_document        = EXCLUDED.lien_document,
    lien_plan            = EXCLUDED.lien_plan,
    date_maj             = EXCLUDED.date_maj,
    shape_area           = EXCLUDED.shape_area,
    shape_length         = EXCLUDED.shape_length,
    geometry             = EXCLUDED.geometry,
    raw_data             = EXCLUDED.raw_data,
    run_id               = EXCLUDED.run_id,
    ingested_at          = EXCLUDED.ingested_at,
    last_seen_at         = EXCLUDED.last_seen_at,
    deleted_at           = NULL;

  GET DIAGNOSTICS v_affected = ROW_COUNT;
  RETURN QUERY SELECT v_received, v_received - v_withgeom, v_withgeom - v_distinct, v_affected;
END;
$$;

CREATE OR REPLACE FUNCTION public.upsert_ge_rdppf_distances_foret_l_batch(
  p_rows jsonb,
  p_run_id uuid
)
RETURNS TABLE (received integer, skipped_no_geom integer, collapsed integer, affected integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'bronze_ch', 'pg_catalog'
SET statement_timeout TO '300s'
AS $$
DECLARE
  v_received integer;
  v_withgeom integer;
  v_distinct integer;
  v_affected integer;
BEGIN
  CREATE TEMP TABLE _stage ON COMMIT DROP AS
  SELECT (e->'attrs') AS attrs,
         public.ge_forest_geom_from_geojson(e->'geom') AS geometry
  FROM jsonb_array_elements(p_rows) AS e;

  SELECT count(*) INTO v_received FROM _stage;

  DELETE FROM _stage WHERE geometry IS NULL OR ST_IsEmpty(geometry);
  SELECT count(*) INTO v_withgeom FROM _stage;

  CREATE TEMP TABLE _keyed ON COMMIT DROP AS
  SELECT DISTINCT ON ((attrs->>'EREBID')::integer, public.ge_forest_geom_hash(geometry))
    (attrs->>'EREBID')::integer                   AS erebid,
    public.ge_forest_geom_hash(geometry)          AS geom_hash,
    (attrs->>'OBJECTID')::integer                 AS objectid,
    (attrs->>'COMMUNE')::smallint                 AS commune,
    attrs->>'STATUT_JURIDIQUE'                    AS statut_juridique,
    public.ge_forest_epoch_ms_to_date((attrs->>'ENTREE_EN_FORCE_DATE')::double precision) AS entree_en_force_date,
    attrs->>'LIEN_DOCUMENT'                       AS lien_document,
    attrs->>'LIEN_PLAN'                           AS lien_plan,
    public.ge_forest_epoch_ms_to_date((attrs->>'DATE_MAJ')::double precision) AS date_maj,
    (attrs->>'Shape__Length')::double precision   AS shape_length,
    geometry,
    attrs                                         AS raw_data
  FROM _stage
  WHERE attrs->>'EREBID' IS NOT NULL
  ORDER BY (attrs->>'EREBID')::integer, public.ge_forest_geom_hash(geometry), (attrs->>'OBJECTID')::integer;

  SELECT count(*) INTO v_distinct FROM _keyed;

  INSERT INTO bronze_ch.ge_rdppf_distances_foret_l AS t
    (erebid, geom_hash, objectid, commune, statut_juridique, entree_en_force_date,
     lien_document, lien_plan, date_maj, shape_length, geometry,
     raw_data, run_id, ingested_at, first_seen_at, last_seen_at)
  SELECT erebid, geom_hash, objectid, commune, statut_juridique, entree_en_force_date,
         lien_document, lien_plan, date_maj, shape_length, geometry,
         raw_data, p_run_id, now(), now(), now()
  FROM _keyed
  ON CONFLICT (erebid, geom_hash) DO UPDATE SET
    objectid             = EXCLUDED.objectid,
    commune              = EXCLUDED.commune,
    statut_juridique     = EXCLUDED.statut_juridique,
    entree_en_force_date = EXCLUDED.entree_en_force_date,
    lien_document        = EXCLUDED.lien_document,
    lien_plan            = EXCLUDED.lien_plan,
    date_maj             = EXCLUDED.date_maj,
    shape_length         = EXCLUDED.shape_length,
    geometry             = EXCLUDED.geometry,
    raw_data             = EXCLUDED.raw_data,
    run_id               = EXCLUDED.run_id,
    ingested_at          = EXCLUDED.ingested_at,
    last_seen_at         = EXCLUDED.last_seen_at,
    deleted_at           = NULL;

  GET DIAGNOSTICS v_affected = ROW_COUNT;
  RETURN QUERY SELECT v_received, v_received - v_withgeom, v_withgeom - v_distinct, v_affected;
END;
$$;

-- ---------------------------------------------------------------------------
-- 4. FFP_LISIERES_FORESTIERES
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.upsert_ge_ffp_lisieres_forestieres_batch(
  p_rows jsonb,
  p_run_id uuid
)
RETURNS TABLE (received integer, skipped_no_geom integer, collapsed integer, affected integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'bronze_ch', 'pg_catalog'
SET statement_timeout TO '300s'
AS $$
DECLARE
  v_received integer;
  v_withgeom integer;
  v_distinct integer;
  v_affected integer;
BEGIN
  CREATE TEMP TABLE _stage ON COMMIT DROP AS
  SELECT (e->'attrs') AS attrs,
         public.ge_forest_geom_from_geojson(e->'geom') AS geometry
  FROM jsonb_array_elements(p_rows) AS e;

  SELECT count(*) INTO v_received FROM _stage;

  DELETE FROM _stage WHERE geometry IS NULL OR ST_IsEmpty(geometry);
  SELECT count(*) INTO v_withgeom FROM _stage;

  CREATE TEMP TABLE _keyed ON COMMIT DROP AS
  SELECT DISTINCT ON (coalesce(attrs->>'ID_DOSSIER', ''), public.ge_forest_geom_hash(geometry))
    coalesce(attrs->>'ID_DOSSIER', '')            AS id_dossier_key,
    public.ge_forest_geom_hash(geometry)          AS geom_hash,
    attrs->>'ID_DOSSIER'                          AS id_dossier,
    (attrs->>'OBJECTID')::integer                 AS objectid,
    attrs->>'TYPE_PROCEDURE'                      AS type_procedure,
    attrs->>'COMMUNE'                             AS commune,
    attrs->>'PARCELLES'                           AS parcelles,
    attrs->>'NUM_AUTOR'                           AS num_autor,
    attrs->>'MZ_PLQ'                              AS mz_plq,
    attrs->>'RELEV_ETAT'                          AS relev_etat,
    public.ge_forest_epoch_ms_to_date((attrs->>'RELEV_DATE')::double precision)           AS relev_date,
    attrs->>'ETAPE_PROCEDURE'                     AS etape_procedure,
    attrs->>'ETAT_DOSSIER'                        AS etat_dossier,
    attrs->>'STATUT_JURIDIQUE'                    AS statut_juridique,
    attrs->>'DEC_NATFOR'                          AS dec_natfor,
    public.ge_forest_epoch_ms_to_date((attrs->>'FAO_REQUETE_DATE')::double precision)     AS fao_requete_date,
    public.ge_forest_epoch_ms_to_date((attrs->>'FAO_DECISION_DATE')::double precision)    AS fao_decision_date,
    public.ge_forest_epoch_ms_to_date((attrs->>'ENTREE_EN_FORCE_DATE')::double precision) AS entree_en_force_date,
    attrs->>'RECOURS'                             AS recours,
    attrs->>'RDPPF_STATUT'                        AS rdppf_statut,
    attrs->>'LIEN_DOCUMENT'                       AS lien_document,
    (attrs->>'Shape__Length')::double precision   AS shape_length,
    geometry,
    attrs                                         AS raw_data
  FROM _stage
  ORDER BY coalesce(attrs->>'ID_DOSSIER', ''), public.ge_forest_geom_hash(geometry), (attrs->>'OBJECTID')::integer;

  SELECT count(*) INTO v_distinct FROM _keyed;

  INSERT INTO bronze_ch.ge_ffp_lisieres_forestieres AS t
    (id_dossier_key, geom_hash, id_dossier, objectid, type_procedure, commune,
     parcelles, num_autor, mz_plq, relev_etat, relev_date, etape_procedure,
     etat_dossier, statut_juridique, dec_natfor, fao_requete_date,
     fao_decision_date, entree_en_force_date, recours, rdppf_statut,
     lien_document, shape_length, geometry, raw_data, run_id,
     ingested_at, first_seen_at, last_seen_at)
  SELECT id_dossier_key, geom_hash, id_dossier, objectid, type_procedure, commune,
         parcelles, num_autor, mz_plq, relev_etat, relev_date, etape_procedure,
         etat_dossier, statut_juridique, dec_natfor, fao_requete_date,
         fao_decision_date, entree_en_force_date, recours, rdppf_statut,
         lien_document, shape_length, geometry, raw_data, p_run_id,
         now(), now(), now()
  FROM _keyed
  ON CONFLICT (id_dossier_key, geom_hash) DO UPDATE SET
    id_dossier           = EXCLUDED.id_dossier,
    objectid             = EXCLUDED.objectid,
    type_procedure       = EXCLUDED.type_procedure,
    commune              = EXCLUDED.commune,
    parcelles            = EXCLUDED.parcelles,
    num_autor            = EXCLUDED.num_autor,
    mz_plq               = EXCLUDED.mz_plq,
    relev_etat           = EXCLUDED.relev_etat,
    relev_date           = EXCLUDED.relev_date,
    etape_procedure      = EXCLUDED.etape_procedure,
    etat_dossier         = EXCLUDED.etat_dossier,
    statut_juridique     = EXCLUDED.statut_juridique,
    dec_natfor           = EXCLUDED.dec_natfor,
    fao_requete_date     = EXCLUDED.fao_requete_date,
    fao_decision_date    = EXCLUDED.fao_decision_date,
    entree_en_force_date = EXCLUDED.entree_en_force_date,
    recours              = EXCLUDED.recours,
    rdppf_statut         = EXCLUDED.rdppf_statut,
    lien_document        = EXCLUDED.lien_document,
    shape_length         = EXCLUDED.shape_length,
    geometry             = EXCLUDED.geometry,
    raw_data             = EXCLUDED.raw_data,
    run_id               = EXCLUDED.run_id,
    ingested_at          = EXCLUDED.ingested_at,
    last_seen_at         = EXCLUDED.last_seen_at,
    deleted_at           = NULL;

  GET DIAGNOSTICS v_affected = ROW_COUNT;
  RETURN QUERY SELECT v_received, v_received - v_withgeom, v_withgeom - v_distinct, v_affected;
END;
$$;

-- ---------------------------------------------------------------------------
-- 5. FFP_FONCTION_PDF
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.upsert_ge_ffp_fonction_pdf_batch(
  p_rows jsonb,
  p_run_id uuid
)
RETURNS TABLE (received integer, skipped_no_geom integer, collapsed integer, affected integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'bronze_ch', 'pg_catalog'
SET statement_timeout TO '300s'
AS $$
DECLARE
  v_received integer;
  v_withgeom integer;
  v_distinct integer;
  v_affected integer;
BEGIN
  CREATE TEMP TABLE _stage ON COMMIT DROP AS
  SELECT (e->'attrs') AS attrs,
         public.ge_forest_geom_from_geojson(e->'geom') AS geometry
  FROM jsonb_array_elements(p_rows) AS e;

  SELECT count(*) INTO v_received FROM _stage;

  DELETE FROM _stage WHERE geometry IS NULL OR ST_IsEmpty(geometry);
  SELECT count(*) INTO v_withgeom FROM _stage;

  CREATE TEMP TABLE _keyed ON COMMIT DROP AS
  SELECT DISTINCT ON (public.ge_forest_geom_hash(geometry))
    public.ge_forest_geom_hash(geometry)          AS geom_hash,
    (attrs->>'OBJECTID')::integer                 AS objectid,
    attrs->>'TYPE'                                AS type,
    (attrs->>'Shape__Area')::double precision     AS shape_area,
    (attrs->>'Shape__Length')::double precision   AS shape_length,
    geometry,
    attrs                                         AS raw_data
  FROM _stage
  ORDER BY public.ge_forest_geom_hash(geometry), (attrs->>'OBJECTID')::integer;

  SELECT count(*) INTO v_distinct FROM _keyed;

  INSERT INTO bronze_ch.ge_ffp_fonction_pdf AS t
    (geom_hash, objectid, type, shape_area, shape_length, geometry,
     raw_data, run_id, ingested_at, first_seen_at, last_seen_at)
  SELECT geom_hash, objectid, type, shape_area, shape_length, geometry,
         raw_data, p_run_id, now(), now(), now()
  FROM _keyed
  ON CONFLICT (geom_hash) DO UPDATE SET
    objectid     = EXCLUDED.objectid,
    type         = EXCLUDED.type,
    shape_area   = EXCLUDED.shape_area,
    shape_length = EXCLUDED.shape_length,
    geometry     = EXCLUDED.geometry,
    raw_data     = EXCLUDED.raw_data,
    run_id       = EXCLUDED.run_id,
    ingested_at  = EXCLUDED.ingested_at,
    last_seen_at = EXCLUDED.last_seen_at,
    deleted_at   = NULL;

  GET DIAGNOSTICS v_affected = ROW_COUNT;
  RETURN QUERY SELECT v_received, v_received - v_withgeom, v_withgeom - v_distinct, v_affected;
END;
$$;

-- ---------------------------------------------------------------------------
-- Soft delete: mark rows not seen in the current run
-- ---------------------------------------------------------------------------
-- Never a hard DELETE. A row that reappears on a later run has deleted_at
-- reset to NULL by the upsert clauses above.
CREATE OR REPLACE FUNCTION public.ge_forest_soft_delete_missing(
  p_table text,
  p_run_id uuid
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'bronze_ch', 'pg_catalog'
SET statement_timeout TO '300s'
AS $$
DECLARE
  v_count integer;
BEGIN
  IF p_table NOT IN ('ge_ffp_cadastre_foret',
                     'ge_rdppf_distances_foret_s',
                     'ge_rdppf_distances_foret_l',
                     'ge_ffp_lisieres_forestieres',
                     'ge_ffp_fonction_pdf') THEN
    RAISE EXCEPTION 'ge_forest_soft_delete_missing: % is not a forest bronze table', p_table;
  END IF;

  EXECUTE format(
    'UPDATE bronze_ch.%I SET deleted_at = now() WHERE run_id <> $1 AND deleted_at IS NULL',
    p_table
  ) USING p_run_id;

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

-- ---------------------------------------------------------------------------
-- Grants: service_role only. These write to bronze; anon and authenticated
-- must never reach them (granting anon on a collector RPC is what silently
-- blinded two monitored DBs for a fortnight).
-- ---------------------------------------------------------------------------
REVOKE ALL ON FUNCTION public.upsert_ge_ffp_cadastre_foret_batch(jsonb, uuid)        FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.upsert_ge_rdppf_distances_foret_s_batch(jsonb, uuid)   FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.upsert_ge_rdppf_distances_foret_l_batch(jsonb, uuid)   FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.upsert_ge_ffp_lisieres_forestieres_batch(jsonb, uuid)  FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.upsert_ge_ffp_fonction_pdf_batch(jsonb, uuid)          FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.ge_forest_soft_delete_missing(text, uuid)              FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.upsert_ge_ffp_cadastre_foret_batch(jsonb, uuid)       TO service_role;
GRANT EXECUTE ON FUNCTION public.upsert_ge_rdppf_distances_foret_s_batch(jsonb, uuid)  TO service_role;
GRANT EXECUTE ON FUNCTION public.upsert_ge_rdppf_distances_foret_l_batch(jsonb, uuid)  TO service_role;
GRANT EXECUTE ON FUNCTION public.upsert_ge_ffp_lisieres_forestieres_batch(jsonb, uuid) TO service_role;
GRANT EXECUTE ON FUNCTION public.upsert_ge_ffp_fonction_pdf_batch(jsonb, uuid)         TO service_role;
GRANT EXECUTE ON FUNCTION public.ge_forest_soft_delete_missing(text, uuid)             TO service_role;

NOTIFY pgrst, 'reload schema';
