-- ═══════════════════════════════════════════════════════════════════════════
-- SITG tree cadastre — bronze ingest RPCs on re-LLM
--
-- Same contract as the forest parser: the loader POSTs a JSONB array to a
-- SECURITY DEFINER function in `public`, and all geometry and type coercion
-- happens server-side, so CI needs only RE_LLM_SUPABASE_SERVICE_ROLE_KEY and
-- never a DB URI.
--
-- Payload element: {"attrs": {<raw ArcGIS attributes>}, "geom": {<GeoJSON>}|null}
--
-- outSR=2056 means the GeoJSON already carries LV95 coordinates. GeoJSON has no
-- CRS member, so ST_GeomFromGeoJSON yields SRID 0 and we stamp 2056 with
-- ST_SetSRID. Do NOT ST_Transform: the coordinates are already LV95 and
-- transforming would move every tree across the canton.
--
-- CONFLICT TARGET IS content_key, NOT globalid (bug f69a9dcb). SITG regenerates
-- globalid on wholesale republish -- measured ZERO overlap across two
-- consecutive publications of the remarquables layer -- and its own metadata
-- says OBJECTID is not a permanent identifier either. The key is computed
-- server-side by public.ge_trees_content_key(geom, species, measure).
--
-- Batches are de-duplicated on content_key with DISTINCT ON before the INSERT,
-- because ON CONFLICT DO UPDATE cannot touch the same row twice in one
-- statement. The number collapsed is RETURNED, not swallowed, so a real key
-- collision shows up in the run log instead of silently shrinking the layer.
-- ═══════════════════════════════════════════════════════════════════════════

-- ArcGIS f=json emits dates as epoch milliseconds. 0 and NULL both mean "none".
CREATE OR REPLACE FUNCTION public.ge_trees_epoch_ms_to_ts(p_ms double precision)
RETURNS timestamptz
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT CASE WHEN p_ms IS NULL OR p_ms = 0 THEN NULL
              ELSE to_timestamp(p_ms / 1000.0) END;
$$;

COMMENT ON FUNCTION public.ge_trees_epoch_ms_to_ts(double precision) IS
  'SITG ArcGIS f=json epoch-milliseconds to timestamptz. 0 and NULL both map to NULL.';


-- ---------------------------------------------------------------------------
-- SIPV_ICA_ARBRE_ISOLE
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.upsert_ge_sipv_arbre_isole_batch(p_rows jsonb, p_run_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, bronze_ch, pg_catalog
AS $$
DECLARE
  v_in        int;
  v_upserted  int;
BEGIN
  SELECT jsonb_array_length(p_rows) INTO v_in;

  WITH src AS (
    SELECT e->'attrs' AS a, e->'geom' AS g
    FROM jsonb_array_elements(p_rows) e
    WHERE nullif(e->'attrs'->>'globalid','') IS NOT NULL
  ), keyed AS (
    SELECT a, g,
           public.ge_trees_content_key(
             CASE WHEN g IS NULL OR g = 'null'::jsonb THEN NULL
                  ELSE ST_SetSRID(ST_GeomFromGeoJSON(g::text), 2056) END,
             a->>'nom_complet',
             nullif(a->>'circonference_1m','')::numeric) AS ck
    FROM src
  ), dedup AS (
    -- Lowest objectid wins, matching the migration: for the double-published
    -- pairs the lower id is the original.
    SELECT DISTINCT ON (ck) a, g, ck
    FROM keyed
    ORDER BY ck, (a->>'objectid')::bigint ASC NULLS LAST
  ), ins AS (
    INSERT INTO bronze_ch.ge_sipv_arbre_isole AS t (
      content_key, last_run_id, globalid, objectid, id_arbre, no_inventaire, nom_complet, classe,
      remarquable, situation, type_plantation, nombre_troncs,
      circonference_1m, diametre_1m, hauteur_tronc, hauteur_totale,
      diametre_couronne, rayon_couronne, forme, stade_developpement, vitalite,
      conduite, type_sol, type_surface, esperance_vie, souche, statut,
      id_acteur, date_plantation, date_plantation_estimee, date_observation,
      geom, updated_at, deleted_at)
    SELECT
      ck,
      p_run_id,
      a->>'globalid',
      nullif(a->>'objectid','')::bigint,
      nullif(a->>'id_arbre','')::numeric::bigint,
      nullif(btrim(a->>'no_inventaire'),''),
      nullif(btrim(a->>'nom_complet'),''),
      nullif(btrim(a->>'classe'),''),
      nullif(btrim(a->>'remarquable'),''),
      nullif(btrim(a->>'situation'),''),
      nullif(btrim(a->>'type_plantation'),''),
      nullif(btrim(a->>'nombre_troncs'),''),
      nullif(a->>'circonference_1m','')::numeric,
      nullif(a->>'diametre_1m','')::numeric,
      nullif(a->>'hauteur_tronc','')::numeric,
      nullif(a->>'hauteur_totale','')::numeric,
      nullif(a->>'diametre_couronne','')::numeric,
      nullif(a->>'rayon_couronne','')::numeric,
      nullif(btrim(a->>'forme'),''),
      nullif(btrim(a->>'stade_developpement'),''),
      nullif(btrim(a->>'vitalite'),''),
      nullif(btrim(a->>'conduite'),''),
      nullif(btrim(a->>'type_sol'),''),
      nullif(btrim(a->>'type_surface'),''),
      nullif(a->>'esperance_vie','')::numeric,
      nullif(btrim(a->>'souche'),''),
      nullif(btrim(a->>'statut'),''),
      nullif(btrim(a->>'id_acteur'),''),
      public.ge_trees_epoch_ms_to_ts(nullif(a->>'date_plantation','')::double precision),
      nullif(a->>'date_plantation_estimee','')::numeric,
      public.ge_trees_epoch_ms_to_ts(nullif(a->>'date_observation','')::double precision),
      CASE WHEN g IS NULL OR g = 'null'::jsonb THEN NULL
           ELSE ST_SetSRID(ST_GeomFromGeoJSON(g::text), 2056) END,
      now(),
      NULL                                   -- reappearing rows un-delete
    FROM dedup
    ON CONFLICT (content_key) DO UPDATE SET
      last_run_id             = EXCLUDED.last_run_id,
      globalid                = EXCLUDED.globalid,
      objectid                = EXCLUDED.objectid,
      id_arbre                = EXCLUDED.id_arbre,
      no_inventaire           = EXCLUDED.no_inventaire,
      nom_complet             = EXCLUDED.nom_complet,
      classe                  = EXCLUDED.classe,
      remarquable             = EXCLUDED.remarquable,
      situation               = EXCLUDED.situation,
      type_plantation         = EXCLUDED.type_plantation,
      nombre_troncs           = EXCLUDED.nombre_troncs,
      circonference_1m        = EXCLUDED.circonference_1m,
      diametre_1m             = EXCLUDED.diametre_1m,
      hauteur_tronc           = EXCLUDED.hauteur_tronc,
      hauteur_totale          = EXCLUDED.hauteur_totale,
      diametre_couronne       = EXCLUDED.diametre_couronne,
      rayon_couronne          = EXCLUDED.rayon_couronne,
      forme                   = EXCLUDED.forme,
      stade_developpement     = EXCLUDED.stade_developpement,
      vitalite                = EXCLUDED.vitalite,
      conduite                = EXCLUDED.conduite,
      type_sol                = EXCLUDED.type_sol,
      type_surface            = EXCLUDED.type_surface,
      esperance_vie           = EXCLUDED.esperance_vie,
      souche                  = EXCLUDED.souche,
      statut                  = EXCLUDED.statut,
      id_acteur               = EXCLUDED.id_acteur,
      date_plantation         = EXCLUDED.date_plantation,
      date_plantation_estimee = EXCLUDED.date_plantation_estimee,
      date_observation        = EXCLUDED.date_observation,
      geom                    = EXCLUDED.geom,
      updated_at              = now(),
      deleted_at              = NULL
    RETURNING 1)
  SELECT count(*) INTO v_upserted FROM ins;

  RETURN jsonb_build_object(
    'received',  v_in,
    'upserted',  v_upserted,
    'collapsed', v_in - v_upserted);
END;
$$;

REVOKE ALL ON FUNCTION public.upsert_ge_sipv_arbre_isole_batch(jsonb, uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.upsert_ge_sipv_arbre_isole_batch(jsonb, uuid) TO service_role;


-- ---------------------------------------------------------------------------
-- FFP_ARBRES_REMARQUABLES
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.upsert_ge_ffp_arbres_remarquables_batch(p_rows jsonb, p_run_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, bronze_ch, pg_catalog
AS $$
DECLARE
  v_in        int;
  v_upserted  int;
BEGIN
  SELECT jsonb_array_length(p_rows) INTO v_in;

  WITH src AS (
    SELECT e->'attrs' AS a, e->'geom' AS g
    FROM jsonb_array_elements(p_rows) e
    WHERE nullif(e->'attrs'->>'globalid','') IS NOT NULL
  ), keyed AS (
    SELECT a, g,
           public.ge_trees_content_key(
             CASE WHEN g IS NULL OR g = 'null'::jsonb THEN NULL
                  ELSE ST_SetSRID(ST_GeomFromGeoJSON(g::text), 2056) END,
             a->>'espece',
             nullif(a->>'diametre_tronc','')::numeric) AS ck
    FROM src
  ), dedup AS (
    SELECT DISTINCT ON (ck) a, g, ck
    FROM keyed
    ORDER BY ck, (a->>'objectid')::bigint ASC NULLS LAST
  ), ins AS (
    INSERT INTO bronze_ch.ge_ffp_arbres_remarquables AS t (
      content_key, last_run_id, globalid, objectid, id_arbre, espece, diametre_tronc,
      interet_1, interet_2, interet_3, etat, remarque, geom,
      updated_at, deleted_at)
    SELECT
      ck,
      p_run_id,
      a->>'globalid',
      nullif(a->>'objectid','')::bigint,
      nullif(a->>'id_arbre','')::bigint,
      nullif(btrim(a->>'espece'),''),
      nullif(a->>'diametre_tronc','')::integer,
      nullif(btrim(a->>'interet_1'),''),
      nullif(btrim(a->>'interet_2'),''),
      nullif(btrim(a->>'interet_3'),''),
      nullif(btrim(a->>'etat'),''),
      nullif(btrim(a->>'remarque'),''),
      CASE WHEN g IS NULL OR g = 'null'::jsonb THEN NULL
           ELSE ST_SetSRID(ST_GeomFromGeoJSON(g::text), 2056) END,
      now(),
      NULL
    FROM dedup
    ON CONFLICT (content_key) DO UPDATE SET
      last_run_id    = EXCLUDED.last_run_id,
      globalid       = EXCLUDED.globalid,
      objectid       = EXCLUDED.objectid,
      id_arbre       = EXCLUDED.id_arbre,
      espece         = EXCLUDED.espece,
      diametre_tronc = EXCLUDED.diametre_tronc,
      interet_1      = EXCLUDED.interet_1,
      interet_2      = EXCLUDED.interet_2,
      interet_3      = EXCLUDED.interet_3,
      etat           = EXCLUDED.etat,
      remarque       = EXCLUDED.remarque,
      geom           = EXCLUDED.geom,
      updated_at     = now(),
      deleted_at     = NULL
    RETURNING 1)
  SELECT count(*) INTO v_upserted FROM ins;

  RETURN jsonb_build_object(
    'received',  v_in,
    'upserted',  v_upserted,
    'collapsed', v_in - v_upserted);
END;
$$;

REVOKE ALL ON FUNCTION public.upsert_ge_ffp_arbres_remarquables_batch(jsonb, uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.upsert_ge_ffp_arbres_remarquables_batch(jsonb, uuid) TO service_role;


-- ---------------------------------------------------------------------------
-- Soft delete: rows not stamped by the run that just completed.
--
-- Driven by last_run_id, not by a list of keys. The previous version shipped
-- every identifier seen in the run as one JSON array -- 239167 elements in a
-- single request for the isole layer -- which was both fragile and unnecessary
-- once each row records the run that last touched it. Same pattern as
-- pipelines/sitg_forest.
--
-- Never a hard DELETE. Rows that reappear at source un-delete on the next run
-- because the upsert resets deleted_at.
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.mark_ge_trees_absent(text, jsonb);

CREATE OR REPLACE FUNCTION public.mark_ge_trees_absent(p_table text, p_run_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, bronze_ch, pg_catalog
AS $$
DECLARE
  v_marked int;
BEGIN
  IF p_table NOT IN ('ge_sipv_arbre_isole','ge_ffp_arbres_remarquables') THEN
    RAISE EXCEPTION 'mark_ge_trees_absent: unexpected table %', p_table;
  END IF;
  IF p_run_id IS NULL THEN
    RAISE EXCEPTION 'mark_ge_trees_absent: p_run_id is required; a NULL run id '
                    'would soft-delete the entire table';
  END IF;

  EXECUTE format(
    'UPDATE bronze_ch.%I SET deleted_at = now()
      WHERE deleted_at IS NULL AND last_run_id IS DISTINCT FROM $1', p_table)
  USING p_run_id;
  GET DIAGNOSTICS v_marked = ROW_COUNT;

  RETURN jsonb_build_object('soft_deleted', v_marked);
END;
$$;

REVOKE ALL ON FUNCTION public.mark_ge_trees_absent(text, uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.mark_ge_trees_absent(text, uuid) TO service_role;

-- PostgREST caches function signatures. This file changes them (p_run_id was
-- added), so the cache must be told or every call 404s with PGRST202.
NOTIFY pgrst, 'reload schema';
