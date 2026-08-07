-- ═══════════════════════════════════════════════════════════════════════════
-- public.get_plot_trees_cadastre — plot drawer RPC
--
-- APPLY ON lamap_db.
--
-- PURE-REF: reads ref.trees_cadastre and nothing else. No bronze, silver or
-- gold reference anywhere in the body.
--
-- SEPARATE FROM get_plot_trees, which stays untouched. That one serves the
-- LiDAR canopy layer (ref.plot_trees, 1.59M detections, estimated crown radius,
-- no species, no legal meaning). This one serves the official register. The
-- drawer shows them as two sections; they are cross-referenced by proximity in
-- the UI and never merged.
--
-- NO FELLING-AUTHORISATION FIELDS. The brief specified felling_request_no,
-- felling_reason, felling_pdf_url and a per-parcel "has a felling authorisation"
-- flag, all sourced from SIPV_ICA_ABATTAGE_SEV_PTS. That layer was not ingested:
-- its licence is "usage privé / A* non commercial" AND "uniquement pour
-- l'affichage dans la carte interactive dédiée", and both clauses independently
-- exclude a commercial product re-displaying it. Do not add these fields
-- without a written licence variation from the Ville de Genève.
--
-- ORDERING. The brief asked for "arbres majeurs first". There is no legal
-- category "arbre majeur" (see the column comments on ref.trees_cadastre), so
-- the drawer leads with what is legally meaningful instead: remarkable trees,
-- then trees over the RCVA authorisation threshold, then largest first.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.get_plot_trees_cadastre(p_egrid text)
RETURNS json
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
  WITH t AS (
    SELECT * FROM ref.trees_cadastre
    WHERE egrid = p_egrid
      AND duplicate_of_tree_id IS NULL   -- same tree in both source layers
  )
  SELECT json_build_object(
    'egrid', p_egrid,
    'summary', json_build_object(
      'total_catalogued',        (SELECT count(*) FROM t),
      'remarquable',             (SELECT count(*) FROM t WHERE is_remarquable),
      'requires_authorisation',  (SELECT count(*) FROM t WHERE requires_felling_authorisation IS TRUE),
      'below_threshold',         (SELECT count(*) FROM t WHERE requires_felling_authorisation IS FALSE),
      -- Surfaced, never folded into "below threshold": these trees have no
      -- circumference measurement at all, so their status is unknown, not free.
      'authorisation_unknown',   (SELECT count(*) FROM t WHERE requires_felling_authorisation IS NULL),
      -- Positions the UI should caveat: 25 m accuracy, or undocumented.
      'low_precision_positions', (SELECT count(*) FROM t
                                  WHERE position_precision_m IS NULL OR position_precision_m >= 25)
    ),
    'trees', COALESCE((
      SELECT json_agg(x ORDER BY
               x.is_remarquable DESC NULLS LAST,
               (x.requires_felling_authorisation IS TRUE) DESC,
               x.circumference_cm DESC NULLS LAST)
      FROM (
        SELECT
          t.tree_id,
          t.source,
          ST_X(t.geometry)                     AS lon,
          ST_Y(t.geometry)                     AS lat,
          t.species,
          t.circumference_cm,
          t.trunk_diameter_cm,
          t.crown_diameter_m,
          t.height_total_m,
          t.requires_felling_authorisation,
          t.felling_flag_basis,
          t.is_remarquable,
          t.remarquable_status,
          t.remarquable_reasons,
          t.position_status,
          t.position_precision_m,
          t.vitality,
          t.development_stage,
          t.observed_at
        FROM t
      ) x
    ), '[]'::json)
  );
$function$;

COMMENT ON FUNCTION public.get_plot_trees_cadastre(text) IS
  'Official SITG tree register for one parcel: inventaire cantonal des arbres '
  'isolés + recensement des arbres remarquables. Separate from get_plot_trees, '
  'which serves the LiDAR canopy layer -- the two populations are never merged. '
  'requires_felling_authorisation is TRI-STATE per RCVA (rsGE L 4 05.04) art. 3 '
  'al. 2: true at/over 45 cm circumference at 1 m, false under it, NULL where '
  'the tree was never measured (42% of the inventory). The summary reports '
  'authorisation_unknown separately from below_threshold for exactly that '
  'reason. A false is NOT permission to fell: authorisation stays mandatory '
  'regardless of size for trees under departmental directive, PLQ-protected '
  'vegetation, compensation plantings and art. 18A-funded plantings, none of '
  'which appear in this data. Carries no felling-authorisation dossier fields: '
  'that SITG layer is licensed non-commercial and dedicated-map-only.';

REVOKE ALL ON FUNCTION public.get_plot_trees_cadastre(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_plot_trees_cadastre(text) TO anon, authenticated;
