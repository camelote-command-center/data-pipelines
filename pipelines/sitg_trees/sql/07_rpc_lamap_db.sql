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
-- drawer shows them as two sections; they are never merged.
--
-- ── felling_status IS THE CONTRACT, NOT THE BOOLEAN ──────────────────────
-- The drawer renders three states and they must stay distinguishable:
--     'requires_authorisation'  at/over 45 cm circumference (RCVA art. 3 al. 2)
--     'free_to_fell'            under 45 cm
--     'unknown'                 never measured -> the UI shows "à vérifier"
--
-- This is returned as TEXT, deliberately, and the underlying boolean is NOT
-- exposed. A nullable boolean in JSON is a trap: `if (t.requires_authorisation)`
-- is falsy for BOTH false and null, so the single most dangerous row in the
-- dataset -- an unmeasured tree -- would render identically to one that is
-- genuinely free to fell. 103'956 of the 239'450 rows the drawer can show are
-- unmeasured, so that collapse would mis-state 43% of the inventory. A text
-- enum cannot be got wrong by accident.
--
-- felling_status is derived from the stored tri-state, NOT from circumference
-- alone. 200 rows carry no circumference but do carry a measured trunk
-- diameter, from which the circumference follows as pi*d; 197 of those are over
-- the threshold. Reading circumference alone would demote 197 protected trees
-- to 'unknown'. felling_flag_basis travels with every row so the frontend can
-- caveat those 200 ('diameter_derived') rather than present them as directly
-- measured.
--
-- 'free_to_fell' MEANS "BELOW THE SIZE THRESHOLD", NOT "MAY BE FELLED".
-- Art. 3 al. 2 keeps authorisation mandatory regardless of size for trees under
-- departmental directive, PLQ-protected vegetation, compensation plantings and
-- art. 18A-funded plantings. None of those appear in this data, so no column
-- here can see them.
--
-- ── NO OWNERSHIP MEANING ─────────────────────────────────────────────────
-- egrid is "the parcel this tree stands on", nothing more. The GE cadastral
-- layer covers public land as well as private, so 239'758 of 239'761 trees
-- match some parcel and a street tree normally matches a domaine-public one.
-- A match is NOT evidence the tree is on private property. No field here is
-- named or commented as though it were.
--
-- ── NO FELLING-AUTHORISATION DOSSIER FIELDS ──────────────────────────────
-- SIPV_ICA_ABATTAGE_SEV_PTS is licensed "usage privé / A* non commercial" and
-- "uniquement pour l'affichage dans la carte interactive dédiée". Both clauses
-- exclude a commercial product re-displaying it. Do not add without a written
-- licence variation from the Ville de Genève.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.get_plot_trees_cadastre(p_egrid text)
RETURNS json
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
  WITH t AS (
    SELECT
      r.*,
      CASE
        WHEN r.requires_felling_authorisation IS TRUE  THEN 'requires_authorisation'
        WHEN r.requires_felling_authorisation IS FALSE THEN 'free_to_fell'
        ELSE 'unknown'
      END AS felling_status
    FROM ref.trees_cadastre r
    WHERE r.egrid = p_egrid
      AND r.duplicate_of_tree_id IS NULL   -- same tree present in both source layers
  )
  SELECT json_build_object(
    'egrid', p_egrid,
    'summary', json_build_object(
      'total_catalogued',             (SELECT count(*) FROM t),
      'count_remarquable',            (SELECT count(*) FROM t WHERE is_remarquable),
      'count_requires_authorisation', (SELECT count(*) FROM t WHERE felling_status = 'requires_authorisation'),
      -- Reported separately from count_free_to_fell on purpose: these trees have
      -- no measurement, so their status is unknown, not permissive.
      'count_unknown',                (SELECT count(*) FROM t WHERE felling_status = 'unknown'),
      'count_free_to_fell',           (SELECT count(*) FROM t WHERE felling_status = 'free_to_fell'),
      -- Positions the UI should caveat: 25 m accuracy, or undocumented.
      'count_low_precision_position', (SELECT count(*) FROM t
                                       WHERE position_precision_m IS NULL OR position_precision_m >= 25)
    ),
    'trees', COALESCE((
      SELECT json_agg(x ORDER BY
               -- remarkable first, then requires_authorisation, then unknown,
               -- then free_to_fell: the drawer leads with what constrains a build.
               x.is_remarquable DESC NULLS LAST,
               CASE x.felling_status
                 WHEN 'requires_authorisation' THEN 0
                 WHEN 'unknown'                THEN 1
                 ELSE 2
               END,
               x.circumference_cm DESC NULLS LAST)
      FROM (
        SELECT
          t.tree_id,
          t.source,
          ST_X(t.geometry)          AS lon,
          ST_Y(t.geometry)          AS lat,
          t.species,
          t.circumference_cm,        -- raw, NULL preserved: NULL = never measured
          t.trunk_diameter_cm,
          t.crown_diameter_m,
          t.height_total_m,
          t.felling_status,          -- 'requires_authorisation'|'free_to_fell'|'unknown'
          t.felling_flag_basis,      -- 'circumference_measured'|'diameter_derived'|null
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
  'Returns {egrid, summary, trees} in ONE call; the drawer headline needs no '
  'second round-trip and no client-side iteration. '
  'felling_status is a THREE-VALUE TEXT enum -- requires_authorisation | '
  'free_to_fell | unknown -- per RCVA (rsGE L 4 05.04) art. 3 al. 2, whose only '
  'size threshold is 45 cm circumference at 1 m. The nullable boolean is '
  'deliberately not exposed: it is falsy for both "below threshold" and "never '
  'measured", which would make 103956 unmeasured trees look free to fell. '
  'free_to_fell means "below the size threshold", NOT "may be felled": '
  'authorisation stays mandatory regardless of size for trees under '
  'departmental directive, PLQ-protected vegetation, compensation plantings and '
  'art. 18A-funded plantings, none of which appear in this data. is_remarquable '
  'is carried from source, never derived from size. egrid carries NO ownership '
  'meaning: the GE cadastral layer covers public land, so a match does not '
  'imply private property. Carries no felling-authorisation dossier fields: '
  'that SITG layer is licensed non-commercial and dedicated-map-only.';

REVOKE ALL ON FUNCTION public.get_plot_trees_cadastre(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_plot_trees_cadastre(text) TO anon, authenticated;

-- PostgREST schema cache must be told, or the RPC 404s from the client.
NOTIFY pgrst, 'reload schema';
