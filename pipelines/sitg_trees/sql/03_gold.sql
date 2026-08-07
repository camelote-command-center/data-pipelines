-- ═══════════════════════════════════════════════════════════════════════════
-- SITG tree cadastre — gold_ch.trees_cadastre on re-LLM
--
-- Union of the two ingested layers, one row per source record, tagged by
-- `source`. This is the OFFICIAL INVENTORY. It is deliberately NOT merged with
-- ref.plot_trees (1.59M LiDAR canopy detections): that is a geometric detection
-- with an ESTIMATED crown radius and no species or legal status, this is a
-- surveyed register with MEASURED circumference, species and protection status.
-- Folding either into the other would destroy the distinction that makes this
-- one legally meaningful.
--
-- ── THE FELLING FLAG ─────────────────────────────────────────────────────
-- There is NO legal category "arbre majeur" in Geneva law, and no 200 cm
-- threshold. Verified against the current consolidated text 2026-08-07.
--
-- The governing instrument is the RCVA -- Règlement sur la conservation de la
-- végétation arborée, rsGE L 4 05.04 (NOT the RPMNS; the RCVA is issued under
-- art. 36 LPMNS of 4 June 1976). Its only size threshold is art. 3 al. 2:
--
--   « N'est pas soumis à autorisation l'abattage, par leur propriétaire, des
--     arbres de moins de 45 cm de circonférence, mesurés à 1 m de hauteur du
--     tronc. »
--
-- So the threshold is 45 cm CIRCUMFERENCE at 1 m, and it is an EXEMPTION:
-- under it the owner may fell freely, at or over it authorisation is required.
--
-- requires_felling_authorisation is therefore TRI-STATE and must stay so:
--   true  -> at or over 45 cm: authorisation required
--   false -> under 45 cm: owner may fell, SUBJECT TO the carve-outs below
--   NULL  -> not measured. 101'462 of 239'167 ICA rows (42%) carry no
--            circumference and a further 2'411 carry a literal 0. Both mean
--            "unmeasured", never "small". Collapsing NULL to false would tell a
--            promoteur he may fell a tree nobody has measured.
--
-- FOUR CARVE-OUTS THIS COLUMN CANNOT SEE. Art. 3 al. 2 keeps authorisation
-- mandatory regardless of size for: (a) trees designated by departmental
-- directive; (b) vegetation marked « à sauvegarder et à créer » in a PLQ
-- (art. 8); (c) compensation vegetation (art. 17); (d) plantings financed by
-- the art. 18A fund. None of these appear in the SITG layers. A `false` here
-- means "below the size threshold", never "you may fell this tree".
--
-- ── REMARKABILITY IS CARRIED, NEVER DERIVED ──────────────────────────────
-- Remarkability is a scored designation: OCAN with the CJB and HEPIA weight six
-- criteria (size, dendrological rarity, presence in the 1976 inventory, form,
-- historical interest, landscape situation) and a tree needs >= 12 of 20 points.
-- It is not a size threshold and cannot be computed from circumference.
--
-- Presence in FFP_ARBRES_REMARQUABLES does NOT mean remarkable: that layer is
-- the assessment register and holds rejected and pending candidates too
-- (208 Approuvé remarquable, 221 En cours de vérification, 165 Non remarquable).
-- Only `etat = 'Approuvé remarquable'` is a legally remarkable tree, and 208
-- matches the ~206 OCAN publishes.
-- ═══════════════════════════════════════════════════════════════════════════

-- ── WHY tree_id IS globalid AND NOT ID_ARBRE ─────────────────────────────
-- ID_ARBRE is the obvious key and is not unique. Measured on the live layer
-- 2026-08-07: 239'167 rows carry 239'166 distinct id_arbre. id_arbre 393253 is
-- held by two genuinely different trees -- Quercus castaneifolia at
-- (2498451.57, 1119665.34) and Zelkova sicula at (2500294.00, 1120297.18),
-- 2 km apart, one with no NO_INVENTAIRE. A primary key on it would have failed
-- the load, or worse, silently dropped one of the two. On the remarquables
-- layer id_arbre is worse still: 0 on 262 of 594 rows.
--
-- tree_id is therefore derived from globalid, an ArcGIS-guaranteed unique GUID.
-- The raw ID_ARBRE is kept in source_tree_id so the SITG-side identifier is not
-- lost and the two layers can still be cross-referenced on it.

CREATE SCHEMA IF NOT EXISTS gold_ch;

CREATE TABLE IF NOT EXISTS gold_ch.trees_cadastre (
  tree_id                        text PRIMARY KEY,
  source_tree_id                 bigint,
  source                         text NOT NULL
                                 CHECK (source IN ('ica_isole','remarquable')),
  geom                           geometry(Point, 2056),
  species                        text,
  circumference_cm               numeric,
  trunk_diameter_cm              numeric,
  crown_diameter_m               numeric,
  crown_radius_m                 numeric,
  height_total_m                 numeric,
  requires_felling_authorisation boolean,
  felling_flag_basis             text
                                 CHECK (felling_flag_basis IN
                                   ('circumference_measured','diameter_derived')),
  is_remarquable                 boolean NOT NULL DEFAULT false,
  remarquable_status             text,
  remarquable_reasons            text,
  position_status                text,
  position_precision_m           numeric,
  is_stump                       boolean,
  vitality                       text,
  development_stage              text,
  tree_class                     text,
  duplicate_of_tree_id           text,
  egrid                          text,
  commune_bfs                    integer,
  observed_at                    timestamptz,
  ingested_at                    timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN gold_ch.trees_cadastre.requires_felling_authorisation IS
  'RCVA (rsGE L 4 05.04) art. 3 al. 2: felling needs departmental authorisation '
  'unless the tree is under 45 cm circumference measured at 1 m. TRI-STATE: '
  'true = at/over 45 cm; false = under it; NULL = never measured (42% of the ICA '
  'layer). NULL must never be read as false. A false does NOT mean the tree may '
  'be felled: art. 3 al. 2 keeps authorisation mandatory regardless of size for '
  'trees under departmental directive, PLQ-protected vegetation, compensation '
  'plantings and art. 18A-funded plantings, none of which are in this data.';

COMMENT ON COLUMN gold_ch.trees_cadastre.felling_flag_basis IS
  'How requires_felling_authorisation was decided. circumference_measured = '
  'from the measured circonference_1m, the value the regulation actually names. '
  'diameter_derived = the source published only a trunk diameter and the '
  'circumference was taken as pi*d, which assumes a circular trunk. NULL '
  'wherever the flag is NULL.';

COMMENT ON COLUMN gold_ch.trees_cadastre.is_remarquable IS
  'Legally remarkable. Carried from source, never derived from size. For the '
  'FFP layer this is etat = ''Approuvé remarquable'' ONLY -- the layer also '
  'holds 221 pending and 165 rejected candidates. For the ICA layer it is the '
  'presence of the REMARQUABLE free-text reason (206 rows).';

COMMENT ON COLUMN gold_ch.trees_cadastre.position_precision_m IS
  'Documented positional accuracy: 25 m for STATUT Historique (1976 paper '
  'survey), 1 m for Relevé (recent field survey). NULL for Positionné (6165 '
  'rows) and Nommé (2320), for which SITG documents no accuracy -- they are '
  'left NULL rather than assigned an invented figure. Matters at parcel '
  'boundaries: a 25 m point can sit on the wrong parcel.';

COMMENT ON COLUMN gold_ch.trees_cadastre.duplicate_of_tree_id IS
  'Set on an FFP remarquables row whose non-zero ID_ARBRE matches an ICA row, '
  'i.e. the same physical tree appearing in both layers. Kept rather than '
  'deleted so both sources stay inspectable, but excluded from headline counts '
  'so a tree is not counted twice.';

COMMENT ON COLUMN gold_ch.trees_cadastre.egrid IS
  'Parcel the tree falls in, by ST_Contains. The GE cadastral layer covers '
  'public land as well as private, so a street tree normally matches a '
  'domaine-public parcel rather than returning NULL: 239758 of 239761 trees '
  'match, and a match therefore does NOT imply private property. NULL means the '
  'point fell outside every GE parcel polygon (3 rows). 1191 trees sit on '
  'parcels that themselves carry no no_commune_no_parcelle, the population '
  'tracked in bug ab70452b. Joined against silver_ch.cadastral_plots, NOT '
  'bronze_ch.ge_plots_geo: the latter holds 72949 rows against 73000 GE plots '
  'and would silently drop trees on the 51 missing parcels.';

CREATE INDEX IF NOT EXISTS trees_cadastre_geom_gix ON gold_ch.trees_cadastre USING GIST (geom);
CREATE INDEX IF NOT EXISTS trees_cadastre_egrid_ix ON gold_ch.trees_cadastre (egrid);
CREATE INDEX IF NOT EXISTS trees_cadastre_source_ix ON gold_ch.trees_cadastre (source);


-- ---------------------------------------------------------------------------
-- Rebuild
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE gold_ch.refresh_trees_cadastre()
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = gold_ch, silver_ch, bronze_ch, public, pg_catalog
SET statement_timeout = '3600s'
AS $procedure$
DECLARE
  v_rows        int;
  v_ica         int;
  v_rmq         int;
  v_flag_null   int;
BEGIN
  -- GE plots in LV95, one row per plot. Subdivided for a selective GIST index:
  -- 73k parcels against 240k points is a lot of ST_Contains, and whole-parcel
  -- polygons make the planner fall back to near-scans.
  DROP TABLE IF EXISTS _plots;
  CREATE TEMP TABLE _plots ON COMMIT DROP AS
    SELECT egrid, commune_bfs, ST_Transform(geometry, 2056) AS g
    FROM silver_ch.cadastral_plots
    WHERE canton_code = 'GE' AND geometry IS NOT NULL;
  CREATE INDEX ON _plots USING GIST (g);
  ANALYZE _plots;

  DROP TABLE IF EXISTS _u;
  CREATE TEMP TABLE _u ON COMMIT DROP AS
  -- ── ICA: the inventory proper ────────────────────────────────────────
  SELECT
    'ica:' || i.globalid                                   AS tree_id,
    i.id_arbre                                             AS source_tree_id,
    'ica_isole'                                            AS source,
    i.geom,
    i.nom_complet                                          AS species,
    nullif(i.circonference_1m, 0)                          AS circumference_cm,
    nullif(i.diametre_1m, 0)                               AS trunk_diameter_cm,
    i.diametre_couronne                                    AS crown_diameter_m,
    i.rayon_couronne                                       AS crown_radius_m,
    i.hauteur_totale                                       AS height_total_m,
    CASE WHEN nullif(i.circonference_1m, 0) IS NULL THEN NULL
         ELSE i.circonference_1m >= 45 END                 AS requires_felling_authorisation,
    CASE WHEN nullif(i.circonference_1m, 0) IS NULL THEN NULL
         ELSE 'circumference_measured' END                 AS felling_flag_basis,
    (i.remarquable IS NOT NULL)                            AS is_remarquable,
    NULL::text                                             AS remarquable_status,
    i.remarquable                                          AS remarquable_reasons,
    i.statut                                               AS position_status,
    CASE i.statut WHEN 'Historique' THEN 25 WHEN 'Relevé' THEN 1 END::numeric
                                                           AS position_precision_m,
    (i.souche = 'Oui')                                     AS is_stump,
    i.vitalite                                             AS vitality,
    i.stade_developpement                                  AS development_stage,
    i.classe                                               AS tree_class,
    NULL::text                                             AS duplicate_of_tree_id,
    i.date_observation                                     AS observed_at
  FROM bronze_ch.ge_sipv_arbre_isole i
  WHERE i.deleted_at IS NULL AND i.id_arbre IS NOT NULL

  UNION ALL

  -- ── FFP: the remarkability register ──────────────────────────────────
  SELECT
    'rmq:' || r.globalid,
    nullif(r.id_arbre, 0),
    'remarquable',
    r.geom,
    r.espece,
    NULL::numeric,                                         -- publishes diameter, not circumference
    nullif(r.diametre_tronc, 0),
    NULL::numeric, NULL::numeric, NULL::numeric,
    CASE WHEN nullif(r.diametre_tronc, 0) IS NULL THEN NULL
         ELSE (r.diametre_tronc * pi()) >= 45 END,
    CASE WHEN nullif(r.diametre_tronc, 0) IS NULL THEN NULL
         ELSE 'diameter_derived' END,
    (r.etat = 'Approuvé remarquable'),
    r.etat,
    nullif(concat_ws(', ', r.interet_1, r.interet_2, r.interet_3), ''),
    NULL::text,
    10::numeric,                                           -- SITG states 10 m for this layer
    NULL::boolean,
    NULL::text, NULL::text, NULL::text,
    (SELECT 'ica:' || x.globalid
       FROM bronze_ch.ge_sipv_arbre_isole x
      WHERE r.id_arbre IS NOT NULL AND r.id_arbre <> 0
        AND x.id_arbre = r.id_arbre AND x.deleted_at IS NULL
      ORDER BY x.globalid
      LIMIT 1),
    NULL::timestamptz
  FROM bronze_ch.ge_ffp_arbres_remarquables r
  WHERE r.deleted_at IS NULL;

  CREATE INDEX ON _u USING GIST (geom);
  ANALYZE _u;

  TRUNCATE gold_ch.trees_cadastre;

  INSERT INTO gold_ch.trees_cadastre (
    tree_id, source_tree_id, source, geom, species, circumference_cm, trunk_diameter_cm,
    crown_diameter_m, crown_radius_m, height_total_m,
    requires_felling_authorisation, felling_flag_basis, is_remarquable,
    remarquable_status, remarquable_reasons, position_status,
    position_precision_m, is_stump, vitality, development_stage, tree_class,
    duplicate_of_tree_id, egrid, commune_bfs, observed_at, ingested_at)
  SELECT
    u.tree_id, u.source_tree_id, u.source, u.geom, u.species, u.circumference_cm, u.trunk_diameter_cm,
    u.crown_diameter_m, u.crown_radius_m, u.height_total_m,
    u.requires_felling_authorisation, u.felling_flag_basis, u.is_remarquable,
    u.remarquable_status, u.remarquable_reasons, u.position_status,
    u.position_precision_m, u.is_stump, u.vitality, u.development_stage, u.tree_class,
    u.duplicate_of_tree_id, p.egrid, p.commune_bfs, u.observed_at, now()
  FROM _u u
  LEFT JOIN LATERAL (
    SELECT pl.egrid, pl.commune_bfs
    FROM _plots pl
    WHERE u.geom IS NOT NULL AND ST_Contains(pl.g, u.geom)
    LIMIT 1
  ) p ON true;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  SELECT count(*) FILTER (WHERE source = 'ica_isole'),
         count(*) FILTER (WHERE source = 'remarquable'),
         count(*) FILTER (WHERE requires_felling_authorisation IS NULL)
    INTO v_ica, v_rmq, v_flag_null
  FROM gold_ch.trees_cadastre;

  RAISE NOTICE 'trees_cadastre rebuilt: % rows (ica %, remarquable %), felling flag unknown on %',
    v_rows, v_ica, v_rmq, v_flag_null;

  -- The four-valued position_status must not collapse, same discipline as
  -- forest_constraint_source: it is what lets the frontend caveat old points.
  IF (SELECT count(DISTINCT position_status) FROM gold_ch.trees_cadastre
      WHERE source = 'ica_isole' AND position_status IS NOT NULL) < 4 THEN
    RAISE EXCEPTION 'position_status collapsed to % distinct values, expected 4',
      (SELECT count(DISTINCT position_status) FROM gold_ch.trees_cadastre
       WHERE source = 'ica_isole' AND position_status IS NOT NULL);
  END IF;
END;
$procedure$;
