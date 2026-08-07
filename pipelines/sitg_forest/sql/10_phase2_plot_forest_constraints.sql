-- ============================================================================
-- Phase 2 — per-plot forest constraints, computed on re-LLM
-- ============================================================================
-- One row per GE plot in gold_ch.plot_forest_constraints, distributed to
-- lamap_db as ref.plot_forest_constraints. Additive: nothing here touches
-- rdppf_forets on plots_registry / mv_plots, or rdppf_forest_distance on
-- gold_ch.core_plots_ext_ge.
--
-- WHERE THIS RUNS. Computed on re-LLM against silver_ch.cadastral_plots, not on
-- lamap_db against mv_plots.geometry_lv95. Verified equivalent before building:
-- both hold exactly 73'000 GE plots totalling 282'485'394.9 m2, and the 4326 ->
-- 2056 round trip against lamap_db's native geometry_lv95 has a Hausdorff
-- distance of 0.000000 m over a 3'000-plot sample. Keeping the computation next
-- to the forest layers keeps the medallion intact and ref.* a pure
-- distribution target.
--
-- EVERYTHING IS METRIC, IN EPSG:2056. No distance or area is ever computed in
-- 4326.
--
-- AREA IS MEASURED AGAINST A UNION, NEVER A SUM OF PER-POLYGON INTERSECTIONS.
-- FFP_CADASTRE_FORET self-overlaps on 9 polygon pairs totalling 0.85 ha, and a
-- naive sum double-counts them. That defect manufactured 4 of the 5 apparent
-- >5pp disagreements with ge_rdppf_synthese during Phase 1 verification. See
-- platform.standards / forest_area_must_use_union_not_sum_of_intersections.
--
-- DENOMINATOR IS THE GEOMETRIC AREA, not mv_plots.surface_m2. surface_m2 is the
-- registered cadastral surface rounded to a whole m2; on a 4 m2 plot that
-- rounding is 9 percent and yields forest percentages above 100. Both are
-- stored so the difference stays auditable.
--
-- NO TILE HARNESS. A full 73'000-plot pass with ST_Subdivide(geom, 256) plus
-- ST_DWithin returns inline. tile_build_queue / build_next_tile_chunk are
-- deliberately not used here.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Target table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_ch.plot_forest_constraints (
  egrid                           text PRIMARY KEY,
  no_commune_no_parcelle          text,
  plot_area_m2                    double precision,
  surface_m2                      numeric,
  forest_area_m2                  double precision,
  forest_pct                      numeric(7,1),
  dist_to_forest_edge_m           numeric(8,2),
  within_20m_of_forest            boolean,
  rdppf_distance_registered_m2    double precision,
  computed_20m_buffer_m2          double precision,
  buildable_area_after_forest_m2  double precision,
  forest_constraint_source        text,
  lisiere_procedure_open          boolean,
  lisiere_last_fao_date           date,
  computed_at                     timestamptz
);

COMMENT ON COLUMN gold_ch.plot_forest_constraints.forest_constraint_source IS
  'FOUR-VALUED AND NEVER COLLAPSED. rdppf_registered = a restriction is entered in the RDPPF and is legally defensible in front of the DT. computed_from_cadastre = our own inference from the factual forest cadastre; it is an ALERT, not a legal statement. both = each independently applies. none = neither. The frontend must word rdppf_registered and computed_from_cadastre differently. Do not pre-merge these into a single is_constrained boolean at the data layer.';

COMMENT ON COLUMN gold_ch.plot_forest_constraints.dist_to_forest_edge_m IS
  'Metres from the plot to the nearest forest, measured against the union of the whole cadastre, so forest on a NEIGHBOURING parcel counts. 0 when the plot itself contains forest. Capped at 200 m; NULL beyond.';

COMMENT ON COLUMN gold_ch.plot_forest_constraints.forest_pct IS
  'forest_area_m2 as a percentage of plot_area_m2 (the GEOMETRIC area), not of the registered surface_m2. Correlates with ge_rdppf_synthese.idge_1079_cadastre_foret at Pearson r = 0.999995 over 7208 plots, which is what lets us stop deriving a sold field from the non-commercial A* layer.';

CREATE INDEX IF NOT EXISTS plot_forest_constraints_within20_idx
  ON gold_ch.plot_forest_constraints (within_20m_of_forest)
  WHERE within_20m_of_forest;
CREATE INDEX IF NOT EXISTS plot_forest_constraints_source_idx
  ON gold_ch.plot_forest_constraints (forest_constraint_source);
CREATE INDEX IF NOT EXISTS plot_forest_constraints_parcel_idx
  ON gold_ch.plot_forest_constraints (no_commune_no_parcelle);

-- ---------------------------------------------------------------------------
-- Refresh procedure
-- ---------------------------------------------------------------------------
-- A PROCEDURE, not a function: it needs its own statement_timeout in the
-- definition clause (proconfig). Setting it with SET LOCAL inside the body
-- would be overridden, because pg_cron sessions inherit the database-level
-- timeout.
CREATE OR REPLACE PROCEDURE gold_ch.refresh_plot_forest_constraints()
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'gold_ch', 'silver_ch', 'bronze_ch', 'public', 'pg_catalog'
SET statement_timeout TO '3600s'
AS $$
DECLARE
  v_rows int;
BEGIN
  -- ── Forest geometry, unioned then subdivided ──────────────────────
  -- ST_Subdivide keeps each piece around 256 vertices so the GIST index is
  -- selective; without it a handful of huge multipolygons force the planner
  -- into near-full scans on every ST_DWithin.
  DROP TABLE IF EXISTS _fu;
  CREATE TEMP TABLE _fu ON COMMIT DROP AS
    SELECT ST_Subdivide(ST_Union(geom_2056), 256) AS g
    FROM silver_ch.cadastral_forest_cadastre;
  CREATE INDEX ON _fu USING GIST (g);
  ANALYZE _fu;

  -- The computed 20 m restriction: buffer FIRST, then union, so that the
  -- buffers of adjacent forest polygons merge instead of overlapping.
  DROP TABLE IF EXISTS _fb;
  CREATE TEMP TABLE _fb ON COMMIT DROP AS
    SELECT ST_Subdivide(ST_Union(ST_Buffer(geom_2056, 20)), 256) AS g
    FROM silver_ch.cadastral_forest_cadastre;
  CREATE INDEX ON _fb USING GIST (g);
  ANALYZE _fb;

  -- The legally registered RDPPF distance surface.
  DROP TABLE IF EXISTS _rd;
  CREATE TEMP TABLE _rd ON COMMIT DROP AS
    SELECT ST_Subdivide(ST_Union(geom_2056), 256) AS g
    FROM silver_ch.cadastral_forest_distance_s;
  CREATE INDEX ON _rd USING GIST (g);
  ANALYZE _rd;

  -- ── GE plots in LV95 ──────────────────────────────────────────────
  -- Identity is resolved in three tiers because silver's no_commune_no_parcelle
  -- is sparse for GE: 211 of 73'000 plots carried NULL. silver_ch.cadastral_plots
  -- builds that column in a CTE driven by bronze_ch.ge_plots_geo LEFT JOINed to
  -- ge_cad_parcelles, so a plot loses its identity two distinct ways:
  --   * absent from the driving table ge_plots_geo, so the CTE emits no row at
  --     all, even though ge_cad_parcelles does hold its identity  -> 106 plots
  --   * present in ge_plots_geo but unmatched in ge_cad_parcelles -> 105 plots
  -- Tier 2 addresses the first, tier 3 the second.
  --
  -- Tiers 2 and 3 are strictly additive: COALESCE only fills a NULL and never
  -- overwrites what silver produced. Checked before landing this that tier 2
  -- and tier 1 never disagree on the 72'789 rows where both are present, so
  -- the fallback cannot silently rewrite an identity that already resolved.
  --
  -- This does NOT reach zero, by design. 19 plots remain NULL: all sit under
  -- BFS 6621 (Genève), which fans out to four cadastral sub-communes, and they
  -- are absent from ge_cad_parcelles so there is nothing to disambiguate
  -- against. Those 19 are the forest-touching subset of 211 GE plots that carry
  -- a NULL identity in public.mv_plots on lamap_db itself. Resolving them here
  -- was deliberately rejected: it would make this derived table more complete
  -- than the table it derives from, leaving two disagreeing sources for the
  -- same identity. That defect is tracked upstream as bug ab70452b, scoped to
  -- all 211. Do not "finish the job" here.
  DROP TABLE IF EXISTS _p;
  CREATE TEMP TABLE _p ON COMMIT DROP AS
  WITH roster_1to1 AS (
    -- Only communes whose federal BFS maps to exactly one cadastral commune.
    -- BFS 6621 (Genève) fans out to four cadastral sub-communes and the federal
    -- source does not say which one a plot sits in. Those plots keep a NULL
    -- rather than take a guess that would read as fact downstream.
    SELECT no_com_federal::int AS bfs, min(no_comm)::text AS no_comm
    FROM bronze_ch.ge_cad_communes
    WHERE no_com_federal IS NOT NULL AND no_comm IS NOT NULL
    GROUP BY 1
    HAVING count(DISTINCT no_comm) = 1
  ), resolved AS (
    SELECT s.egrid,
           s.surface_m2,
           s.geometry,
           COALESCE(s.no_commune_no_parcelle,
                    nullif(trim(c.no_comm), '') || '/' || nullif(trim(c.no_parcelle), ''),
                    r.no_comm || '/' || nullif(trim(f.parcel_number), ''))  AS no_commune_no_parcelle,
           COALESCE(s.no_commune,  nullif(trim(c.no_comm), ''),
                    r.no_comm)                                             AS no_commune,
           COALESCE(s.no_parcelle, nullif(trim(c.no_parcelle), ''),
                    nullif(trim(f.parcel_number), ''))                     AS no_parcelle
    FROM silver_ch.cadastral_plots s
    -- LATERAL ... LIMIT 1 rather than a plain join: ge_cad_parcelles holds
    -- 73'085 rows against 73'000 plots, so a plain join would multiply rows and
    -- break the one-row-per-plot contract the row-count assertion below relies on.
    LEFT JOIN LATERAL (
      SELECT p2.no_comm, p2.no_parcelle
      FROM bronze_ch.ge_cad_parcelles p2
      WHERE p2.egrid::text = s.egrid
      ORDER BY p2.no_comm, p2.no_parcelle
      LIMIT 1
    ) c ON true
    LEFT JOIN LATERAL (
      SELECT f2.commune_bfs, f2.parcel_number
      FROM bronze_ch.federal_cadastral_parcels f2
      WHERE f2.egrid = s.egrid
      ORDER BY f2.commune_bfs, f2.parcel_number
      LIMIT 1
    ) f ON true
    -- commune_bfs is already integer on this table, so no cast or digit guard.
    LEFT JOIN roster_1to1 r ON r.bfs = f.commune_bfs
    WHERE s.canton_code = 'GE'
  )
  SELECT egrid,
         no_commune_no_parcelle,
         -- The components feed the lisières join below, which casts them to
         -- int. Accept digit-only fallbacks only: a lettered parcel number
         -- arriving from the federal source would otherwise abort the entire
         -- refresh at index creation, three statements from here.
         CASE WHEN no_commune  ~ '^[0-9]+$' THEN no_commune  END AS no_commune,
         CASE WHEN no_parcelle ~ '^[0-9]+$' THEN no_parcelle END AS no_parcelle,
         surface_m2,
         ST_Transform(geometry, 2056) AS g
  FROM resolved;
  CREATE INDEX ON _p USING GIST (g);
  CREATE INDEX ON _p ((no_commune::int), (no_parcelle::int));
  ANALYZE _p;

  -- ── Per-plot forest measures ──────────────────────────────────────
  DROP TABLE IF EXISTS _m;
  CREATE TEMP TABLE _m ON COMMIT DROP AS
  SELECT
    p.egrid,
    -- Union-based, so self-overlapping forest polygons cannot double-count.
    COALESCE((SELECT ST_Area(ST_Intersection(p.g, ST_Union(u.g)))
                FROM _fu u WHERE ST_Intersects(p.g, u.g)), 0)        AS forest_area_m2,
    -- 200 m search cap. Beyond it the distance is stored as NULL rather than
    -- as a large number, so "far from forest" is never confused with "measured".
    (SELECT min(ST_Distance(p.g, u.g)) FROM _fu u
      WHERE ST_DWithin(p.g, u.g, 200))                               AS dist_raw_m,
    COALESCE((SELECT ST_Area(ST_Intersection(p.g, ST_Union(b.g)))
                FROM _fb b WHERE ST_Intersects(p.g, b.g)), 0)        AS buffer20_m2,
    COALESCE((SELECT ST_Area(ST_Intersection(p.g, ST_Union(d.g)))
                FROM _rd d WHERE ST_Intersects(p.g, d.g)), 0)        AS rdppf_m2,
    -- buildable = plot minus the UNION of the registered surface and the
    -- computed buffer. Union, not sum: the two overlap almost everywhere.
    COALESCE((SELECT ST_Area(ST_Intersection(p.g,
                ST_Union(ST_Union(b.g), COALESCE(
                  (SELECT ST_Union(d.g) FROM _rd d WHERE ST_Intersects(p.g, d.g)),
                  ST_GeomFromText('POLYGON EMPTY', 2056)))))
                FROM _fb b WHERE ST_Intersects(p.g, b.g)), 0)        AS constrained_union_m2,
    ST_Area(p.g)                                                     AS plot_area_m2
  FROM _p p;
  CREATE INDEX ON _m (egrid);
  ANALYZE _m;

  -- ── Lisiere linkage ───────────────────────────────────────────────
  -- The parcelles child carries a FEDERAL BFS commune number; plots carry the
  -- GE cadastral commune number (1-48). They are bridged through the roster.
  -- BFS 6621 (Ville de Geneve) covers four cadastral sub-communes, so a parcel
  -- under 6621 can match more than one plot. That ambiguity is inherent to the
  -- source and is why this drives a boolean flag, never a legal statement.
  DROP TABLE IF EXISTS _lis;
  CREATE TEMP TABLE _lis ON COMMIT DROP AS
  SELECT
    p.egrid,
    bool_or(NOT COALESCE(l.in_force, false))                          AS procedure_open,
    max(GREATEST(l.fao_requete_date, l.fao_decision_date))            AS last_fao_date
  FROM silver_ch.cadastral_forest_lisieres_parcelles pa
  JOIN silver_ch.cadastral_forest_lisieres l
    ON l.id_dossier_key = pa.id_dossier_key AND l.geom_hash = pa.geom_hash
  JOIN bronze_ch.ge_cad_communes c
    ON c.no_com_federal::int = pa.no_commune
  JOIN _p p
    ON p.no_commune::int = c.no_comm::int AND p.no_parcelle::int = pa.no_parcelle
  WHERE pa.parse_status = 'parsed' AND pa.no_parcelle IS NOT NULL
  GROUP BY p.egrid;
  CREATE INDEX ON _lis (egrid);
  ANALYZE _lis;

  -- ── Write ─────────────────────────────────────────────────────────
  TRUNCATE gold_ch.plot_forest_constraints;

  INSERT INTO gold_ch.plot_forest_constraints (
    egrid, no_commune_no_parcelle, plot_area_m2, surface_m2,
    forest_area_m2, forest_pct, dist_to_forest_edge_m, within_20m_of_forest,
    rdppf_distance_registered_m2, computed_20m_buffer_m2,
    buildable_area_after_forest_m2, forest_constraint_source,
    lisiere_procedure_open, lisiere_last_fao_date, computed_at)
  SELECT
    p.egrid,
    p.no_commune_no_parcelle,
    m.plot_area_m2,
    p.surface_m2,
    m.forest_area_m2,
    round((100.0 * m.forest_area_m2 / NULLIF(m.plot_area_m2, 0))::numeric, 1),
    CASE
      WHEN m.forest_area_m2 > 0 THEN 0::numeric        -- contains forest
      WHEN m.dist_raw_m IS NULL THEN NULL              -- beyond the 200 m cap
      ELSE round(m.dist_raw_m::numeric, 2)
    END,
    (m.forest_area_m2 > 0 OR (m.dist_raw_m IS NOT NULL AND m.dist_raw_m <= 20)),
    m.rdppf_m2,
    m.buffer20_m2,
    GREATEST(m.plot_area_m2 - m.constrained_union_m2, 0),
    CASE
      WHEN m.rdppf_m2 > 0 AND m.buffer20_m2 > 0 THEN 'both'
      WHEN m.rdppf_m2 > 0                       THEN 'rdppf_registered'
      WHEN m.buffer20_m2 > 0                    THEN 'computed_from_cadastre'
      ELSE 'none'
    END,
    COALESCE(l.procedure_open, false),
    l.last_fao_date,
    now()
  FROM _p p
  JOIN _m m USING (egrid)
  LEFT JOIN _lis l USING (egrid);

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RAISE NOTICE 'plot_forest_constraints rebuilt: % rows', v_rows;

  IF v_rows <> (SELECT count(*) FROM silver_ch.cadastral_plots WHERE canton_code = 'GE') THEN
    RAISE EXCEPTION 'plot_forest_constraints wrote % rows but there are % GE plots',
      v_rows, (SELECT count(*) FROM silver_ch.cadastral_plots WHERE canton_code = 'GE');
  END IF;
END;
$$;

REVOKE ALL ON PROCEDURE gold_ch.refresh_plot_forest_constraints() FROM PUBLIC, anon, authenticated;

-- ---------------------------------------------------------------------------
-- Gold view for distribution
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold_ch.v_plot_forest_constraints_full AS
SELECT
  egrid, no_commune_no_parcelle, plot_area_m2, surface_m2,
  forest_area_m2, forest_pct, dist_to_forest_edge_m, within_20m_of_forest,
  rdppf_distance_registered_m2, computed_20m_buffer_m2,
  buildable_area_after_forest_m2, forest_constraint_source,
  lisiere_procedure_open, lisiere_last_fao_date, computed_at
FROM gold_ch.plot_forest_constraints;
