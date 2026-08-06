-- ============================================================================
-- Phase 2 — ref.plot_forest_constraints on lamap_db
-- ============================================================================
-- Consumer-side target for gold_ch.plot_forest_constraints. No geometry: this
-- table is per-plot measures keyed on egrid, joined to mv_plots by the read
-- path. PURE-REF, additive, and it modifies nothing that already exists.
-- ============================================================================

CREATE TABLE IF NOT EXISTS ref.plot_forest_constraints (
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

CREATE TABLE IF NOT EXISTS ref._staging_plot_forest_constraints
  (LIKE ref.plot_forest_constraints INCLUDING DEFAULTS);

COMMENT ON COLUMN ref.plot_forest_constraints.forest_constraint_source IS
  'FOUR-VALUED AND NEVER COLLAPSED. rdppf_registered is legally defensible in front of the DT; computed_from_cadastre is our own inference and is an ALERT, not a legal statement. The frontend must word them differently. Do not merge into a single is_constrained boolean.';

CREATE INDEX IF NOT EXISTS plot_forest_constraints_within20_idx
  ON ref.plot_forest_constraints (within_20m_of_forest) WHERE within_20m_of_forest;
CREATE INDEX IF NOT EXISTS plot_forest_constraints_source_idx
  ON ref.plot_forest_constraints (forest_constraint_source);
CREATE INDEX IF NOT EXISTS plot_forest_constraints_parcel_idx
  ON ref.plot_forest_constraints (no_commune_no_parcelle);

NOTIFY pgrst, 'reload schema';
