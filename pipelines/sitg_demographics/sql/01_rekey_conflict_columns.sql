-- ═══════════════════════════════════════════════════════════════════════════
-- sitg_demographics — re-key nine bronze tables off objectid
--
-- APPLY ON re-LLM (bronze_ch). Run BEFORE deploying the matching import.py.
--
-- WHY (bug ab259069)
--   All nine datasets upserted ON CONFLICT (objectid). SITG's own metadata
--   states OBJECTID must not be used as a permanent identifier, and this repo
--   has now been bitten by that twice: ge_ffp_arbres_remarquables doubled when
--   globalid rotated (f69a9dcb), ge_cad_batiments misaligned on a surrogate
--   key (177de4c5). This is the third instance.
--
--   On OTC_AMENAG_2ROUES it was already costing rows. That layer declares
--   objectIdField = "fid" and ALSO carries an unrelated attribute literally
--   named `objectid`, which is not unique: 10 collisions over 6769 live
--   features. Upserting on it collapsed 6 rows every single run — the fetch
--   brought 6769, bronze held 6763, and nothing failed.
--
-- WHAT WAS MEASURED, 2026-08-08 (all nine layers, live source vs bronze)
--   Six layers DO have a genuine attribute business key, unique across the
--   whole live snapshot. Those get the real key: a content hash would be
--   strictly worse, because it would rotate whenever a published value is
--   corrected, orphaning the row it was supposed to track.
--
--     ge_ocs_popbatlog_commune      no_commune     45/45 unique
--     ge_ocs_population_ssecteur    ssecteur_7   485/485 unique
--     ge_ocs_popbatlog_vge_secteur  secteur        16/16 unique
--     ge_ocs_emploi_commune         no_commune     45/45 unique
--     ge_ocs_emploi_vge_secteur     secteur        16/16 unique
--     ge_otc_macaron                zone_macaron   53/53 unique (see NULL note)
--
--   Three layers have NO attribute identity at all. Best attribute-only
--   cardinality: stationnement 6976/12380, amenag_2roues 2387/6769,
--   parcelles_histo 19280/19306 — and for parcelles_histo no combination of
--   the ten source attributes is unique. Those three get a content hash.
--
--   objectid was ALSO checked for rotation, by matching bronze against the
--   live layer on geometry rather than on the id itself: 0 moved on all three
--   hash layers, and 0 moved on the six keyed layers matched by business key.
--   So objectid has held so far. That is not a reason to keep it — SITG says
--   plainly that it is not permanent, and "has not rotated yet" is exactly
--   what remarquables looked like the day before it rotated.
--
-- COLLISIONS: 19 groups, all inspected individually, all genuine
--   parcelles_histo  17 groups / 35 rows — byte-identical attributes AND raw
--                    geometry, differing only in objectid and globalid. One
--                    group is a triple (Grand-Saconnex 26/13716).
--   amenag_2roues     1 group  /  2 rows — fid 2413 and 2416, identical.
--   stationnement     1 group  /  2 rows — objectid 10765/10766, identical in
--                    every column the CURRENT parser writes, and identical raw
--                    geometry. They differ only in `de` and `a`: "avenue de
--                    Riant-Parc" vs "avenue du Mervelet". Those two columns are
--                    JS-era leftovers that the ArcGIS layer no longer publishes
--                    and this parser has never written (3'247 rows already hold
--                    NULL). They CANNOT go into the key — a fresh fetch would
--                    produce NULL for them and orphan the existing row. So the
--                    collapse loses one stale cross-street annotation. That is
--                    a deliberate, recorded trade, and backup.<table>_prerekey_
--                    20260808 keeps the value.
--
-- THE HASH
--   md5 over the source attributes the parser actually writes, plus the
--   geometry with every coordinate rounded to 7 decimal degrees. At 46°N that
--   is ~1.1 cm north-south and ~0.8 cm east-west, far below the positional
--   accuracy of any of these layers, so it cannot merge two features the
--   source distinguishes. Coordinates are rounded and rendered as text, not
--   hashed as binary, so a key stays inspectable when it has to be debugged.
--
--   Columns deliberately EXCLUDED from every hash:
--     objectid, globalid, fid  — the identifiers this whole migration exists
--                                to stop trusting.
--     shape_len, shape_area    — derived from geometry, already covered, and
--                                float noise in a key is a rotation waiting.
--     commune                  — enrichment, not source. NULL on all three OTC
--                                layers today; if it were ever populated the
--                                key would rotate under every row.
--     no_commune_no_parcelle   — derived, NULL on all 19'306 rows.
--     de, a                    — see the stationnement note above.
--
-- IDENTITY MUST AGREE BETWEEN THIS FILE AND THE PARSER
--   import.py computes the same key in Python, because the client-side
--   collapse in shared.supabase_client._dedupe_on_conflict_key has to be able
--   to see the key: a GENERATED column would be invisible to it, and the two
--   real duplicate pairs would then hit Postgres 21000 and take a whole
--   500-row batch down with them. The two implementations are proven equal by
--   re-running the parser and asserting the key set is unchanged — if they
--   disagreed, every row would insert a second time and the assertion fires.
-- ═══════════════════════════════════════════════════════════════════════════

\set ON_ERROR_STOP on

-- ── 0. Snapshot everything first ─────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS backup;

DO $snap$
DECLARE t text; n bigint;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'ge_ocs_popbatlog_commune','ge_ocs_population_ssecteur',
    'ge_ocs_popbatlog_vge_secteur','ge_ocs_emploi_commune',
    'ge_ocs_emploi_vge_secteur','ge_otc_macaron','ge_otc_stationnement',
    'ge_otc_amenag_2roues','ge_cad_parcelles_histo']
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS backup.%I', t || '_prerekey_20260808');
    EXECUTE format('CREATE TABLE backup.%I AS SELECT * FROM bronze_ch.%I',
                   t || '_prerekey_20260808', t);
    EXECUTE format('SELECT count(*) FROM backup.%I', t || '_prerekey_20260808') INTO n;
    RAISE NOTICE 'snapshot backup.%_prerekey_20260808: % rows', t, n;
  END LOOP;
END $snap$;

-- ── 1. Geometry key helper ───────────────────────────────────────────────
-- IMMUTABLE so it can be used in an index or a CHECK later if wanted. Mirrors
-- shared.sitg_arcgis.arcgis_geom_key() in Python exactly: same rounding scale,
-- same half-up rounding (numeric), same separators, same fallbacks.
CREATE OR REPLACE FUNCTION public.sitg_arcgis_geom_key(p_geometry text, p_decimals int DEFAULT 7)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $fn$
  WITH g AS (SELECT nullif(btrim(coalesce(p_geometry, '')), '')::jsonb AS j)
  SELECT CASE
    -- No geometry at all. A literal marker, never an empty string: '' would
    -- make every geometry-less row collide with every other one.
    WHEN (SELECT j FROM g) IS NULL THEN 'nogeom'
    -- Point.
    WHEN (SELECT j FROM g) ? 'x' THEN
      'pt:' || round(((SELECT j FROM g)->>'x')::numeric, p_decimals)::text || ',' ||
               round(((SELECT j FROM g)->>'y')::numeric, p_decimals)::text
    -- Polyline (paths) or polygon (rings).
    WHEN (SELECT j FROM g) ?| ARRAY['paths','rings'] THEN
      coalesce((
        SELECT string_agg(part, '|' ORDER BY ord)
        FROM (
          SELECT ord,
                 (SELECT coalesce(string_agg(
                            round((pt->>0)::numeric, p_decimals)::text || ',' ||
                            round((pt->>1)::numeric, p_decimals)::text, ';' ORDER BY pord), '')
                    FROM jsonb_array_elements(ring) WITH ORDINALITY AS p(pt, pord)) AS part
          FROM jsonb_array_elements(
                 coalesce((SELECT j FROM g)->'paths', (SELECT j FROM g)->'rings'))
               WITH ORDINALITY AS r(ring, ord)
        ) s), '')
    -- Anything else (multipoint, envelope, a shape ArcGIS grows later): fall
    -- back to the raw text so the key stays unique per distinct geometry.
    -- Silently returning '' here would collapse every unrecognised shape into
    -- one row, which is the failure this migration is fixing.
    ELSE 'raw:' || md5(p_geometry)
  END;
$fn$;

COMMENT ON FUNCTION public.sitg_arcgis_geom_key(text, int) IS
  'Canonical text form of an ArcGIS JSON geometry with coordinates rounded to '
  'p_decimals decimal degrees (default 7 ~ 1 cm at 46N). Feeds the content_key '
  'of SITG layers that publish no durable identifier. Must stay identical to '
  'shared.sitg_arcgis.arcgis_geom_key() in Python — import.py computes the key '
  'client-side so that duplicate collapse can see it.';

-- ── 2. Content key on the three layers with no attribute identity ────────
ALTER TABLE bronze_ch.ge_otc_stationnement    ADD COLUMN IF NOT EXISTS content_key text;
ALTER TABLE bronze_ch.ge_otc_amenag_2roues    ADD COLUMN IF NOT EXISTS content_key text;
ALTER TABLE bronze_ch.ge_cad_parcelles_histo  ADD COLUMN IF NOT EXISTS content_key text;

UPDATE bronze_ch.ge_otc_stationnement SET content_key = md5(concat_ws('|',
    coalesce(nom_rues, ''), coalesce(type_stationnement, ''), coalesce(nombre_places, ''),
    public.sitg_arcgis_geom_key(geometry)));

UPDATE bronze_ch.ge_otc_amenag_2roues SET content_key = md5(concat_ws('|',
    coalesce(code_voie, ''), coalesce(nom_voie, ''), coalesce(type_amenagement, ''),
    coalesce(realisation, ''), coalesce(circul2r, ''), coalesce(circvoit, ''),
    coalesce(affectation, ''), coalesce(tourngauche, ''),
    public.sitg_arcgis_geom_key(geometry)));

UPDATE bronze_ch.ge_cad_parcelles_histo SET content_key = md5(concat_ws('|',
    coalesce(commune, ''), coalesce(no_comm, ''), coalesce(no_parcelle, ''),
    coalesce(surface, ''), coalesce(mutmai, ''), coalesce(daterad, ''),
    coalesce(provenance, ''), coalesce(validite, ''), coalesce(ideddp, ''),
    coalesce(type_propri, ''), public.sitg_arcgis_geom_key(geometry)));

-- ── 3. Collapse the duplicates, but only the ones that were inspected ────
-- The expected counts are asserted, not assumed. If the source has published
-- something new since the 2026-08-08 measurement this stops rather than
-- quietly deleting rows nobody looked at.
DO $dedupe$
DECLARE
  r record;
  v_expected constant jsonb :=
    '{"ge_otc_stationnement":1,"ge_otc_amenag_2roues":1,"ge_cad_parcelles_histo":18}';
  v_actual int;
  v_deleted int;
BEGIN
  FOR r IN SELECT * FROM jsonb_each_text(v_expected) AS e(tbl, expected) LOOP
    EXECUTE format(
      'SELECT count(*) - count(DISTINCT content_key) FROM bronze_ch.%I', r.tbl)
      INTO v_actual;
    -- 0 means this file already ran; the collapse is done and re-running must
    -- not be an error. Anything else means the source moved since the counts
    -- below were established by hand.
    CONTINUE WHEN v_actual = 0;
    IF v_actual <> r.expected::int THEN
      RAISE EXCEPTION
        'bronze_ch.%: % duplicate content_keys, expected %. Every collision in '
        'this migration was inspected by hand on 2026-08-08; a different number '
        'means the source moved. Re-inspect before deleting anything.',
        r.tbl, v_actual, r.expected;
    END IF;
    -- Keep the lowest id: deterministic, and the lower id is the row that has
    -- been carrying whatever downstream state exists.
    EXECUTE format(
      'DELETE FROM bronze_ch.%I a USING bronze_ch.%I b
        WHERE a.content_key = b.content_key AND a.id > b.id', r.tbl, r.tbl);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RAISE NOTICE 'bronze_ch.%: collapsed % duplicate row(s)', r.tbl, v_deleted;
  END LOOP;
END $dedupe$;

ALTER TABLE bronze_ch.ge_otc_stationnement    ALTER COLUMN content_key SET NOT NULL;
ALTER TABLE bronze_ch.ge_otc_amenag_2roues    ALTER COLUMN content_key SET NOT NULL;
ALTER TABLE bronze_ch.ge_cad_parcelles_histo  ALTER COLUMN content_key SET NOT NULL;

-- ── 4. Swap the unique constraints ───────────────────────────────────────
-- objectid keeps a PLAIN index. Dropping the uniqueness is the point: leaving
-- it would mean a rotated objectid raises a constraint violation mid-batch
-- instead of being the ignorable provenance value it now is.
DO $swap$
DECLARE
  r record;
  v_targets constant jsonb := '[
    {"t":"ge_ocs_popbatlog_commune",     "k":"(no_commune)",   "nd":false},
    {"t":"ge_ocs_population_ssecteur",   "k":"(ssecteur_7)",   "nd":false},
    {"t":"ge_ocs_popbatlog_vge_secteur", "k":"(secteur)",      "nd":false},
    {"t":"ge_ocs_emploi_commune",        "k":"(no_commune)",   "nd":false},
    {"t":"ge_ocs_emploi_vge_secteur",    "k":"(secteur)",      "nd":false},
    {"t":"ge_otc_macaron",               "k":"(zone_macaron)", "nd":true},
    {"t":"ge_otc_stationnement",         "k":"(content_key)",  "nd":false},
    {"t":"ge_otc_amenag_2roues",         "k":"(content_key)",  "nd":false},
    {"t":"ge_cad_parcelles_histo",       "k":"(content_key)",  "nd":false}]';
BEGIN
  FOR r IN SELECT * FROM jsonb_to_recordset(v_targets) AS x(t text, k text, nd boolean) LOOP
    -- NULLS NOT DISTINCT for macaron: fid 52 is a polygon with every attribute
    -- NULL, so zone_macaron IS NULL on exactly one feature. Under the default
    -- NULLS DISTINCT that row can never match itself and would be re-inserted
    -- on every run forever — the same unbounded growth this migration is here
    -- to stop, just arriving through a different door.
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON bronze_ch.%I %s %s',
                   r.t || '_key_ux', r.t, r.k,
                   CASE WHEN r.nd THEN 'NULLS NOT DISTINCT' ELSE '' END);
    EXECUTE format('DROP INDEX IF EXISTS bronze_ch.%I', 'idx_' || r.t || '_unique');
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON bronze_ch.%I (objectid)',
                   r.t || '_objectid_ix', r.t);
    RAISE NOTICE 'bronze_ch.%: unique key now %', r.t, r.k;
  END LOOP;
END $swap$;

-- ── 5. Say so in the catalogue ───────────────────────────────────────────
DO $doc$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'ge_ocs_popbatlog_commune','ge_ocs_population_ssecteur',
    'ge_ocs_popbatlog_vge_secteur','ge_ocs_emploi_commune',
    'ge_ocs_emploi_vge_secteur','ge_otc_macaron','ge_otc_stationnement',
    'ge_otc_amenag_2roues','ge_cad_parcelles_histo']
  LOOP
    EXECUTE format($c$COMMENT ON COLUMN bronze_ch.%I.objectid IS %L$c$, t,
      'Source ArcGIS id, provenance only. NOT the upsert key since bug '
      'ab259069: SITG states OBJECTID is not a permanent identifier, and on '
      'OTC_AMENAG_2ROUES the column of this name is not even the layer''s '
      'object-id field (that is fid) and is not unique.');
  END LOOP;
END $doc$;

COMMENT ON COLUMN bronze_ch.ge_otc_stationnement.content_key IS
  'md5(nom_rues|type_stationnement|nombre_places|geometry at 7 dp). The layer '
  'publishes no durable identifier and its attributes give only 6976 distinct '
  'values over 12380 features, so geometry is part of the identity.';
COMMENT ON COLUMN bronze_ch.ge_otc_amenag_2roues.content_key IS
  'md5(code_voie|nom_voie|type_amenagement|realisation|circul2r|circvoit|'
  'affectation|tourngauche|geometry at 7 dp). Replaces the `objectid` attribute, '
  'which is not this layer''s object-id field (fid is) and holds 10 duplicate '
  'values, silently collapsing 6 rows per run.';
COMMENT ON COLUMN bronze_ch.ge_cad_parcelles_histo.content_key IS
  'md5(commune|no_comm|no_parcelle|surface|mutmai|daterad|provenance|validite|'
  'ideddp|type_propri|geometry at 7 dp). No combination of source attributes is '
  'unique on this layer — 26 collisions on all ten together — so geometry is '
  'part of the identity.';
COMMENT ON COLUMN bronze_ch.ge_otc_macaron.zone_macaron IS
  'Upsert key. Unique across all 53 live features. Its unique index is NULLS '
  'NOT DISTINCT: fid 52 is a polygon with no attributes at all, and under the '
  'default that row would be re-inserted on every run.';

-- ── 6. Report ────────────────────────────────────────────────────────────
SELECT c.relname AS table_name,
       (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid AND i.indisunique) AS unique_indexes,
       (SELECT string_agg(substring(pg_get_indexdef(i.indexrelid) FROM 'USING .*$'), ' ; ')
          FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
         WHERE i.indrelid = c.oid AND i.indisunique AND ic.relname <> c.relname || '_pkey') AS key_index
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'bronze_ch'
  AND c.relname IN ('ge_ocs_popbatlog_commune','ge_ocs_population_ssecteur',
                    'ge_ocs_popbatlog_vge_secteur','ge_ocs_emploi_commune',
                    'ge_ocs_emploi_vge_secteur','ge_otc_macaron',
                    'ge_otc_stationnement','ge_otc_amenag_2roues',
                    'ge_cad_parcelles_histo')
ORDER BY 1;
