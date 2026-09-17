-- ============================================================================
-- 2026-09-17 — Owner/party -> entity resolver: structural name rule (bug 8151336e)
-- ============================================================================
-- Why: silver_ch.fn_resolve_buyer_to_entity trusted a single candidate with strict_word_similarity >= 0.85
-- (fuzzy tier). Substring containment scores 1.0 even when the family name or a given name differs, so
-- "BENOIT Pascal" was attached to "DESTOUCHES Benoit Pascal" and "ROHRBACH François" to "ROHRBACH Françoise".
-- It also deferred any name with >= 2 similar candidates (41.7% of resolvable labelled cases).
--
-- Measured on 268 labelled FAO seller mentions (truth = single-owner parcel land-registry owner; RE-LLM
-- silver_ch.jev_shadow_evaluations c7a1e0d2-0f3e-5b1a-9d6e-2a9b1c0e5001): production 147 correct / 10 wrong /
-- 111 abstain; structural rule 215 / 9 / 44 (206 / 9 / 53 without the location tie-break).
--
-- Rule (candidate generation unchanged: trigram top 50, same entity_type, legal-form filter):
--   individuals  : same family name (leading capitalised tokens; accents, case, hyphens, spaces ignored) AND every
--                  given name of the mention appears among the candidate's given names (an initial matches);
--   organisations: same distinctive tokens after removing legal forms (Jaccard >= 0.8);
--   identical normalised names count as one party (duplicate records) -> one compatible party => match;
--   several compatible parties => tie-break on p_city vs candidate head office / communes of plots owned; else defer;
--   no compatible candidate but a name-similar one (>= 0.85) => defer for review (never force-match, never create);
--   no similar candidate at all => create / defer exactly as before.
-- Return type, decisions vocabulary and signature are unchanged (both callers keep working).
-- ROLLBACK: restore silver_ch.fn_resolve_buyer_to_entity and silver_ch.populate_plot_owner_overrides from
--           backup.fn_defs_20260913 (fn = '<name>@pre-structural-rule'); helper functions are additive.
-- ============================================================================

CREATE OR REPLACE FUNCTION silver_ch.fn_party_name_norm(p text)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path = public, pg_temp AS $$
  SELECT btrim(regexp_replace(regexp_replace(lower(public.unaccent(coalesce(p,''))), '[-''’.,/"()]', ' ', 'g'), '\s+', ' ', 'g'))
$$;

-- family name key (leading tokens whose letters are all upper case) and normalised given-name tokens
CREATE OR REPLACE FUNCTION silver_ch.fn_person_name_parts(p text, OUT family_key text, OUT given text[])
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE SET search_path = silver_ch, public, pg_temp AS $$
DECLARE t text; letters text; in_family boolean := true; fam text[] := '{}';
BEGIN
  given := '{}';
  FOREACH t IN ARRAY regexp_split_to_array(btrim(coalesce(p,'')), '\s+') LOOP
    CONTINUE WHEN t = '';
    letters := regexp_replace(t, '[^[:alpha:]]', '', 'g');
    IF in_family AND letters <> '' AND letters = upper(letters) THEN
      fam := fam || t;
    ELSE
      in_family := false;
      IF silver_ch.fn_party_name_norm(t) <> '' THEN given := given || silver_ch.fn_party_name_norm(t); END IF;
    END IF;
  END LOOP;
  family_key := replace(silver_ch.fn_party_name_norm(array_to_string(fam, ' ')), ' ', '');
END $$;

CREATE OR REPLACE FUNCTION silver_ch.fn_party_names_compatible(p_mention text, p_candidate text, p_entity_type text)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE SET search_path = silver_ch, public, pg_temp AS $$
DECLARE m record; c record; g text; ok boolean; a text[]; b text[]; inter int; uni int;
  legal constant text[] := ARRAY['sa','sarl','ag','gmbh','srl','sagl','snc','scs','ltd','llc','inc','en','liquidation'];
BEGIN
  IF p_entity_type = 'person' THEN
    SELECT * INTO m FROM silver_ch.fn_person_name_parts(p_mention);
    SELECT * INTO c FROM silver_ch.fn_person_name_parts(p_candidate);
    IF coalesce(m.family_key,'') = '' OR coalesce(c.family_key,'') = '' OR m.family_key <> c.family_key THEN RETURN false; END IF;
    FOREACH g IN ARRAY m.given LOOP
      SELECT EXISTS (SELECT 1 FROM unnest(c.given) x WHERE x = g OR (length(g) = 1 AND left(x, 1) = g)) INTO ok;
      IF NOT ok THEN RETURN false; END IF;
    END LOOP;
    RETURN true;
  END IF;
  -- registered legal names may carry their translations in parentheses: "REALSTONE FONDATION DE PLACEMENT
  -- (REALSTONE ANLAGESTIFTUNG) (...)". Compare the name outside parentheses when one remains.
  a := ARRAY(SELECT DISTINCT x FROM unnest(string_to_array(silver_ch.fn_party_name_norm(
         COALESCE(NULLIF(btrim(regexp_replace(p_mention, '\([^)]*\)', ' ', 'g')), ''), p_mention)), ' ')) x WHERE x <> '' AND x <> ALL (legal));
  b := ARRAY(SELECT DISTINCT x FROM unnest(string_to_array(silver_ch.fn_party_name_norm(
         COALESCE(NULLIF(btrim(regexp_replace(p_candidate, '\([^)]*\)', ' ', 'g')), ''), p_candidate)), ' ')) x WHERE x <> '' AND x <> ALL (legal));
  IF cardinality(a) = 0 OR cardinality(b) = 0 THEN RETURN false; END IF;
  SELECT count(*) INTO inter FROM (SELECT unnest(a) INTERSECT SELECT unnest(b)) i;
  SELECT count(*) INTO uni FROM (SELECT unnest(a) UNION SELECT unnest(b)) u;
  RETURN inter::numeric / uni >= 0.8;
END $$;

-- Resolver with the structural rule. Created under _v2 for side-by-side validation, then swapped in below.
CREATE OR REPLACE FUNCTION silver_ch.fn_resolve_buyer_to_entity_v2(p_name text, p_dob date DEFAULT NULL::date, p_city text DEFAULT NULL::text)
 RETURNS TABLE(decision text, entity_id uuid, confidence numeric, reason text, candidates jsonb)
 LANGUAGE plpgsql STABLE
 SET search_path TO 'silver_ch', 'public', 'pg_catalog'
AS $function$
DECLARE
  n RECORD; v_search_norm text; v_target_type text; v_mention_norm text; v_c jsonb;
  v_best_word numeric; v_groups int; v_similar int; v_loc jsonb; v_city text;
  v_pick uuid; v_pick_word numeric; v_pick_key text;
BEGIN
  IF p_name IS NULL OR btrim(p_name)='' THEN
    decision:='defer'; entity_id:=NULL; confidence:=NULL; reason:='empty_input'; candidates:='[]'::jsonb;
    RETURN NEXT; RETURN;
  END IF;
  SELECT * INTO n FROM silver_ch.fn_normalize_owner_name(p_name);
  v_target_type := CASE WHEN n.is_person THEN 'person' ELSE 'company' END;
  v_search_norm := CASE WHEN n.is_person AND n.surname_norm IS NOT NULL
                        THEN n.surname_norm || COALESCE(' ' || n.firstname_norm, '') ELSE n.base_norm END;
  v_mention_norm := silver_ch.fn_party_name_norm(p_name);

  -- candidate generation identical to the previous resolver (trigram top 50, type, legal-form filter)
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'entity_id', f.cand_id, 'canonical_name', f.canonical_name, 'name_key', f.name_key,
           'sim_word', round(f.sim_word::numeric,3), 'sim_str', round(f.sim_str::numeric,3), 'lfc', f.cand_lfc,
           'compatible', silver_ch.fn_party_names_compatible(p_name, f.canonical_name, v_target_type))
         ORDER BY f.sim_word DESC, f.sim_str DESC, f.cand_id), '[]'::jsonb)
    INTO v_c
  FROM (
    SELECT e.entity_id AS cand_id, e.canonical_name, e.name_normalized AS name_key,
           similarity(e.name_normalized_unaccented, v_search_norm) AS sim_str,
           strict_word_similarity(v_search_norm, e.name_normalized_unaccented) AS sim_word,
           e.legal_form_class AS cand_lfc
    FROM silver_ch.entities e
    WHERE e.entity_type = v_target_type AND e.status = 'active'
      AND (e.name_normalized_unaccented % v_search_norm OR v_search_norm <% e.name_normalized_unaccented)
    ORDER BY sim_word DESC, sim_str DESC
    LIMIT 50
  ) f
  WHERE v_target_type <> 'company' OR n.legal_form_class IS NULL OR f.cand_lfc IS NOT DISTINCT FROM n.legal_form_class;

  SELECT max((x->>'sim_word')::numeric),
         count(DISTINCT x->>'name_key') FILTER (WHERE (x->>'compatible')::boolean),
         count(*) FILTER (WHERE NOT (x->>'compatible')::boolean AND (x->>'sim_word')::numeric >= 0.85)
    INTO v_best_word, v_groups, v_similar
  FROM jsonb_array_elements(v_c) x;
  candidates := COALESCE((SELECT jsonb_agg(x - 'name_key') FROM (SELECT x FROM jsonb_array_elements(v_c) x LIMIT 5) s), '[]'::jsonb);

  IF v_groups = 1 THEN
    SELECT (x->>'entity_id')::uuid, (x->>'sim_word')::numeric, x->>'name_key' INTO v_pick, v_pick_word, v_pick_key
    FROM jsonb_array_elements(v_c) WITH ORDINALITY t(x, i) WHERE (x->>'compatible')::boolean ORDER BY i LIMIT 1;
    entity_id := v_pick; confidence := v_pick_word;
    IF silver_ch.fn_party_name_norm(v_pick_key) = v_mention_norm THEN
      decision := 'exact'; reason := 'structural_match same_name';
    ELSE
      decision := 'fuzzy'; reason := 'structural_match compatible_names';
    END IF;
    RETURN NEXT; RETURN;
  END IF;

  IF v_groups > 1 THEN
    v_city := silver_ch.fn_party_name_norm(p_city);
    IF v_city <> '' THEN
      SELECT jsonb_agg(DISTINCT x->>'name_key') INTO v_loc
      FROM jsonb_array_elements(v_c) x
      WHERE (x->>'compatible')::boolean AND EXISTS (
        SELECT 1 FROM silver_ch.link_plot_owners o LEFT JOIN gold_ch.core_plots p ON p.egrid = o.egrid
        WHERE o.entity_id = (x->>'entity_id')::uuid
          AND (silver_ch.fn_party_name_norm(o.head_office) = v_city OR silver_ch.fn_party_name_norm(p.commune_name) = v_city));
      IF jsonb_array_length(COALESCE(v_loc, '[]'::jsonb)) = 1 THEN
        SELECT (x->>'entity_id')::uuid, (x->>'sim_word')::numeric INTO v_pick, v_pick_word
        FROM jsonb_array_elements(v_c) WITH ORDINALITY t(x, i)
        WHERE (x->>'compatible')::boolean AND x->>'name_key' = v_loc->>0
          AND EXISTS (SELECT 1 FROM silver_ch.link_plot_owners o LEFT JOIN gold_ch.core_plots p ON p.egrid = o.egrid
                      WHERE o.entity_id = (x->>'entity_id')::uuid
                        AND (silver_ch.fn_party_name_norm(o.head_office) = v_city OR silver_ch.fn_party_name_norm(p.commune_name) = v_city))
        ORDER BY i LIMIT 1;
        decision := 'fuzzy'; entity_id := v_pick; confidence := v_pick_word; reason := 'structural_match location_tiebreak';
        RETURN NEXT; RETURN;
      END IF;
    END IF;
    decision := 'ambiguous_defer'; entity_id := NULL; confidence := v_best_word;
    reason := format('structural_ambiguous parties=%s', v_groups);
    RETURN NEXT; RETURN;
  END IF;

  -- no structurally compatible candidate
  IF v_similar > 0 THEN
    decision := 'defer'; entity_id := NULL; confidence := v_best_word; reason := 'similar_but_incompatible_names';
  ELSIF v_best_word IS NULL OR v_best_word < 0.65 THEN
    IF n.is_person AND n.surname_norm IS NOT NULL AND n.firstname_norm IS NOT NULL THEN
      decision := 'create'; entity_id := NULL; confidence := NULL; reason := 'no_match person';
    ELSIF NOT n.is_person AND n.base_norm IS NOT NULL AND length(n.base_norm) >= 2 THEN
      decision := 'create'; entity_id := NULL; confidence := NULL; reason := format('no_match company base_norm=%s', n.base_norm);
    ELSE
      decision := 'defer'; entity_id := NULL; confidence := v_best_word; reason := 'low_confidence';
    END IF;
  ELSE
    decision := 'defer'; entity_id := NULL; confidence := v_best_word; reason := 'weak_incompatible';
  END IF;
  RETURN NEXT;
END;
$function$;

-- ---------------------------------------------------------------- swap + monotone application
-- Backups of the previous definitions (applied once).
INSERT INTO backup.fn_defs_20260913 (fn, md5, definition)
SELECT p.proname || '@pre-structural-rule', md5(pg_get_functiondef(p.oid)), pg_get_functiondef(p.oid)
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'silver_ch' AND p.proname IN ('fn_resolve_buyer_to_entity', 'populate_plot_owner_overrides')
  AND NOT EXISTS (SELECT 1 FROM backup.fn_defs_20260913 b WHERE b.fn = p.proname || '@pre-structural-rule');

-- worklist rows whose buyer is now resolved are marked, never deleted
ALTER TABLE silver_ch.plots_missing_owners DROP CONSTRAINT plots_missing_owners_status_chk;
ALTER TABLE silver_ch.plots_missing_owners ADD CONSTRAINT plots_missing_owners_status_chk
  CHECK (status = ANY (ARRAY['pending'::text, 'rf_confirmed'::text, 'auto_resolved'::text]));

-- the production resolver takes the validated body (signature and return type unchanged)
CREATE OR REPLACE FUNCTION silver_ch.fn_resolve_buyer_to_entity(p_name text, p_dob date DEFAULT NULL::date, p_city text DEFAULT NULL::text)
 RETURNS TABLE(decision text, entity_id uuid, confidence numeric, reason text, candidates jsonb)
 LANGUAGE plpgsql STABLE
 SET search_path TO 'silver_ch', 'public', 'pg_catalog'
AS $function$
DECLARE
  n RECORD; v_search_norm text; v_target_type text; v_mention_norm text; v_c jsonb;
  v_best_word numeric; v_groups int; v_similar int; v_loc jsonb; v_city text;
  v_pick uuid; v_pick_word numeric; v_pick_key text;
BEGIN
  IF p_name IS NULL OR btrim(p_name)='' THEN
    decision:='defer'; entity_id:=NULL; confidence:=NULL; reason:='empty_input'; candidates:='[]'::jsonb;
    RETURN NEXT; RETURN;
  END IF;
  SELECT * INTO n FROM silver_ch.fn_normalize_owner_name(p_name);
  v_target_type := CASE WHEN n.is_person THEN 'person' ELSE 'company' END;
  v_search_norm := CASE WHEN n.is_person AND n.surname_norm IS NOT NULL
                        THEN n.surname_norm || COALESCE(' ' || n.firstname_norm, '') ELSE n.base_norm END;
  v_mention_norm := silver_ch.fn_party_name_norm(p_name);

  -- candidate generation identical to the previous resolver (trigram top 50, type, legal-form filter)
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'entity_id', f.cand_id, 'canonical_name', f.canonical_name, 'name_key', f.name_key,
           'sim_word', round(f.sim_word::numeric,3), 'sim_str', round(f.sim_str::numeric,3), 'lfc', f.cand_lfc,
           'compatible', silver_ch.fn_party_names_compatible(p_name, f.canonical_name, v_target_type))
         ORDER BY f.sim_word DESC, f.sim_str DESC, f.cand_id), '[]'::jsonb)
    INTO v_c
  FROM (
    SELECT e.entity_id AS cand_id, e.canonical_name, e.name_normalized AS name_key,
           similarity(e.name_normalized_unaccented, v_search_norm) AS sim_str,
           strict_word_similarity(v_search_norm, e.name_normalized_unaccented) AS sim_word,
           e.legal_form_class AS cand_lfc
    FROM silver_ch.entities e
    WHERE e.entity_type = v_target_type AND e.status = 'active'
      AND (e.name_normalized_unaccented % v_search_norm OR v_search_norm <% e.name_normalized_unaccented)
    ORDER BY sim_word DESC, sim_str DESC
    LIMIT 50
  ) f
  WHERE v_target_type <> 'company' OR n.legal_form_class IS NULL OR f.cand_lfc IS NOT DISTINCT FROM n.legal_form_class;

  SELECT max((x->>'sim_word')::numeric),
         count(DISTINCT x->>'name_key') FILTER (WHERE (x->>'compatible')::boolean),
         count(*) FILTER (WHERE NOT (x->>'compatible')::boolean AND (x->>'sim_word')::numeric >= 0.85)
    INTO v_best_word, v_groups, v_similar
  FROM jsonb_array_elements(v_c) x;
  candidates := COALESCE((SELECT jsonb_agg(x - 'name_key') FROM (SELECT x FROM jsonb_array_elements(v_c) x LIMIT 5) s), '[]'::jsonb);

  IF v_groups = 1 THEN
    SELECT (x->>'entity_id')::uuid, (x->>'sim_word')::numeric, x->>'name_key' INTO v_pick, v_pick_word, v_pick_key
    FROM jsonb_array_elements(v_c) WITH ORDINALITY t(x, i) WHERE (x->>'compatible')::boolean ORDER BY i LIMIT 1;
    entity_id := v_pick; confidence := v_pick_word;
    IF silver_ch.fn_party_name_norm(v_pick_key) = v_mention_norm THEN
      decision := 'exact'; reason := 'structural_match same_name';
    ELSE
      decision := 'fuzzy'; reason := 'structural_match compatible_names';
    END IF;
    RETURN NEXT; RETURN;
  END IF;

  IF v_groups > 1 THEN
    v_city := silver_ch.fn_party_name_norm(p_city);
    IF v_city <> '' THEN
      SELECT jsonb_agg(DISTINCT x->>'name_key') INTO v_loc
      FROM jsonb_array_elements(v_c) x
      WHERE (x->>'compatible')::boolean AND EXISTS (
        SELECT 1 FROM silver_ch.link_plot_owners o LEFT JOIN gold_ch.core_plots p ON p.egrid = o.egrid
        WHERE o.entity_id = (x->>'entity_id')::uuid
          AND (silver_ch.fn_party_name_norm(o.head_office) = v_city OR silver_ch.fn_party_name_norm(p.commune_name) = v_city));
      IF jsonb_array_length(COALESCE(v_loc, '[]'::jsonb)) = 1 THEN
        SELECT (x->>'entity_id')::uuid, (x->>'sim_word')::numeric INTO v_pick, v_pick_word
        FROM jsonb_array_elements(v_c) WITH ORDINALITY t(x, i)
        WHERE (x->>'compatible')::boolean AND x->>'name_key' = v_loc->>0
          AND EXISTS (SELECT 1 FROM silver_ch.link_plot_owners o LEFT JOIN gold_ch.core_plots p ON p.egrid = o.egrid
                      WHERE o.entity_id = (x->>'entity_id')::uuid
                        AND (silver_ch.fn_party_name_norm(o.head_office) = v_city OR silver_ch.fn_party_name_norm(p.commune_name) = v_city))
        ORDER BY i LIMIT 1;
        decision := 'fuzzy'; entity_id := v_pick; confidence := v_pick_word; reason := 'structural_match location_tiebreak';
        RETURN NEXT; RETURN;
      END IF;
    END IF;
    decision := 'ambiguous_defer'; entity_id := NULL; confidence := v_best_word;
    reason := format('structural_ambiguous parties=%s', v_groups);
    RETURN NEXT; RETURN;
  END IF;

  -- no structurally compatible candidate
  IF v_similar > 0 THEN
    decision := 'defer'; entity_id := NULL; confidence := v_best_word; reason := 'similar_but_incompatible_names';
  ELSIF v_best_word IS NULL OR v_best_word < 0.65 THEN
    IF n.is_person AND n.surname_norm IS NOT NULL AND n.firstname_norm IS NOT NULL THEN
      decision := 'create'; entity_id := NULL; confidence := NULL; reason := 'no_match person';
    ELSIF NOT n.is_person AND n.base_norm IS NOT NULL AND length(n.base_norm) >= 2 THEN
      decision := 'create'; entity_id := NULL; confidence := NULL; reason := format('no_match company base_norm=%s', n.base_norm);
    ELSE
      decision := 'defer'; entity_id := NULL; confidence := v_best_word; reason := 'low_confidence';
    END IF;
  ELSE
    decision := 'defer'; entity_id := NULL; confidence := v_best_word; reason := 'weak_incompatible';
  END IF;
  RETURN NEXT;
END;
$function$;

CREATE OR REPLACE FUNCTION silver_ch.populate_plot_owner_overrides(p_egrid text DEFAULT NULL::text)
 RETURNS TABLE(overrides_upserted integer, entities_created integer, enqueued_missing integer, egrids_guard_passed integer)
 LANGUAGE plpgsql
 SET search_path TO 'silver_ch', 'public', 'pg_catalog'
 SET statement_timeout TO '1200s'
AS $function$
DECLARE
  v_over int := 0; v_ent int := 0; v_miss int := 0; v_eg int := 0;
BEGIN
  -- 1) latest qualifying transaction per egrid (achat with both buyers & sellers)
  CREATE TEMP TABLE _latest ON COMMIT DROP AS
  SELECT DISTINCT ON (lt.egrid)
         lt.egrid, t.transaction_id, t.transaction_date, t.publication_date,
         t.buyers_display, t.sellers_display, t.affaire_number, t.commune
  FROM silver_ch.link_plot_transactions lt
  JOIN silver_ch.event_transactions t ON t.transaction_id = lt.transaction_id
  WHERE (p_egrid IS NULL OR lt.egrid = p_egrid)
    AND t.transaction_type = 'achat'
    AND COALESCE(t.buyers_display,'') <> ''
    AND COALESCE(t.sellers_display,'') <> ''
  ORDER BY lt.egrid, t.transaction_date DESC NULLS LAST, t.publication_date DESC NULLS LAST;

  -- 2) seller-match guard: >=1 seller's normalized words are a subset of a current RF owner's words (unchanged)
  CREATE TEMP TABLE _guard ON COMMIT DROP AS
  WITH sellers AS (
    SELECT l.egrid, l.transaction_id, l.transaction_date, l.buyers_display, l.affaire_number, l.commune,
           array_remove(string_to_array(regexp_replace(lower(public.unaccent(btrim(s))),'[^a-z0-9 ]',' ','g'),' '),'') AS seller_words
    FROM _latest l, unnest(string_to_array(l.sellers_display, ', ')) s
  ),
  owners AS (
    SELECT o.egrid, o.owner_name, o.entity_id,
           array_remove(string_to_array(regexp_replace(lower(public.unaccent(o.owner_name_normalized)),'[^a-z0-9 ]',' ','g'),' '),'') AS owner_words
    FROM silver_ch.link_plot_owners o
    WHERE p_egrid IS NULL OR o.egrid = p_egrid
  )
  SELECT DISTINCT ON (s.egrid)
         s.egrid, s.transaction_id, s.transaction_date, s.buyers_display, s.affaire_number, s.commune,
         o.owner_name AS old_owner_name, o.entity_id AS old_entity_id
  FROM sellers s
  JOIN owners o ON o.egrid = s.egrid
   AND array_length(s.seller_words,1) >= 1
   AND s.seller_words <@ o.owner_words
  ORDER BY s.egrid, o.owner_name;
  GET DIAGNOSTICS v_eg = ROW_COUNT;

  -- 3) explode buyers and resolve each with the structural resolver, passing the buyer's FAO domicile
  --    (fallback: transaction commune) for the location tie-break.
  --    2026-09-17 monotone rule: an existing active match that still passes the structural name check is kept
  --    when the resolver now defers or picks a duplicate record with the same normalised name; only structurally
  --    incompatible matches are replaced or withdrawn.
  CREATE TEMP TABLE _resolved ON COMMIT DROP AS
  WITH b AS (
    SELECT g.egrid, g.transaction_id, g.transaction_date, g.old_owner_name, g.old_entity_id, g.affaire_number, g.commune,
           btrim(x) AS buyer_name
    FROM _guard g, unnest(string_to_array(g.buyers_display, ', ')) x
    WHERE btrim(x) <> ''
  )
  SELECT b.egrid, b.transaction_id, b.transaction_date, b.old_owner_name, b.old_entity_id, b.buyer_name,
         CASE WHEN keep THEN ex.resolver_decision ELSE r.decision END AS decision,
         CASE WHEN keep THEN ex.new_entity_id ELSE r.entity_id END AS resolved_entity_id,
         CASE WHEN keep THEN ex.confidence ELSE r.confidence END AS confidence,
         keep AS kept_existing,
         (silver_ch.fn_normalize_owner_name(b.buyer_name)).is_person AS is_person
  FROM b
  CROSS JOIN LATERAL (SELECT COALESCE(
      (SELECT o->>'city' FROM bronze_ch.fao_transactions f, jsonb_array_elements(f.new_owner_s::jsonb) o
        WHERE f.affaire_number = b.affaire_number AND b.affaire_number IS NOT NULL
          AND lower(public.unaccent(o->>'name')) = lower(public.unaccent(b.buyer_name)) LIMIT 1),
      b.commune) AS city) c
  CROSS JOIN LATERAL silver_ch.fn_resolve_buyer_to_entity(b.buyer_name, NULL, c.city) r
  LEFT JOIN silver_ch.plot_owner_overrides ex
         ON ex.egrid = b.egrid AND ex.new_owner_name = b.buyer_name AND ex.status = 'active' AND ex.new_entity_id IS NOT NULL
  LEFT JOIN silver_ch.entities exe ON exe.entity_id = ex.new_entity_id
  LEFT JOIN silver_ch.entities ne ON ne.entity_id = r.entity_id
  CROSS JOIN LATERAL (SELECT (ex.new_entity_id IS NOT NULL
      AND silver_ch.fn_party_names_compatible(b.buyer_name, exe.canonical_name, exe.entity_type)
      AND (r.entity_id IS NULL OR r.entity_id = ex.new_entity_id OR ne.name_normalized = exe.name_normalized)) AS keep) k;

  -- 4) create entities for confident 'create' decisions (idempotent: NOT EXISTS guard)
  WITH to_create AS (
    SELECT DISTINCT buyer_name,
           CASE WHEN is_person THEN 'person' ELSE 'company' END AS et,
           lower(public.unaccent(buyer_name)) AS nn
    FROM _resolved WHERE decision = 'create'
  ), ins AS (
    INSERT INTO silver_ch.entities
      (entity_id, entity_type, canonical_name, name_normalized, name_normalized_unaccented,
       legal_form_class, status, source_tables, first_seen_at, last_seen_at, created_at, updated_at)
    SELECT gen_random_uuid(), tc.et, tc.buyer_name, tc.nn, tc.nn,
           (silver_ch.fn_normalize_owner_name(tc.buyer_name)).legal_form_class,
           'active', ARRAY['plot_owner_overrides'], now(), now(), now(), now()
    FROM to_create tc
    WHERE NOT EXISTS (SELECT 1 FROM silver_ch.entities e WHERE e.entity_type = tc.et AND e.name_normalized = tc.nn)
    RETURNING 1
  ) SELECT count(*) INTO v_ent FROM ins;

  -- 5) attach entity_id for create decisions (now resolvable by normalized name + type)
  UPDATE _resolved r SET resolved_entity_id = e.entity_id
  FROM silver_ch.entities e
  WHERE r.decision = 'create' AND r.resolved_entity_id IS NULL
    AND e.name_normalized = lower(public.unaccent(r.buyer_name))
    AND e.entity_type = CASE WHEN r.is_person THEN 'person' ELSE 'company' END;

  -- 6) upsert active overrides (name always kept; entity_id NULL for ambiguous/defer)
  WITH up AS (
    INSERT INTO silver_ch.plot_owner_overrides
      (egrid, old_owner_name, old_entity_id, new_owner_name, new_entity_id,
       transaction_id, transaction_date, status, confidence, resolver_decision, updated_at)
    SELECT egrid, old_owner_name, old_entity_id, buyer_name, resolved_entity_id,
           transaction_id, transaction_date, 'active', confidence, decision, now()
    FROM _resolved
    ON CONFLICT (egrid, new_owner_name) DO UPDATE SET
      new_entity_id     = EXCLUDED.new_entity_id,
      old_owner_name    = EXCLUDED.old_owner_name,
      old_entity_id     = EXCLUDED.old_entity_id,
      transaction_id    = EXCLUDED.transaction_id,
      transaction_date  = EXCLUDED.transaction_date,
      status            = 'active',
      confidence        = EXCLUDED.confidence,
      resolver_decision = EXCLUDED.resolver_decision,
      updated_at        = now()
    WHERE silver_ch.plot_owner_overrides.new_entity_id   IS DISTINCT FROM EXCLUDED.new_entity_id
       OR silver_ch.plot_owner_overrides.transaction_id  IS DISTINCT FROM EXCLUDED.transaction_id
       OR silver_ch.plot_owner_overrides.status          <> 'active'
       OR silver_ch.plot_owner_overrides.resolver_decision IS DISTINCT FROM EXCLUDED.resolver_decision
    RETURNING 1
  ) SELECT count(*) INTO v_over FROM up;

  -- 7) retire stale active overrides for processed egrids (buyer no longer in latest tx)
  UPDATE silver_ch.plot_owner_overrides o SET status='retired', updated_at=now()
  WHERE o.status='active'
    AND o.egrid IN (SELECT egrid FROM _guard)
    AND NOT EXISTS (SELECT 1 FROM _resolved r WHERE r.egrid=o.egrid AND r.buyer_name=o.new_owner_name);

  -- 8) Path B worklist: ambiguous/unmatched buyers (name retained, not force-assigned)
  WITH mq AS (
    INSERT INTO silver_ch.plots_missing_owners (egrid, buyer_name, transaction_id, reason, status)
    SELECT egrid, buyer_name, transaction_id,
           CASE WHEN decision='ambiguous_defer' THEN 'ambiguous' ELSE 'unmatched' END, 'pending'
    FROM _resolved WHERE decision IN ('ambiguous_defer','defer') AND resolved_entity_id IS NULL
    ON CONFLICT (egrid, buyer_name) DO UPDATE SET status = 'pending', reason = EXCLUDED.reason
      WHERE silver_ch.plots_missing_owners.status = 'auto_resolved'
    RETURNING 1
  ) SELECT count(*) INTO v_miss FROM mq;

  -- 9) worklist rows whose buyer now has an entity are marked auto_resolved (never deleted)
  UPDATE silver_ch.plots_missing_owners m SET status = 'auto_resolved'
  FROM _resolved r
  WHERE m.egrid = r.egrid AND m.buyer_name = r.buyer_name AND m.status = 'pending' AND r.resolved_entity_id IS NOT NULL;

  overrides_upserted := v_over; entities_created := v_ent; enqueued_missing := v_miss; egrids_guard_passed := v_eg;
  RETURN NEXT;
END;
$function$;

DROP FUNCTION IF EXISTS silver_ch.fn_resolve_buyer_to_entity_v2(text, date, text);   -- validation copy only
