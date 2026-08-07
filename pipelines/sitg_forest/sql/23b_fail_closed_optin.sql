-- ============================================================================
-- Make gold_ch.sync_full_refresh FAIL CLOSED on a non-allowlisted target
-- ============================================================================
-- On 2026-08-07 a mis-parameterised call reached Branch B, the legacy
-- TRUNCATE+INSERT path, and truncated a live ref table. The procedure emitted a
-- NOTICE saying it was "still on legacy TRUNCATE path" and then did it anyway.
-- A NOTICE is not a guard.
--
-- The fix has to live in the procedure, not the caller. A caller-side check only
-- protects callers that remember to check, and the caller that caused this had
-- read and quoted the warning earlier the same session before walking into it.
-- That was a process gap, not a knowledge gap, so it needs a hard gate.
--
-- Branch B is still legitimately used by the legacy targets that have not been
-- cut over yet. Those are opted in EXPLICITLY via
-- sync_registry.allow_legacy_truncate, which is backfilled to true for exactly
-- the rows relying on it today, so behaviour is unchanged for them. Anything
-- new defaults to false and is refused.
-- ============================================================================

ALTER TABLE gold_ch.sync_registry
  ADD COLUMN IF NOT EXISTS allow_legacy_truncate boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN gold_ch.sync_registry.allow_legacy_truncate IS
  'Explicit opt-in to Branch B, the legacy TRUNCATE+INSERT path on the LIVE target table. Default false: sync_full_refresh refuses a non-allowlisted target unless this is true. Set only for targets that have not yet been cut over to Branch A, and clear it the moment they are. Never set this to silence an error on a new target: the correct fix is to add the tuple to c_allowlist.';

-- Backfill: every row that reaches Branch B TODAY keeps working. A target is on
-- Branch A if its tuple appears in the c_allowlist literal inside the procedure.
UPDATE gold_ch.sync_registry r
   SET allow_legacy_truncate = true
  FROM (SELECT pg_get_functiondef(p.oid) AS f
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'gold_ch' AND p.proname = 'sync_full_refresh') d
 WHERE strpos(d.f, '''' || r.source_table || ''',''' || r.target_table || '''') = 0;
