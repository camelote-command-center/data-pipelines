/**
 * Regression test for the safe_cast date parser.
 *
 * Covers two shipped bugs:
 *   f3538ed1 (2026-07-06) — a bare `raw::date` read '02.07.2026' as 2026-02-07
 *                           under Postgres MDY datestyle.
 *   817d1453 (2026-08-17) — French long-form dates ('07 août 2026') failed to
 *                           parse at all.
 *
 * IMPORTANT — this harness must never write to production.
 * It calls `public.probe_safe_cast_to_date`, a SIDE-EFFECT-FREE wrapper over
 * `safe_cast.parse_date`. That function contains no INSERT, so a CI run cannot
 * add rows to `safe_cast.quarantine`. Do NOT point this test back at
 * `assert_safe_cast_to_date` / `safe_cast.to_date`: those quarantine by design,
 * and CI traffic would then show up as a production data-quality signal.
 *
 * All parsing lives in `safe_cast.parse_date`; `safe_cast.to_date` merely
 * delegates to it and adds the range clamp plus quarantine bookkeeping. So
 * asserting the probe is an equivalent guard — a revert of the parsing logic
 * still fails this build.
 *
 * An unparseable value makes parse_date RAISE, surfacing here as an RPC error
 * and a failed build. That is intended: fail loudly rather than write silently.
 *
 * Run with: npx tsx pipelines/transactions-fao/date-parse.test.ts
 * Env: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY pointed at re-LLM.
 */
import { supabase } from '../_shared/supabase.js';

let pass = 0;
let fail = 0;

async function check(raw: string, expected: string, note: string) {
  const { data, error } = await supabase.rpc('probe_safe_cast_to_date', { raw });
  if (error) {
    console.error(`  ✗ parse_date('${raw}') → RPC error: ${error.message}`);
    fail++;
    return;
  }
  if (data === expected) {
    console.log(`  ✓ parse_date('${raw}') = ${data}  (${note})`);
    pass++;
  } else {
    console.error(`  ✗ parse_date('${raw}') = ${data}, expected ${expected}  (${note})`);
    fail++;
  }
}

(async () => {
  console.log('safe_cast.parse_date — DD.MM.YYYY regression (bug f3538ed1):');
  await check('02.07.2026', '2026-07-02', 'dotted day<=12 — previously mis-parsed to 2026-02-07');
  await check('15.07.2026', '2026-07-15', 'dotted day>12 — previously went NULL');
  await check('2026-07-02', '2026-07-02', 'ISO fall-through must stay unchanged');

  console.log('\nsafe_cast.parse_date — French long-form regression (bug 817d1453):');
  await check('07 août 2026', '2026-08-07', 'accented month');
  await check('07 aout 2026', '2026-08-07', 'unaccented spelling of the same month');
  await check('1er mai 2026', '2026-05-01', '"1er" ordinal day');
  await check('3 décembre 2025', '2025-12-03', 'accented month, single-digit day');

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail > 0 ? 1 : 0);
})();
