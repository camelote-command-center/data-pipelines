/**
 * Regression test for the DD.MM.YYYY date mis-parse (bug f3538ed1).
 *
 * Asserts the LIVE re-LLM `safe_cast.to_date` via the public wrapper RPC
 * `public.assert_safe_cast_to_date`. Before the 2026-07-06 fix, safe_cast did a
 * bare `raw::date` which — under Postgres MDY datestyle — read '02.07.2026' as
 * 2026-02-07 (Feb 7). If safe_cast is ever reverted to that behaviour, this test
 * fails the build.
 *
 * Run with: npx tsx pipelines/transactions-fao/date-parse.test.ts
 * Env: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY pointed at re-LLM
 *      (the transactions-fao workflow already sets these to the RE_LLM_* secrets).
 */
import { supabase } from '../_shared/supabase.js';

let pass = 0;
let fail = 0;

async function check(raw: string, expected: string, note: string) {
  const { data, error } = await supabase.rpc('assert_safe_cast_to_date', { raw });
  if (error) {
    console.error(`  ✗ safe_cast.to_date('${raw}') → RPC error: ${error.message}`);
    fail++;
    return;
  }
  if (data === expected) {
    console.log(`  ✓ safe_cast.to_date('${raw}') = ${data}  (${note})`);
    pass++;
  } else {
    console.error(`  ✗ safe_cast.to_date('${raw}') = ${data}, expected ${expected}  (${note})`);
    fail++;
  }
}

(async () => {
  console.log('safe_cast.to_date — DD.MM.YYYY regression (bug f3538ed1):');
  await check('02.07.2026', '2026-07-02', 'dotted day<=12 — previously mis-parsed to 2026-02-07');
  await check('15.07.2026', '2026-07-15', 'dotted day>12 — previously went NULL');
  await check('2026-07-02', '2026-07-02', 'ISO fall-through must stay unchanged');
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail > 0 ? 1 : 0);
})();
