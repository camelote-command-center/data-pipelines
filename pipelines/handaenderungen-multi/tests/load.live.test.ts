/**
 * LIVE load-path verification against re-LLM + lamap_db.
 * Proves the real load.ts end to end: insert → re-upsert (idempotent) → query the
 * ownerless flag + egrid → cleanup. Writes test canton='SZ' rows (net-new canton) then purges.
 *
 * Env: RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY (re-LLM REST),
 *      SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (lamap REST — unused for ref),
 *      LAMAP_DB_URI (lamap session pooler — ref/serving writes + counts).
 */
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { ingest } from '../src/ingest.js';
import { schwyzAdapter } from '../src/adapters/schwyz.js';
import { upsertBronze, syncToLamap, purgeCantons } from '../src/load.js';
import { reconcileEgrid } from '../src/reconcile.js';
import { endPool } from '../src/lamap-pg.js';
import { Pool } from 'pg';

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), '..', 'fixtures');
const CANTON = 'SZ';
const lamap = new Pool({ connectionString: process.env.LAMAP_DB_URI, max: 2 });
let fail = 0;
const check = (cond: boolean, note: string) => {
  console.log(`  ${cond ? '✓' : '✗'} ${note}`);
  if (!cond) fail++;
};

async function counts() {
  const reLlm = await import('../../_shared/re-llm.js');
  const b = await reLlm.supabase.schema('bronze_ch').from('transactions_national')
    .select('*', { count: 'exact', head: true }).eq('canton', CANTON);
  const ref = await lamap.query('SELECT count(*)::int n FROM ref.transactions_national WHERE canton=$1', [CANTON]);
  const srv = await lamap.query('SELECT count(*)::int n FROM public.transactions_national_data WHERE canton=$1', [CANTON]);
  return { bronze: b.count ?? 0, ref: ref.rows[0].n, serving: srv.rows[0].n };
}

async function main() {
  console.log('— pre-clean —');
  await purgeCantons([CANTON]);
  const before = await counts();
  console.log(`  baseline ${CANTON}:`, JSON.stringify(before));
  check(before.bronze === 0 && before.ref === 0 && before.serving === 0, 'clean slate (0/0/0)');

  console.log('— ingest fixtures (regex, no LLM) —');
  const { rows, stats } = await ingest(schwyzAdapter, { fixtureDir: FIXTURES, client: null });
  console.log(`  ${JSON.stringify(stats)}`);

  // Exercise reconcile (best-effort against real ref.plots) + prove the generated egrid
  // column surfaces raw_data.egrid by injecting a known value on one row.
  const rec = await reconcileEgrid(rows);
  console.log(`  egrid reconciled (real ref.plots): ${rec.resolved}/${rows.length}`);
  rows[0].raw_data.egrid = 'CH-TEST-EGRID-0001';

  console.log('— run 1: upsertBronze → syncToLamap —');
  const nB = await upsertBronze(rows);
  const nL = await syncToLamap([CANTON]);
  const after1 = await counts();
  console.log(`  bronze upserted ${nB}, lamap synced ${nL}; counts:`, JSON.stringify(after1));
  check(after1.ref === rows.length, `ref count == ${rows.length}`);
  check(after1.serving === rows.length, `serving count == ${rows.length}`);
  check(after1.bronze === rows.length, `bronze count == ${rows.length}`);

  // Prove is_ownerless_event + egrid are first-class queryable columns (not buried jsonb).
  const ow = await lamap.query(
    `SELECT source_id, egrid, is_ownerless_event FROM ref.transactions_national
      WHERE canton=$1 AND is_ownerless_event = true`, [CANTON]);
  console.log('  WHERE is_ownerless_event=true →', JSON.stringify(ow.rows));
  check(ow.rowCount === stats.ownerless, `ownerless queryable == ${stats.ownerless}`);
  const eg = await lamap.query(
    `SELECT source_id, egrid FROM ref.transactions_national WHERE canton=$1 AND egrid IS NOT NULL`, [CANTON]);
  console.log('  WHERE egrid IS NOT NULL →', JSON.stringify(eg.rows));
  check(eg.rows.some((r) => r.egrid === 'CH-TEST-EGRID-0001'), 'egrid queryable (generated col surfaces raw_data.egrid)');

  console.log('— run 2: re-upsert (idempotency) —');
  await upsertBronze(rows);
  await syncToLamap([CANTON]);
  const after2 = await counts();
  console.log(`  counts after re-run:`, JSON.stringify(after2));
  check(after2.ref === after1.ref, 're-upsert did not duplicate ref');
  check(after2.serving === after1.serving, 're-upsert did not duplicate serving');
  check(after2.bronze === after1.bronze, 're-upsert did not duplicate bronze');

  console.log('— cleanup —');
  await purgeCantons([CANTON]);
  const end = await counts();
  console.log(`  final ${CANTON}:`, JSON.stringify(end));
  check(end.bronze === 0 && end.ref === 0 && end.serving === 0, 'cleaned up (0/0/0)');

  await lamap.end();
  await endPool();
  console.log(fail === 0 ? '\nLIVE LOAD PATH: PASS' : `\nLIVE LOAD PATH: FAIL (${fail})`);
  if (fail) process.exit(1);
}
main().catch(async (e) => {
  console.error(e instanceof Error ? e.message : e);
  await lamap.end().catch(() => {});
  await endPool().catch(() => {});
  process.exit(1);
});
