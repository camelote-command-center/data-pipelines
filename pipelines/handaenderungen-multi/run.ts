/**
 * CLI entry — canton-parameterized Handänderungen ingest.
 *
 *   npx tsx run.ts --canton SZ --fixtures ./fixtures            # dry run over fixtures
 *   npx tsx run.ts --canton SZ                                  # live (throws: access-gated)
 *   npx tsx run.ts --canton SZ --fixtures ./fixtures --load     # + upsert bronze + sync lamap
 *
 * Env (live/load only): RE_LLM_SUPABASE_URL, RE_LLM_SUPABASE_SERVICE_ROLE_KEY,
 * SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (lamap_db), ANTHROPIC_API_KEY (LLM fallback).
 */

import Anthropic from '@anthropic-ai/sdk';
import { getAdapter } from './src/adapters/index.js';
import { ingest } from './src/ingest.js';
import type { Canton } from './src/types.js';

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : undefined;
}
const has = (name: string) => process.argv.includes(`--${name}`);

async function main() {
  const canton = arg('canton') as Canton | undefined;
  if (canton !== 'SZ' && canton !== 'LU') {
    console.error('Usage: --canton SZ|LU [--fixtures <dir>] [--load]. (VS is a no-go — no adapter.)');
    process.exit(2);
  }
  const fixtureDir = arg('fixtures');
  const client = process.env.ANTHROPIC_API_KEY ? new Anthropic() : null;

  const { rows, quarantine, stats } = await ingest(getAdapter(canton), { fixtureDir, client });
  console.log(`[${canton}] ${JSON.stringify(stats)}`);
  if (quarantine.length) console.warn(`  ⚠️ ${quarantine.length} blocks quarantined (unparseable)`);

  if (has('load')) {
    const { upsertBronze, syncToLamap } = await import('./src/load.js');
    const { reconcileEgrid } = await import('./src/reconcile.js');
    const rec = await reconcileEgrid(rows);
    console.log(`  egrid resolved: ${rec.resolved}/${rows.length}`);
    const nBronze = await upsertBronze(rows);
    console.log(`  bronze upserted: ${nBronze}`);
    const nLamap = await syncToLamap([canton]);
    console.log(`  lamap synced: ${nLamap}`);
  } else {
    console.log(`  (dry run — ${rows.length} rows ready; pass --load to persist)`);
  }
}

main().catch((e) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
