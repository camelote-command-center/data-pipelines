/**
 * Orchestrator: fetch content (stage 1) then rebuild events (stage 2).
 * This is the entry point the weekly GitHub Actions cron runs.
 * All tuning is via env (see fetch-content.ts / build-events.ts).
 */
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
function step(file: string) {
  console.log(`\n▶ ${file}`);
  const r = spawnSync('npx', ['tsx', join(HERE, file)], { stdio: 'inherit', env: process.env });
  if (r.status !== 0) { console.error(`✗ ${file} exited ${r.status}`); process.exit(r.status ?? 1); }
}

step('fetch-content.ts');
step('build-events.ts');
console.log('\n✓ amtsblatt-be run complete');
