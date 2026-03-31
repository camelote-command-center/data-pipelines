/**
 * probe-range.ts — Binary search to find the valid ID range on ACTIS VD
 *
 * Tests a set of candidate IDs against the ACTIS REST endpoint.
 * A valid ID returns HTML containing "No CAMAC".
 * Updates state.json with the discovered range_min and range_max.
 *
 * Usage:
 *   npx tsx probe-range.ts
 */

import { readFileSync, writeFileSync } from 'fs';
import { resolve } from 'path';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const ACTIS_BASE = 'https://www.actis.vd.ch/rest/exp/idqry/9008/param';
const RATE_LIMIT_MS = 1000; // 1 req/sec
const STATE_PATH = resolve(__dirname, 'state.json');

// Candidate IDs to probe — broad sweep then narrowing
const PROBE_IDS = [
  1, 1000, 10000, 50000, 100000,
  150000, 200000, 220000, 240000, 250000,
  260000, 270000, 280000, 290000, 300000,
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchActis(id: number): Promise<string | null> {
  const url = `${ACTIS_BASE}/${id}`;
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const buffer = await res.arrayBuffer();
    const text = new TextDecoder('latin1').decode(buffer);
    // Strip HTML tags and collapse whitespace
    const clean = text
      .replace(/<[^>]*>/g, ' ')
      .replace(/&[^;]+;/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    if (clean.includes('No CAMAC')) return clean;
    return null;
  } catch (err) {
    console.error(`  [ERROR] Fetch failed for ID ${id}:`, (err as Error).message);
    return null;
  }
}

function readState(): Record<string, number> {
  try {
    return JSON.parse(readFileSync(STATE_PATH, 'utf-8'));
  } catch {
    return { last_processed_id: 0, range_min: 0, range_max: 0, total_found: 0, total_processed: 0 };
  }
}

function writeState(state: Record<string, number>): void {
  writeFileSync(STATE_PATH, JSON.stringify(state, null, 2) + '\n');
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  ACTIS VD — Range Probe');
  console.log('='.repeat(60));
  console.log(`\n  Testing ${PROBE_IDS.length} candidate IDs...\n`);

  const results: { id: number; valid: boolean }[] = [];

  for (const id of PROBE_IDS) {
    process.stdout.write(`  ID ${id.toString().padStart(6)} ... `);
    const text = await fetchActis(id);
    const valid = text !== null;
    results.push({ id, valid });
    console.log(valid ? 'VALID' : 'empty');
    await sleep(RATE_LIMIT_MS);
  }

  // Determine range
  const validIds = results.filter((r) => r.valid).map((r) => r.id);

  console.log('\n' + '-'.repeat(60));

  if (validIds.length === 0) {
    console.log('  No valid IDs found in probe set.');
    console.log('  The ACTIS API may be down or IDs may be outside the tested range.');
    return;
  }

  const rangeMin = Math.min(...validIds);
  const rangeMax = Math.max(...validIds);

  console.log(`  Valid IDs found:  ${validIds.length}`);
  console.log(`  Range min:        ${rangeMin}`);
  console.log(`  Range max:        ${rangeMax}`);
  console.log(`  Known working:    224927, 234178, 241566, 245042, 246789, 248082`);

  // Refine: binary search for lower bound between last-invalid and first-valid
  const sortedResults = [...results].sort((a, b) => a.id - b.id);
  let lowerBound = rangeMin;
  let upperBound = rangeMax;

  // Find the transition point: last invalid before first valid
  for (let i = 0; i < sortedResults.length - 1; i++) {
    if (!sortedResults[i].valid && sortedResults[i + 1].valid) {
      lowerBound = sortedResults[i].id;
      break;
    }
  }

  // Find the transition point: last valid before first invalid (at the top)
  for (let i = sortedResults.length - 1; i > 0; i--) {
    if (!sortedResults[i].valid && sortedResults[i - 1].valid) {
      upperBound = sortedResults[i].id;
      break;
    }
  }

  console.log(`\n  Approximate valid range: ${lowerBound} — ${upperBound}`);
  console.log(`  Estimated IDs to scan:  ${upperBound - lowerBound}`);

  // Update state
  const state = readState();
  state.range_min = lowerBound;
  state.range_max = upperBound;
  writeState(state);

  console.log(`\n  State saved to ${STATE_PATH}`);
  console.log('='.repeat(60));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
