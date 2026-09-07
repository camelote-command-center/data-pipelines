/**
 * Canton-parameterized ingest core.
 *
 * adapter.discover → parse (regex → Sonnet fallback) → ownerless flag → normalize to
 * bronze rows. Pure w.r.t. the database: it returns rows + stats; persistence lives in
 * load.ts so this path is fully unit-testable against fixtures with no network.
 */

import type Anthropic from '@anthropic-ai/sdk';
import type { BronzeTxnRow } from './types.js';
import type { SourceAdapter } from './adapters/types.js';
import { parseBlock } from './parse.js';
import { withOwnerlessFlag } from './ownerless.js';
import { toBronzeRow } from './normalize.js';

export interface IngestStats {
  discovered: number;
  parsedHigh: number;
  parsedLow: number;
  quarantined: number;
  ownerless: number;
}

export interface IngestResult {
  rows: BronzeTxnRow[];
  quarantine: string[]; // raw blocks that fully failed to parse
  stats: IngestStats;
}

export async function ingest(
  adapter: SourceAdapter,
  opts: { fixtureDir?: string; since?: string; client?: Anthropic | null },
): Promise<IngestResult> {
  const raws = await adapter.discover({ fixtureDir: opts.fixtureDir, since: opts.since });
  const client = opts.client ?? null;

  const rows: BronzeTxnRow[] = [];
  const quarantine: string[] = [];
  const stats: IngestStats = {
    discovered: raws.length,
    parsedHigh: 0,
    parsedLow: 0,
    quarantined: 0,
    ownerless: 0,
  };

  for (const raw of raws) {
    const parsed = await parseBlock(raw.text, client);
    if (!parsed || (!parsed.sellers.length && !parsed.buyers.length && !parsed.parcel_number)) {
      quarantine.push(raw.text);
      stats.quarantined++;
      continue;
    }
    if (parsed.parse_confidence === 'high') stats.parsedHigh++;
    else stats.parsedLow++;

    const flagged = withOwnerlessFlag(parsed);
    if (flagged.is_ownerless_event) stats.ownerless++;
    rows.push(toBronzeRow(flagged, raw.pub));
  }

  // Dedupe within-run on (source_id, canton) — the source can repeat a parcel across issues.
  const seen = new Set<string>();
  const deduped = rows.filter((r) => {
    const k = `${r.source_id}§${r.canton}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  return { rows: deduped, quarantine, stats };
}
