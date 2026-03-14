import { DubaiPulseClient } from '../api/dubai-pulse-client.js';
import { ENDPOINTS } from '../api/endpoints.js';
import { supabase } from '../config/supabase.js';
import { startIngestion, completeIngestion } from './ingestion-log.js';
import { bulkInsert } from './bulk-insert.js';
import { parseCsv } from './csv-ingestion.js';
import { transformTransactionRow } from '../parsers/transactions.js';
import { transformRentalRow } from '../parsers/rentals.js';
import { transformProjectRow } from '../parsers/projects.js';
import { transformValuationRow } from '../parsers/valuations.js';
import { transformLandRow } from '../parsers/properties-land.js';
import { transformBuildingRow } from '../parsers/properties-buildings.js';
import { transformUnitRow } from '../parsers/properties-units.js';
import { transformBrokerRow } from '../parsers/brokers.js';
import { transformDeveloperRow } from '../parsers/developers.js';

type TransformFn = (row: Record<string, any>) => Record<string, any>;

const TRANSFORMERS: Record<string, TransformFn> = {
  transactions: transformTransactionRow,
  rentals: transformRentalRow,
  projects: transformProjectRow,
  valuations: transformValuationRow,
  land: transformLandRow,
  buildings: transformBuildingRow,
  units: transformUnitRow,
  brokers: transformBrokerRow,
  developers: transformDeveloperRow,
};

const DEDUP_RPCS: Record<string, string> = {
  transactions: 'dedup_transactions',
  rentals: 'dedup_rentals',
};

const PROMOTE_RPCS: Record<string, string> = {
  transactions: 'promote_transactions',
  rentals: 'promote_rentals',
};

const GOLD_RPCS: Record<string, { rpc: string; param: string }> = {
  transactions: { rpc: 'refresh_market_data_from_transactions', param: 'months_back' },
  rentals: { rpc: 'refresh_market_data_from_rentals', param: 'months_back' },
};

export interface IngestOptions {
  source: string;
  dateFrom: string;
  dateTo: string;
  mode: 'api' | 'csv';
  csvPath?: string;
}

export async function ingestSource(options: IngestOptions): Promise<void> {
  const { source, dateFrom, dateTo, mode, csvPath } = options;

  const endpoint = ENDPOINTS[source];
  if (!endpoint) throw new Error(`Unknown source: ${source}. Valid: ${Object.keys(ENDPOINTS).join(', ')}`);

  const transformer = TRANSFORMERS[source];
  if (!transformer) throw new Error(`No transformer for source: ${source}`);

  console.log(`\n${'='.repeat(60)}`);
  console.log(`Ingesting ${source} (${mode}) — ${dateFrom} to ${dateTo}`);
  console.log(`${'='.repeat(60)}\n`);

  const logId = await startIngestion(`dld_${source}`, 'dubai', dateFrom, dateTo, { method: mode });

  try {
    let rows: Record<string, any>[];

    if (mode === 'api') {
      const client = new DubaiPulseClient();
      let filter: string | undefined;
      if (endpoint.dateField) {
        filter = `${endpoint.dateField} >= '${dateFrom}' AND ${endpoint.dateField} <= '${dateTo}'`;
      }
      rows = await client.fetchAll(endpoint.slug, filter);
    } else {
      if (!csvPath) throw new Error('CSV mode requires --csv path');
      rows = await parseCsv(csvPath);
    }

    console.log(`[Ingest] Transforming ${rows.length} rows...`);
    const records = rows.map(transformer);

    const { inserted, failed } = await bulkInsert(endpoint.table, records, logId);

    await completeIngestion(logId, rows.length, inserted, 0, failed, null);

    // All RPCs are called via public schema wrappers

    // Dedup if applicable
    const dedupRpc = DEDUP_RPCS[source];
    if (dedupRpc) {
      console.log(`[Ingest] Running dedup: ${dedupRpc}()`);
      const { error } = await supabase.rpc(dedupRpc);
      if (error) console.error(`[Ingest] Dedup warning: ${error.message}`);
    }

    // Promote to silver if applicable
    const promoteRpc = PROMOTE_RPCS[source];
    if (promoteRpc) {
      console.log(`[Ingest] Promoting: ${promoteRpc}()`);
      const { error } = await supabase.rpc(promoteRpc, {
        since: new Date(dateFrom).toISOString(),
      });
      if (error) console.error(`[Ingest] Promote warning: ${error.message}`);
    }

    // Refresh gold aggregates if applicable
    const goldConfig = GOLD_RPCS[source];
    if (goldConfig) {
      console.log(`[Ingest] Refreshing gold: ${goldConfig.rpc}()`);
      const { error } = await supabase.rpc(goldConfig.rpc, {
        [goldConfig.param]: 3,
      });
      if (error) console.error(`[Ingest] Gold refresh warning: ${error.message}`);
    }

    console.log(`\n[Ingest] ${source} complete: ${inserted} inserted, ${failed} failed\n`);
  } catch (error: any) {
    await completeIngestion(logId, 0, 0, 0, 0, error.message);
    throw error;
  }
}
