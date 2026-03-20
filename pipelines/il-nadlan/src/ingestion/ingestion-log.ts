import { createClient } from '@supabase/supabase-js';
import { supabase } from '../config/supabase.js';

function schemaClient(schema: string) {
  return createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, {
    auth: { persistSession: false, autoRefreshToken: false },
    db: { schema },
  });
}

export async function startIngestion(
  source: string,
  region: string,
  dateFrom: string,
  dateTo: string,
  metadata?: Record<string, any>,
): Promise<string> {
  const { data, error } = await supabase.rpc('start_ingestion', {
    p_source: source,
    p_region: region,
    p_date_from: dateFrom,
    p_date_to: dateTo,
    p_metadata: metadata || {},
  });

  if (error) throw new Error(`start_ingestion failed: ${error.message}`);
  return data as string;
}

export async function completeIngestion(
  logId: string,
  fetched: number,
  inserted: number,
  updated: number,
  failed: number,
  errorMsg?: string,
): Promise<void> {
  const { error } = await supabase.rpc('complete_ingestion', {
    p_log_id: logId,
    p_records_fetched: fetched,
    p_records_inserted: inserted,
    p_records_updated: updated,
    p_records_failed: failed,
    p_error: errorMsg || null,
  });

  if (error) {
    console.error(`complete_ingestion failed: ${error.message}`);
  }
}

export async function dedupTransactions(): Promise<number> {
  const { data, error } = await supabase.rpc('dedup_transactions');
  if (error) {
    console.error(`dedup_transactions failed: ${error.message}`);
    return 0;
  }
  return (data as number) || 0;
}

export async function promoteTransactions(since?: string): Promise<number> {
  const { data, error } = await schemaClient('silver_il').rpc('promote_transactions', {
    p_since: since || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString(),
  });
  if (error) {
    console.error(`promote_transactions failed: ${error.message}`);
    return 0;
  }
  return (data as number) || 0;
}

export async function refreshMarketData(monthsBack?: number): Promise<number> {
  const { data, error } = await schemaClient('gold_il').rpc('refresh_market_data', {
    p_months_back: monthsBack || 12,
  });
  if (error) {
    console.error(`refresh_market_data failed: ${error.message}`);
    return 0;
  }
  return (data as number) || 0;
}
