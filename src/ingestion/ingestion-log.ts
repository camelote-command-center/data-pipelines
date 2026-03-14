import { supabase } from '../config/supabase.js';

export async function startIngestion(
  source: string,
  emirate: string,
  dateFrom: string,
  dateTo: string,
  metadata: Record<string, any> = {},
): Promise<string> {
  const { data, error } = await supabase.rpc('start_ingestion', {
    source,
    emirate,
    date_from: dateFrom,
    date_to: dateTo,
    metadata_jsonb: metadata,
  });

  if (error) throw new Error(`start_ingestion failed: ${error.message}`);
  console.log(`[Ingestion] Started log ${data} for ${source} (${dateFrom} → ${dateTo})`);
  return data as string;
}

export async function completeIngestion(
  logId: string,
  recordsFetched: number,
  recordsInserted: number,
  recordsUpdated: number,
  recordsFailed: number,
  errorMessage: string | null,
): Promise<void> {
  const { error } = await supabase.rpc('complete_ingestion', {
    log_id: logId,
    p_records_fetched: recordsFetched,
    p_records_inserted: recordsInserted,
    p_records_updated: recordsUpdated,
    p_records_failed: recordsFailed,
    p_error: errorMessage,
  });

  if (error) throw new Error(`complete_ingestion failed: ${error.message}`);

  const status = errorMessage ? 'FAILED' : 'OK';
  console.log(
    `[Ingestion] Completed ${logId}: ${status} — fetched=${recordsFetched} inserted=${recordsInserted} updated=${recordsUpdated} failed=${recordsFailed}`,
  );
}
