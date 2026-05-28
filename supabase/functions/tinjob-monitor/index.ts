import "jsr:@supabase/functions-js/edge-runtime.d.ts";

/**
 * tinjob-monitor — refreshes camelote_data.datasets rows for:
 *   - tinjob_jobs_ch   (display_id #100)
 *   - tinjob_jobup_ch  (display_id  #99)
 *
 * Reads the SOURCE OF TRUTH directly: max(ingested_at) per source from
 * Tinjob.public.job_listings. Does NOT trust ingestion_log — the
 * `ingest-jobs` cron on Tinjob currently leaves log rows in 'running'
 * state and the next cron reaps them as 'failed', so ingestion_log
 * never shows fresh 'completed' rows even though scraping is healthy
 * (job_listings keeps growing). Per-source freshness from job_listings
 * is decoupled from that bug.
 *
 * Triggered daily at 08:00 UTC by camelote_data.cron 'tinjob-monitor-daily'.
 *
 * Env:
 *   TINJOB_SERVICE_ROLE_KEY     — read access to Tinjob (jobs-ch)
 *   SUPABASE_SERVICE_ROLE_KEY   — write access to camelote-data datasets table
 */

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

const TINJOB_URL = 'https://rztpdglitcpnkrvdksck.supabase.co';
const TINJOB_KEY = Deno.env.get('TINJOB_SERVICE_ROLE_KEY') || '';
const CAMELOTE_URL = 'https://dxugbpeacnorjunpljih.supabase.co';
const CAMELOTE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || '';

// Map Tinjob source → camelote_data.datasets.id
const DATASETS: Record<string, string> = {
  'jobs.ch':  'a00cc7a7-ffa2-4d04-ad05-3f4717ac22cc', // #100
  'jobup.ch': '97d601c8-32d6-424f-8c4a-1e04eb39786f', // #99
};

async function tinjob(path: string, params = '') {
  const r = await fetch(`${TINJOB_URL}/rest/v1/${path}${params}`, {
    headers: { apikey: TINJOB_KEY, Authorization: `Bearer ${TINJOB_KEY}` },
  });
  if (!r.ok) throw new Error(`Tinjob ${path}: ${r.status} ${await r.text()}`);
  return r.json();
}

async function patchDataset(id: string, body: Record<string, unknown>) {
  const r = await fetch(`${CAMELOTE_URL}/rest/v1/datasets?id=eq.${id}`, {
    method: 'PATCH',
    headers: {
      apikey: CAMELOTE_KEY,
      Authorization: `Bearer ${CAMELOTE_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'return=minimal',
    },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`Dataset patch ${id}: ${r.status} ${await r.text()}`);
}

async function insertHealthHistory(datasetId: string, recordCount: number) {
  const r = await fetch(`${CAMELOTE_URL}/rest/v1/dataset_health_history`, {
    method: 'POST',
    headers: {
      apikey: CAMELOTE_KEY,
      Authorization: `Bearer ${CAMELOTE_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'return=minimal',
    },
    body: JSON.stringify({
      dataset_id: datasetId,
      record_count: recordCount,
      checked_at: new Date().toISOString(),
    }),
  });
  if (!r.ok) console.warn(`health_history skipped (${r.status})`);
}

/**
 * Per-source freshness: latest `ingested_at` of any row in job_listings
 * for this source. PostgREST: `?source=eq.X&select=ingested_at&order=ingested_at.desc&limit=1`.
 */
async function freshnessFor(source: string): Promise<{ lastAcquiredAt: string | null; count: number }> {
  const latest = await tinjob(
    'job_listings',
    `?source=eq.${encodeURIComponent(source)}&select=ingested_at&order=ingested_at.desc&limit=1`,
  );
  const lastAcquiredAt = latest[0]?.ingested_at || null;

  // Active count — used as record_count and for health history
  const countResp = await fetch(
    `${TINJOB_URL}/rest/v1/job_listings?source=eq.${encodeURIComponent(source)}&is_active=eq.true&select=id`,
    {
      headers: {
        apikey: TINJOB_KEY,
        Authorization: `Bearer ${TINJOB_KEY}`,
        Prefer: 'count=exact',
        Range: '0-0',
      },
    },
  );
  if (!countResp.ok) throw new Error(`Tinjob count: ${countResp.status} ${await countResp.text()}`);
  const range = countResp.headers.get('content-range') || '0-0/0';
  const count = parseInt(range.split('/')[1] || '0', 10);

  return { lastAcquiredAt, count };
}

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });

  try {
    if (!TINJOB_KEY) throw new Error('TINJOB_SERVICE_ROLE_KEY not set');
    if (!CAMELOTE_KEY) throw new Error('SUPABASE_SERVICE_ROLE_KEY not set');

    const results: Record<string, unknown> = {};
    for (const [source, datasetId] of Object.entries(DATASETS)) {
      const { lastAcquiredAt, count } = await freshnessFor(source);
      await patchDataset(datasetId, {
        record_count: count,
        last_acquired_at: lastAcquiredAt,
        // For a data-table-watcher capability, last_db_update_at ≡ last_acquired_at:
        // the target DB was last updated when the most recent row was ingested.
        // The Lovable monitoring UI treats NULL last_db_update_at as overdue, so this
        // must be set or #99/#100 will keep showing up in the overdue list even when
        // health_status is on_track.
        last_db_update_at: lastAcquiredAt,
        last_error: null,
        updated_at: new Date().toISOString(),
      });
      await insertHealthHistory(datasetId, count);
      results[source] = { dataset_id: datasetId, active_jobs: count, last_acquired_at: lastAcquiredAt };
    }

    return new Response(
      JSON.stringify({ success: true, timestamp: new Date().toISOString(), sources: results }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } },
    );
  } catch (error) {
    console.error('tinjob-monitor error:', error);
    return new Response(JSON.stringify({ error: (error as Error).message }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }
});
