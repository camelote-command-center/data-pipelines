import "jsr:@supabase/functions-js/edge-runtime.d.ts";

/**
 * relm-sync-monitor — refreshes camelote_data.datasets rows for the relm_sync_*
 * monitor entries by reading max(finished_at) per source_table from re-LLM's
 * gold_ch.sync_log directly.
 *
 * Why this exists: the re-LLM refresh-and-distribute crons (jobids 43/44/45)
 * were simplified at some point and no longer PATCH camelote-data with sync
 * freshness. The data flows correctly into the consumer DBs (lamap_db, lamap-crm,
 * lamap-lbi) and gold_ch.sync_log records the success, but camelote shows the
 * dataset rows stuck on stale `last_acquired_at` because nothing is writing them.
 *
 * Same architectural pattern as tinjob-monitor: decouple monitoring from
 * whatever the producer chain is doing — read the source of truth (sync_log)
 * directly. If the producer cron is fixed later to PATCH again, this monitor
 * is idempotent and harmless.
 *
 * The trigger trg_datasets_sync_last_db_update_at on public.datasets mirrors
 * last_acquired_at into last_db_update_at automatically — we only need to set
 * last_acquired_at here.
 *
 * Env:
 *   RE_LLM_DB_URL              — full postgres URI for re-LLM (session pooler)
 *   SUPABASE_SERVICE_ROLE_KEY  — write access to camelote-data datasets table
 */

import { Client } from "https://deno.land/x/postgres@v0.19.3/mod.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const CAMELOTE_URL = "https://dxugbpeacnorjunpljih.supabase.co";
const CAMELOTE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const RE_LLM_DB_URL = Deno.env.get("RE_LLM_DB_URL") || "";

/**
 * Map a camelote dataset code (relm_sync_<freq>_<basename>) to candidate
 * re-LLM sync_log source_table names. Order matters — first match wins.
 */
function candidateSources(code: string): string[] {
  // code = "relm_sync_daily_link_plot_sad" → basename = "link_plot_sad"
  const parts = code.split("_");
  if (parts.length < 4 || parts[0] !== "relm" || parts[1] !== "sync") return [];
  const basename = parts.slice(3).join("_");
  return [
    `silver_ch.${basename}`,
    `gold_ch.${basename}`,
    `gold_ch.v_${basename}_full`,
    `gold_ch.core_${basename}`,
  ];
}

async function patchDataset(id: string, body: Record<string, unknown>) {
  const r = await fetch(`${CAMELOTE_URL}/rest/v1/datasets?id=eq.${id}`, {
    method: "PATCH",
    headers: {
      apikey: CAMELOTE_KEY,
      Authorization: `Bearer ${CAMELOTE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`PATCH ${id}: ${r.status} ${await r.text()}`);
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  if (!CAMELOTE_KEY) return new Response(JSON.stringify({ error: "SUPABASE_SERVICE_ROLE_KEY missing" }), { status: 500 });
  if (!RE_LLM_DB_URL) return new Response(JSON.stringify({ error: "RE_LLM_DB_URL missing" }), { status: 500 });

  const client = new Client(RE_LLM_DB_URL);
  try {
    await client.connect();

    // 1. Get latest successful sync per source_table from re-LLM
    const syncRows = await client.queryObject<{ source_table: string; latest: Date }>(
      `SELECT source_table, max(finished_at) AS latest
       FROM gold_ch.sync_log
       WHERE status = 'success'
       GROUP BY source_table`,
    );
    const sourceLatest = new Map<string, string>(
      syncRows.rows.map((r) => [r.source_table, (r.latest as Date).toISOString()]),
    );

    // 2. Get all relm_sync_* datasets from camelote-data
    const dsResp = await fetch(
      `${CAMELOTE_URL}/rest/v1/datasets?code=like.relm_sync_*&select=id,display_id,code,last_acquired_at`,
      { headers: { apikey: CAMELOTE_KEY, Authorization: `Bearer ${CAMELOTE_KEY}` } },
    );
    if (!dsResp.ok) throw new Error(`fetch datasets: ${dsResp.status}`);
    const datasets: Array<{ id: string; display_id: number; code: string; last_acquired_at: string | null }> =
      await dsResp.json();

    // 3. For each, find the best matching source_table and PATCH if newer
    const results: Array<{ display_id: number; code: string; matched: string | null; before: string | null; after: string | null; updated: boolean }> = [];
    let matched = 0, updated = 0, skipped = 0, unmatched = 0;

    for (const d of datasets) {
      const candidates = candidateSources(d.code);
      const match = candidates.find((c) => sourceLatest.has(c));
      if (!match) {
        unmatched++;
        results.push({ display_id: d.display_id, code: d.code, matched: null, before: d.last_acquired_at, after: null, updated: false });
        continue;
      }
      matched++;
      const newLatest = sourceLatest.get(match)!;
      if (d.last_acquired_at && new Date(d.last_acquired_at) >= new Date(newLatest)) {
        skipped++;
        results.push({ display_id: d.display_id, code: d.code, matched: match, before: d.last_acquired_at, after: d.last_acquired_at, updated: false });
        continue;
      }
      await patchDataset(d.id, {
        last_acquired_at: newLatest,
        last_error: null,
        updated_at: new Date().toISOString(),
      });
      updated++;
      results.push({ display_id: d.display_id, code: d.code, matched: match, before: d.last_acquired_at, after: newLatest, updated: true });
    }

    return new Response(
      JSON.stringify({
        success: true,
        timestamp: new Date().toISOString(),
        summary: { total: datasets.length, matched, updated, skipped, unmatched },
        results,
      }, null, 2),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (error) {
    console.error("relm-sync-monitor error:", error);
    return new Response(JSON.stringify({ error: (error as Error).message }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } finally {
    try { await client.end(); } catch { /* ignore */ }
  }
});
