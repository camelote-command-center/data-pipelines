import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const SUPABASE_DB_SCHEMA = "bronze_ch";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const BATCH_SIZE = 50;

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY).schema(SUPABASE_DB_SCHEMA);

    // Get jobs that haven't been URL-checked in 7 days (or never)
    const { data: jobs, error } = await supabase
      .from("tinjob_job_listings")
      .select("id, job_url")
      .eq("is_active", true)
      .not("job_url", "is", null)
      .or("url_checked_at.is.null,url_checked_at.lt." + new Date(Date.now() - 7 * 86400000).toISOString())
      .order("url_checked_at", { ascending: true, nullsFirst: true })
      .limit(BATCH_SIZE);

    if (error) throw error;
    if (!jobs || jobs.length === 0) {
      return new Response(JSON.stringify({ success: true, checked: 0, message: "All URLs recently checked" }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    let checked = 0;
    let deactivated = 0;
    let stillActive = 0;
    let errors = 0;

    // Check URLs in parallel (batches of 10 to avoid overwhelming)
    for (let i = 0; i < jobs.length; i += 10) {
      const batch = jobs.slice(i, i + 10);

      const results = await Promise.allSettled(
        batch.map(async (job) => {
          if (!job.job_url) return { id: job.id, status: 0 };

          try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 8000);

            const res = await fetch(job.job_url, {
              method: "HEAD",
              headers: {
                "User-Agent": "Mozilla/5.0 (compatible; TinJob/1.0; +https://tinjob.ch)",
              },
              redirect: "follow",
              signal: controller.signal,
            });

            clearTimeout(timeout);
            return { id: job.id, status: res.status };
          } catch {
            // Network error, timeout, etc. — don't deactivate, just mark as checked
            return { id: job.id, status: 0 };
          }
        })
      );

      for (const result of results) {
        if (result.status !== "fulfilled") {
          errors++;
          continue;
        }

        const { id, status } = result.value;
        checked++;

        // 404 or 410 = job removed
        if (status === 404 || status === 410) {
          await supabase.from("tinjob_job_listings").update({
            is_active: false,
            url_status: status,
            url_checked_at: new Date().toISOString(),
          }).eq("id", id);
          deactivated++;
        } else {
          // Any other status (200, 301, 403, 0/error) — mark as checked but keep active
          await supabase.from("tinjob_job_listings").update({
            url_status: status || null,
            url_checked_at: new Date().toISOString(),
          }).eq("id", id);
          stillActive++;
        }
      }
    }

    // Also clean up matches for any newly deactivated jobs
    if (deactivated > 0) {
      // Tinjob matches are archived on RE-LLM; active parser state only needs listing deactivation.
    }

    return new Response(JSON.stringify({
      success: true,
      checked,
      deactivated,
      still_active: stillActive,
      errors,
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (err) {
    console.error("check-stale-urls error:", err);
    return new Response(JSON.stringify({ error: String(err) }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
