// pipeline-watchdog-alert — THIN Resend adapter. No DB access, no auth/business logic.
// Recipient is HARDCODED (not payload-driven) by design (no open-relay shape).
// Reuses the existing project RESEND_API_KEY env + the send-alert-emails sender convention.
const RESEND_API_KEY = Deno.env.get("RESEND_API_KEY");
const FROM = "LaMap Alertes <alertes@lamap.ch>";
const TO = "info@lamap.ch"; // Ilan admin inbox — hardcoded, NEVER from payload.

Deno.serve(async (req) => {
  try {
    const p = await req.json().catch(() => ({}));
    const stale = Array.isArray(p?.stale_pipes) ? p.stale_pipes : [];
    const n = stale.length;
    const names = stale.map((s: any) => s.pipe_name).join(", ") || "(none)";
    const isTest = p?.is_forced_test === true;
    const subject = `[lamap pipeline]${isTest ? "[TEST]" : ""} ${n} pipe(s) stale: ${names}`;

    const lines: string[] = [];
    lines.push(`${isTest ? "FORCED-BREAK TEST — " : ""}Pipeline freshness watchdog (${p?.environment ?? "production"})`);
    lines.push(`Triggered: ${p?.triggered_at ?? new Date().toISOString()}`);
    if (p?.summary) lines.push(`Summary: ${p.summary}`);
    lines.push("");
    for (const s of stale) {
      lines.push(`• ${s.pipe_name}: lag=${s.lag ?? "?"}, last_event_date=${s.last_event_date ?? "?"}, last_upstream_sync_at=${s.last_upstream_sync_at ?? "?"}`);
    }
    lines.push("");
    lines.push("Tracked as a high-severity incident on pixxels_data (admin.pixxels.io → incidents).");
    const text = lines.join("\n");

    if (!RESEND_API_KEY) {
      return new Response(JSON.stringify({ ok: false, error: "RESEND_API_KEY missing" }), { status: 500, headers: { "Content-Type": "application/json" } });
    }
    // No retries by design — watchdog cron logs failures + the bug-row channel is the redundancy.
    const resp = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: { "Authorization": `Bearer ${RESEND_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({ from: FROM, to: TO, subject, text }),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      return new Response(JSON.stringify({ ok: false, resend: data }), { status: 502, headers: { "Content-Type": "application/json" } });
    }
    return new Response(JSON.stringify({ ok: true, id: data?.id, subject }), { status: 200, headers: { "Content-Type": "application/json" } });
  } catch (e) {
    return new Response(JSON.stringify({ ok: false, error: String(e) }), { status: 500, headers: { "Content-Type": "application/json" } });
  }
});
