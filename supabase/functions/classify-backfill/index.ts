// One-shot batch classifier for backfilling pending knowledge rows.
//
// Deliberately invoked (NOT the per-row AFTER INSERT trigger). Processes up to
// `limit` pending rows scoped to a document source, with bounded concurrency,
// reusing the SAME _shared classifiers as classify-row (DeepSeek V3 primary,
// Sonnet 4.6 fallback) so results match existing rows. DEEPSEEK_API_KEY /
// ANTHROPIC_API_KEY resolve from Deno.env exactly as every other function here.
//
// Request body (service-role auth):
//   { source: string, limit?=50, dry_run?=true, concurrency?=6,
//     doc_type?: string|null, kinds?: ("chunks"|"documents")[] }
// dry_run=true  -> classify and RETURN the classifications, write nothing.
// dry_run=false -> classify, domain-filter, UPDATE, return counts.
//
// Call repeatedly with dry_run=false until processed=0 to drain the backlog
// (each call stays well under the edge wall-clock limit).

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.4";
import { classify as classifySonnet, ClassifyInput, Classification } from "../_shared/classifier.ts";
import { classify as classifyDeepSeek } from "../_shared/classifier-deepseek.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

function jsonResp(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

type Item = {
  id: string;
  table: "documents" | "chunks";
  kind: "document" | "chunk";
  title: string | null;
  input: ClassifyInput;
};

// Mirror classify-row's two-tier routing: DeepSeek first, Sonnet fallback.
async function classifyItem(input: ClassifyInput): Promise<{ cls: Classification | null; model: string }> {
  let cls = await classifyDeepSeek(input);
  let model = "deepseek-chat";
  if (!cls || cls.status === "needs_review") {
    const sonnet = await classifySonnet(input);
    if (sonnet) {
      if (!cls || sonnet.status === "auto") { cls = sonnet; model = "claude-sonnet-4-6"; }
      else if (sonnet.confidence > cls.confidence) { cls = sonnet; model = "claude-sonnet-4-6"; }
    }
  }
  return { cls, model };
}

// Bounded-concurrency map.
async function pool<T, R>(items: T[], n: number, fn: (t: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(n, items.length) }, async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      out[idx] = await fn(items[idx]);
    }
  });
  await Promise.all(workers);
  return out;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  try {
    const body = await req.json().catch(() => ({}));
    const source = String(body.source ?? "");
    const limit = Math.max(1, Math.min(200, Number(body.limit ?? 50)));
    const dryRun = body.dry_run !== false; // default true
    const concurrency = Math.max(1, Math.min(12, Number(body.concurrency ?? 6)));
    const docType = body.doc_type ? String(body.doc_type) : null;
    const kinds: string[] = Array.isArray(body.kinds) && body.kinds.length
      ? body.kinds : ["chunks", "documents"];

    if (!source) return jsonResp({ ok: false, error: "source required" }, 400);

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
    );
    const schema = "knowledge_ch";

    // Canonical per-domain vocab. The model can hallucinate topics for the 19
    // topicless domains (e.g. 'burial' under civic_services); validate_taxonomy
    // would reject those on write. We drop any topic/asset_class not valid for
    // the chosen domain → topicless domains persist with empty topics (allowed).
    const gk = supabase.schema("knowledge_global");
    const [{ data: topicRows }, { data: acRows }] = await Promise.all([
      gk.from("topics").select("domain,key").eq("is_active", true),
      gk.from("asset_classes").select("domain,key").eq("is_active", true),
    ]);
    const topicsByDomain = new Map<string, Set<string>>();
    for (const t of (topicRows ?? []) as any[]) {
      (topicsByDomain.get(t.domain) ?? topicsByDomain.set(t.domain, new Set()).get(t.domain)!).add(t.key);
    }
    const acByDomain = new Map<string, Set<string>>();
    for (const a of (acRows ?? []) as any[]) {
      (acByDomain.get(a.domain) ?? acByDomain.set(a.domain, new Set()).get(a.domain)!).add(a.key);
    }
    const filterTopics = (domain: string, topics: string[] | null | undefined) =>
      (topics ?? []).filter((t) => topicsByDomain.get(domain)?.has(t));
    const filterACs = (domain: string, acs: string[] | null | undefined) =>
      (acs ?? []).filter((a) => acByDomain.get(domain)?.has(a));

    // ---- Fetch up to `limit` pending rows, chunks preferred then documents --
    const items: Item[] = [];

    if (kinds.includes("chunks") && items.length < limit) {
      let q = supabase.schema(schema).from("chunks")
        .select("id,content,section_title,document_id,documents!inner(title,language,country,source,document_type)")
        .eq("documents.source", source)
        .eq("categorization_status", "pending");
      if (docType) q = q.eq("documents.document_type", docType);
      const { data, error } = await q.limit(limit - items.length);
      if (error) return jsonResp({ ok: false, error: `fetch chunks: ${error.message}` }, 500);
      for (const c of (data ?? []) as any[]) {
        const doc = c.documents ?? {};
        items.push({
          id: c.id, table: "chunks", kind: "chunk", title: doc.title ?? null,
          input: {
            kind: "chunk", content: c.content ?? "", section_title: c.section_title,
            doc_title: doc.title ?? null, language: doc.language ?? null, country: doc.country ?? null,
          },
        });
      }
    }

    if (kinds.includes("documents") && items.length < limit) {
      const { data, error } = await supabase.schema(schema).from("documents")
        .select("id,title,description,language,country,document_type")
        .eq("source", source).eq("categorization_status", "pending")
        .limit(limit - items.length);
      if (error) return jsonResp({ ok: false, error: `fetch documents: ${error.message}` }, 500);
      for (const d of (data ?? []) as any[]) {
        const { data: fc } = await supabase.schema(schema).from("chunks")
          .select("content").eq("document_id", d.id)
          .order("chunk_index", { ascending: true }).limit(1).maybeSingle();
        items.push({
          id: d.id, table: "documents", kind: "document", title: d.title ?? null,
          input: {
            kind: "document", title: d.title,
            content: `${d.description ?? ""}\n\n${fc?.content ?? ""}`.trim(),
            language: d.language, country: d.country,
          },
        });
      }
    }

    if (items.length === 0) {
      return jsonResp({ ok: true, source, processed: 0, remaining: 0, dry_run: dryRun, results: [] });
    }

    // ---- Classify with bounded concurrency -------------------------------
    const classified = await pool(items, concurrency, async (it) => {
      const { cls, model } = await classifyItem(it.input);
      return { it, cls, model };
    });

    // ---- Dry run: return classifications, write nothing ------------------
    if (dryRun) {
      const results = classified.map(({ it, cls, model }) => ({
        id: it.id, table: it.table, title: it.title,
        domain: cls?.domain ?? null,
        topics: cls ? filterTopics(cls.domain, cls.topics) : [],   // what would be written
        topics_raw: cls?.topics ?? [],                              // what the model returned
        asset_classes: cls ? (filterACs(cls.domain, cls.asset_classes).length ? filterACs(cls.domain, cls.asset_classes) : null) : null,
        chunk_type: cls?.chunk_type ?? null,
        confidence: cls?.confidence ?? null,
        status: cls?.status ?? null,
        model,
      }));
      return jsonResp({ ok: true, source, dry_run: true, processed: results.length, results });
    }

    // ---- Persist (validate_taxonomy stays enabled; on reject leave pending) -
    let auto = 0, review = 0, deferred = 0, failed = 0;
    for (const { it, cls } of classified) {
      if (!cls) { deferred++; continue; }
      const ft = filterTopics(cls.domain, cls.topics);
      const fac = filterACs(cls.domain, cls.asset_classes);
      const payload: Record<string, unknown> = {
        domain: cls.domain,
        asset_classes: fac.length ? fac : null,
        topics: ft.length ? ft : null,
        tags: cls.tags,
        categorization_confidence: cls.confidence,
        categorization_version: 2,
        categorization_status: cls.status,
      };
      if (it.table === "chunks") payload.chunk_type = cls.chunk_type;
      const { error: upErr } = await supabase.schema(schema).from(it.table)
        .update(payload).eq("id", it.id);
      if (upErr) { deferred++; console.error(`update ${it.table} ${it.id}: ${upErr.message}`); continue; }
      if (cls.status === "auto") auto++; else review++;
    }

    return jsonResp({
      ok: true, source, dry_run: false,
      processed: classified.length, auto, needs_review: review, deferred, failed,
    });
  } catch (err) {
    console.error("classify-backfill error:", err);
    return jsonResp({ ok: false, error: (err as Error).message }, 500);
  }
});
