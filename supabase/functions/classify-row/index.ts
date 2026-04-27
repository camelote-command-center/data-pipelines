// Unified classify-row endpoint. Receives {schema, table, row_id} from AFTER INSERT
// triggers via pg_net.http_post. Idempotent: re-fetches the row and skips if already
// classified. Trigger-rejected updates leave the row 'pending' for batch backfill.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.4";
import { classify, ClassifyInput } from "../_shared/classifier.ts";
import { writeReviewIfNeeded } from "../_shared/review.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const SUPPORTED_TABLES = new Set(["entries", "documents", "chunks"]);
const SUPPORTED_SCHEMAS = new Set(["knowledge_ch", "knowledge_global", "knowledge_ae"]);

function jsonResp(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const body = await req.json().catch(() => null);
    if (!body || typeof body !== "object") {
      return jsonResp({ ok: false, error: "Invalid JSON body" }, 400);
    }

    const schema = String(body.schema ?? "");
    const table  = String(body.table  ?? "");
    const rowId  = String(body.row_id ?? "");

    if (!SUPPORTED_SCHEMAS.has(schema)) {
      return jsonResp({ ok: false, error: `unsupported schema: ${schema}` }, 400);
    }
    if (!SUPPORTED_TABLES.has(table)) {
      return jsonResp({ ok: false, error: `unsupported table: ${table}` }, 400);
    }
    if (!rowId) {
      return jsonResp({ ok: false, error: "row_id required" }, 400);
    }

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
    );

    // Per-table column selection
    const sel = table === "entries"
      ? "id,title,content,country,language,categorization_status"
      : table === "documents"
      ? "id,title,description,language,country,categorization_status"
      : "id,content,section_title,document_id,categorization_status,chunk_index";

    const { data: row, error: rowErr } = await supabase
      .schema(schema).from(table).select(sel).eq("id", rowId).maybeSingle();

    if (rowErr) return jsonResp({ ok: false, error: rowErr.message }, 500);
    if (!row)   return jsonResp({ ok: false, error: "row not found" }, 404);

    // Idempotency: skip if already classified
    const status = (row as any).categorization_status;
    if (status && status !== "pending") {
      return jsonResp({ ok: true, skipped: true, reason: `already ${status}` });
    }

    // Build classifier input
    let input: ClassifyInput;
    let excerptForReview: string;

    if (table === "chunks") {
      const r = row as any;
      const { data: doc } = await supabase
        .schema(schema).from("documents")
        .select("title,language,country").eq("id", r.document_id).maybeSingle();
      input = {
        kind: "chunk",
        content: r.content ?? "",
        section_title: r.section_title,
        doc_title: doc?.title ?? null,
        language: doc?.language ?? null,
        country: doc?.country ?? null,
      };
      excerptForReview = r.content ?? "";
    } else if (table === "documents") {
      const r = row as any;
      const { data: firstChunk } = await supabase
        .schema(schema).from("chunks")
        .select("content")
        .eq("document_id", r.id)
        .order("chunk_index", { ascending: true })
        .limit(1).maybeSingle();
      input = {
        kind: "document",
        title: r.title,
        content: `${r.description ?? ""}\n\n${firstChunk?.content ?? ""}`.trim(),
        language: r.language,
        country: r.country,
      };
      excerptForReview = r.description ?? "";
    } else {
      const r = row as any;
      input = {
        kind: "entry",
        title: r.title,
        content: r.content ?? "",
        language: r.language,
        country: r.country,
      };
      excerptForReview = r.content ?? "";
    }

    const cls = await classify(input);
    if (!cls) {
      // API/network error — leave row 'pending'. Batch backfill catches it.
      return jsonResp({ ok: false, deferred: true, reason: "classifier api error" });
    }

    const updatePayload: Record<string, unknown> = {
      domain: cls.domain,
      asset_classes: cls.asset_classes,
      topics: cls.topics,
      tags: cls.tags,
      categorization_confidence: cls.confidence,
      categorization_version: 2,
      categorization_status: cls.status,
    };
    // documents has no chunk_type column (per file 01)
    if (table === "chunks" || table === "entries") {
      updatePayload.chunk_type = cls.chunk_type;
    }

    try {
      const { error: upErr } = await supabase
        .schema(schema).from(table).update(updatePayload).eq("id", rowId);
      if (upErr) throw upErr;
    } catch (err) {
      // Trigger rejection (cross-axis confusion) or other DB error.
      // Leave row 'pending', log, return — batch backfill will retry with
      // the patched prompt that has AXIS DISCIPLINE + COMMON CONFUSIONS.
      console.error(
        `classify-row update failed for ${schema}.${table}.${rowId}:`,
        err instanceof Error ? err.message : err,
      );
      return jsonResp({ ok: false, deferred: true, reason: "trigger rejection or db error" });
    }

    if (cls.status === "needs_review") {
      // Atomic dedup via uniq_review_pending_per_source partial unique index.
      // 23505 unique_violation (concurrent retry) is swallowed inside writeReviewIfNeeded.
      try {
        await writeReviewIfNeeded(supabase, schema, table, rowId, excerptForReview, cls);
      } catch (err) {
        console.error(
          `classify-row review insert failed for ${schema}.${table}.${rowId}:`,
          err instanceof Error ? err.message : err,
        );
      }
    }

    return jsonResp({
      ok: true,
      status: cls.status,
      confidence: cls.confidence,
      schema,
      table,
      row_id: rowId,
    });
  } catch (err) {
    console.error("classify-row error:", err);
    return jsonResp({ ok: false, error: (err as Error).message }, 500);
  }
});
