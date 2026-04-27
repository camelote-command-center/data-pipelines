import { Classification } from "./classifier.ts";

export async function writeReviewIfNeeded(
  // deno-lint-ignore no-explicit-any
  supabase: any,
  sourceSchema: string,
  sourceTable: string,
  sourceId: string,
  contentExcerpt: string,
  cls: Classification,
): Promise<void> {
  if (cls.status !== "needs_review") return;
  const { error } = await supabase.schema("knowledge_global").from("categorization_review").insert({
    source_schema: sourceSchema,
    source_table: sourceTable,
    source_id: sourceId,
    content_excerpt: contentExcerpt.slice(0, 1000),
    suggested_domain: cls.domain,
    suggested_asset_classes: cls.asset_classes,
    suggested_topics: cls.topics,
    suggested_chunk_type: cls.chunk_type,
    suggested_tags: cls.tags,
    confidence: cls.confidence,
    reasoning: cls.reasoning,
  });
  // 23505 unique_violation = concurrent retry beat us; uniq_review_pending_per_source enforces dedup atomically.
  if (error && error.code !== "23505") {
    console.error("review queue write failed:", error);
  }
}
