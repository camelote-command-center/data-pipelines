import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.56.0";
import { getCorsHeaders, handleCorsPreflightRequest } from "../_shared/cors.ts";

const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const anthropicKey = Deno.env.get("ANTHROPIC_API_KEY")!;

const supabase = createClient(supabaseUrl, supabaseKey);

const VALID_CATEGORIES = [
  "marketing", "clients", "finances", "hr", "legal",
  "management", "operations", "products", "sales",
  "service_providers", "strategy", "tech", "ai",
] as const;

const VALID_TYPES = ["task", "bug", "idea", "note"] as const;
const VALID_PRIORITIES = ["P1", "P2", "P3", "P4"] as const;

interface EnrichmentResult {
  category: string | null;
  sub_category: string | null;
  type: string;
  priority: string;
  tags: string[];
  startup_name: string | null;
}

interface StartupCtx {
  id: string;
  name: string;
  tagline: string | null;
  description: string | null;
  keywords: string[];
}

// Strip diacritics + lowercase, so "estimation" matches "Estimation" and "Genève" matches "geneve".
function normalize(s: string): string {
  return s.normalize("NFD").replace(/\p{Diacritic}/gu, "").toLowerCase();
}

// Deterministic startup match: scan task text for any startup keyword (or name).
// Returns the best match (longest keyword wins; ties broken by first hit).
function matchStartupByKeywords(
  text: string,
  startups: StartupCtx[],
): StartupCtx | null {
  const t = ` ${normalize(text)} `;
  let best: { s: StartupCtx; len: number } | null = null;
  for (const s of startups) {
    // Build the candidate set: normalized name + each keyword
    const candidates = [normalize(s.name), ...s.keywords.map(normalize)];
    for (const c of candidates) {
      if (!c) continue;
      // Word-boundary match using surrounding spaces / punctuation
      const padded = ` ${c} `;
      if (t.includes(padded) || t.includes(` ${c}:`) || t.includes(` ${c},`) || t.includes(` ${c}.`)) {
        if (!best || c.length > best.len) {
          best = { s, len: c.length };
        }
      }
    }
  }
  return best?.s ?? null;
}

async function enrichWithClaude(
  title: string,
  description: string | null,
  startups: StartupCtx[],
  preMatchedStartup: string | null,
): Promise<EnrichmentResult> {
  // Compact startup catalogue for the prompt — name, tagline, keywords.
  const catalogue = startups
    .map((s) => {
      const parts = [`- ${s.name}`];
      if (s.tagline) parts.push(`(${s.tagline})`);
      const kws = (s.keywords || []).slice(0, 12).join(", ");
      if (kws) parts.push(`— terms: ${kws}`);
      return parts.join(" ");
    })
    .join("\n");

  const startupGuidance = preMatchedStartup
    ? `The task text contains a known feature/term of "${preMatchedStartup}", so set startup_name="${preMatchedStartup}" unless something in the task explicitly contradicts that.`
    : `Infer the startup from ANY product, feature, internal term, domain, or vocabulary mentioned — not only when the startup's name is spelled out. If you cannot infer with reasonable confidence, set startup_name=null.`;

  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": anthropicKey,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: "claude-sonnet-4-20250514",
      max_tokens: 300,
      messages: [
        {
          role: "user",
          content: `You are a task classifier for a startup portfolio manager. Given a task title and optional description, classify it.

Available startups (each shown with tagline and characteristic terms — match by ANY of these):
${catalogue}

${startupGuidance}

Available categories: ${VALID_CATEGORIES.join(", ")}

Available types: task (action to do), bug (something broken to fix), idea (something to explore later), note (just information to remember)

Priorities: P1 (urgent/blocking), P2 (important), P3 (normal), P4 (someday/nice-to-have)

Sub-categories are free-form but common ones include: UI/UX, SEO, Google Indexing, Server & Hosting, Tickets, Content, Social Media, Analytics, Blog, Email, Branding, Pricing, Onboarding, Legal Compliance, Accounting, Payroll, Recruitment, Contracts, Data Pipeline, API, Database, DevOps, Infrastructure

Respond ONLY with valid JSON, no markdown:
{"category": "tech", "sub_category": "UI/UX", "type": "bug", "priority": "P2", "tags": ["frontend", "mobile"], "startup_name": "Lamap"}

If you can't determine a field, use null for category/sub_category/startup_name, "task" for type, "P3" for priority, [] for tags.

Task title: ${title}
${description ? `Description: ${description.slice(0, 500)}` : ""}`,
        },
      ],
    }),
  });

  if (!response.ok) {
    console.error("Claude API error:", response.status, await response.text());
    return { category: null, sub_category: null, type: "task", priority: "P3", tags: [], startup_name: preMatchedStartup };
  }

  const data = await response.json();
  const text = data.content[0].text.trim();

  try {
    const parsed = JSON.parse(text);
    if (parsed.category && !VALID_CATEGORIES.includes(parsed.category)) parsed.category = null;
    if (parsed.type && !VALID_TYPES.includes(parsed.type)) parsed.type = "task";
    if (parsed.priority && !VALID_PRIORITIES.includes(parsed.priority)) parsed.priority = "P3";
    if (!Array.isArray(parsed.tags)) parsed.tags = [];
    // If pre-match found a startup but the AI dropped it, restore it.
    if (preMatchedStartup && !parsed.startup_name) parsed.startup_name = preMatchedStartup;
    return parsed;
  } catch {
    console.error("Failed to parse Claude response:", text);
    return { category: null, sub_category: null, type: "task", priority: "P3", tags: [], startup_name: preMatchedStartup };
  }
}

serve(async (req) => {
  const preflightResponse = handleCorsPreflightRequest(req);
  if (preflightResponse) return preflightResponse;
  const corsHeaders = getCorsHeaders(req);

  try {
    const body = await req.json().catch(() => ({}));
    const taskIds: string[] | undefined = body.task_ids;
    const limit: number = body.limit || 20;
    const force: boolean = body.force === true;

    const { data: startupsRaw } = await supabase
      .from("startups")
      .select("id, name, tagline, description, keywords");
    const startups: StartupCtx[] = (startupsRaw || []).map((s: any) => ({
      id: s.id,
      name: s.name,
      tagline: s.tagline,
      description: s.description,
      keywords: Array.isArray(s.keywords) ? s.keywords : [],
    }));
    const startupByName = new Map(startups.map((s) => [s.name.toLowerCase(), s]));

    let query = supabase
      .from("tasks")
      .select("id, title, description, startup_id, category, type, priority")
      .order("created_at", { ascending: false })
      .limit(limit);

    if (taskIds?.length) {
      query = supabase
        .from("tasks")
        .select("id, title, description, startup_id, category, type, priority")
        .in("id", taskIds);
    } else if (!force) {
      query = query.eq("ai_enriched", false);
    }

    const { data: tasks, error: fetchError } = await query;
    if (fetchError) throw fetchError;
    if (!tasks?.length) {
      return new Response(
        JSON.stringify({ message: "No tasks to enrich", enriched: 0 }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } },
      );
    }

    let enriched = 0;
    const errors: string[] = [];
    const debug: any[] = [];

    for (const task of tasks) {
      try {
        const fullText = `${task.title || ""}\n${task.description || ""}`;
        const preMatch = matchStartupByKeywords(fullText, startups);
        const result = await enrichWithClaude(
          task.title,
          task.description,
          startups,
          preMatch?.name ?? null,
        );

        const update: Record<string, unknown> = { ai_enriched: true, ai_metadata: result };

        if (!task.category && result.category) update.category = result.category;
        if (result.sub_category) update.sub_category = result.sub_category;
        if (!task.type || task.type === "task") update.type = result.type;
        if (!task.priority || task.priority === "P3") update.priority = result.priority;
        if (result.tags?.length) update.tags = result.tags;

        // Resolve startup_name → startup_id. Prefer the AI's pick; fall back to deterministic keyword match.
        if (!task.startup_id || force) {
          let resolvedId: string | undefined;
          if (result.startup_name) {
            resolvedId = startupByName.get(result.startup_name.toLowerCase())?.id;
          }
          if (!resolvedId && preMatch) resolvedId = preMatch.id;
          if (resolvedId) update.startup_id = resolvedId;
        }

        const { error: updateError } = await supabase.from("tasks").update(update).eq("id", task.id);

        if (updateError) errors.push(`Task ${task.id}: ${updateError.message}`);
        else enriched++;

        debug.push({
          id: task.id,
          title: task.title,
          pre_match: preMatch?.name ?? null,
          ai_startup: result.startup_name,
          assigned_startup_id: update.startup_id ?? null,
        });
      } catch (e) {
        errors.push(`Task ${task.id}: ${e.message}`);
      }
    }

    return new Response(
      JSON.stringify({
        enriched,
        total: tasks.length,
        debug: body.debug ? debug : undefined,
        errors: errors.length ? errors : undefined,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (e) {
    return new Response(
      JSON.stringify({ error: e.message }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  }
});
