// Knowledge classifier — v2 (multi-domain).
//
// Adds AXIS 0 = domain (24 values, mutually exclusive). AXIS 1 (asset_classes) and
// AXIS 2 (topics) are now domain-conditional: their valid keys depend on the chosen
// domain. AXIS 3 (chunk_type) stays domain-agnostic.
//
// Vocabularies are fetched once at cold start from `knowledge_global.{domains, topics,
// asset_classes, chunk_types}` and cached for the life of the function instance.
//
// Implementation note: raw fetch (not @anthropic-ai/sdk) to match existing edge
// functions in this repo.

const ANTHROPIC_API_KEY = Deno.env.get("ANTHROPIC_API_KEY");
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
const MODEL = "claude-sonnet-4-6";
const CONFIDENCE_THRESHOLD = 0.75;

// ---- Vocab loading & cache ----------------------------------------------------

type DomainRow = { key: string; label_en: string; label_fr: string; description_fr: string | null };
type TopicRow = { key: string; domain: string; label_en: string; label_fr: string };
type AssetClassRow = { key: string; domain: string; label_en: string; label_fr: string };
type ChunkTypeRow = { key: string };

type Vocab = {
  domains: DomainRow[];
  topicsByDomain: Map<string, TopicRow[]>;
  assetClassesByDomain: Map<string, AssetClassRow[]>;
  chunkTypes: string[];
};

let _vocabPromise: Promise<Vocab> | null = null;

async function loadVocab(): Promise<Vocab> {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("classifier loadVocab: SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set");
  }
  const headers = {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    "Accept-Profile": "knowledge_global",
  };
  const get = async (path: string) => {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, { headers });
    if (!r.ok) throw new Error(`vocab fetch ${path} ${r.status}: ${await r.text()}`);
    return r.json();
  };

  const [domains, topics, assetClasses, chunkTypes]: [
    DomainRow[], TopicRow[], AssetClassRow[], ChunkTypeRow[]
  ] = await Promise.all([
    get("domains?select=key,label_en,label_fr,description_fr&is_active=eq.true&order=sort_order"),
    get("topics?select=key,domain,label_en,label_fr&is_active=eq.true"),
    get("asset_classes?select=key,domain,label_en,label_fr&is_active=eq.true"),
    get("chunk_types?select=key&is_active=eq.true"),
  ]);

  const topicsByDomain = new Map<string, TopicRow[]>();
  for (const t of topics) {
    const arr = topicsByDomain.get(t.domain) ?? [];
    arr.push(t);
    topicsByDomain.set(t.domain, arr);
  }
  const assetClassesByDomain = new Map<string, AssetClassRow[]>();
  for (const a of assetClasses) {
    const arr = assetClassesByDomain.get(a.domain) ?? [];
    arr.push(a);
    assetClassesByDomain.set(a.domain, arr);
  }
  return {
    domains,
    topicsByDomain,
    assetClassesByDomain,
    chunkTypes: chunkTypes.map(c => c.key),
  };
}

function getVocab(): Promise<Vocab> {
  if (!_vocabPromise) _vocabPromise = loadVocab();
  return _vocabPromise;
}

// ---- Prompt + tool schema -----------------------------------------------------

function buildSystemPrompt(v: Vocab): string {
  const domainList = v.domains
    .map(d => `  - ${d.key}: ${d.description_fr ?? d.label_fr}`)
    .join("\n");

  const perDomainTopics: Record<string, string[]> = {};
  for (const [d, ts] of v.topicsByDomain) {
    perDomainTopics[d] = ts.map(t => t.key).sort();
  }
  const perDomainAssetClasses: Record<string, string[]> = {};
  for (const [d, acs] of v.assetClassesByDomain) {
    perDomainAssetClasses[d] = acs.map(a => a.key).sort();
  }

  return `You are a knowledge classifier for a multi-domain knowledge base. Classify a piece of text across four orthogonal axes.

# CRITICAL: AXIS DISCIPLINE
The four axes are NOT interchangeable. Each value belongs to exactly ONE axis.
Before returning, validate every value against ONLY its axis's enum:
  - domain         → ONLY a value from AXIS 0 below
  - asset_classes[]→ ONLY values from AXIS 1, AND only valid for the chosen domain
  - topics[]       → ONLY values from AXIS 2 that belong to the chosen domain
  - chunk_type     → ONLY a value from AXIS 3 (domain-agnostic)

Topics are domain-scoped. Don't put a real_estate topic under domain='health'.
Don't put a health topic under domain='real_estate'.

# AXIS 0 — DOMAIN (single-label, mutually exclusive, REQUIRED)
Pick exactly one. Definitions in French (the priority audience is Geneva/lamap.ch):
${domainList}

# AXIS 1 — ASSET CLASSES (multi-label, OPTIONAL, only meaningful when domain='real_estate')
Asset classes are physical real-estate property types. For ANY domain other than
'real_estate' set asset_classes to an empty array. Valid keys per domain:
${JSON.stringify(perDomainAssetClasses, null, 2)}

If knowledge applies generically across all real-estate asset classes (e.g. general
contract law preamble), return an empty array even when domain='real_estate'.

# AXIS 2 — TOPICS (multi-label, REQUIRED, at least one)
Topics are domain-scoped knowledge facets. ONLY pick keys from the list under your
chosen domain. Order by relevance. Per-domain valid keys:
${JSON.stringify(perDomainTopics, null, 2)}

# AXIS 3 — CHUNK TYPE (single-label, REQUIRED, domain-agnostic)
Allowed: ${v.chunkTypes.join(", ")}.
- definition: a term and its meaning
- rule: statute, regulation, normative requirement
- case_law: court decisions, jurisprudence
- data_point: a number, statistic, fact, benchmark value
- formula: a calculation or equation
- procedure: step-by-step how-to
- template: model contracts, sample clauses, forms
- example: concrete instance illustrating a rule or method
- qa_pair: explicit Q&A from FAQ-style sources
- commentary: analysis, opinion, interpretation
- warning: caveats, restrictions, exceptions, "attention!" notes
- metadata: titles, ToC, headers (skip-this-at-retrieval marker)
If unclear, prefer 'commentary'.

# TAGS
1–5 short lowercase snake_case tags (e.g. 'lex_weber', 'minergie', 'cap_rate', 'ius_zone5', 'tpg', 'urgences').

# CONFIDENCE
Honest 0–1 self-assessment. ≥ 0.85 only when unambiguous.

# REASONING
1–2 sentences, in the source language.

Always output via the classify tool.`;
}

function buildTool(v: Vocab) {
  return {
    name: "classify",
    description: "Classify a knowledge item across the four taxonomy axes (domain + asset_classes + topics + chunk_type).",
    input_schema: {
      type: "object" as const,
      required: ["domain", "topics", "chunk_type", "confidence", "reasoning"],
      properties: {
        domain:        { type: "string", enum: v.domains.map(d => d.key) },
        asset_classes: { type: "array",  items: { type: "string" } },
        topics:        { type: "array", minItems: 1, items: { type: "string" } },
        chunk_type:    { type: "string", enum: v.chunkTypes },
        tags:          { type: "array", maxItems: 5, items: { type: "string", pattern: "^[a-z0-9_]+$" } },
        confidence:    { type: "number", minimum: 0, maximum: 1 },
        reasoning:     { type: "string" },
      },
    },
  };
}

// ---- Types --------------------------------------------------------------------

export type Classification = {
  domain: string;
  asset_classes: string[] | null;
  topics: string[];
  chunk_type: string;
  tags: string[] | null;
  confidence: number;
  reasoning: string;
  status: "auto" | "needs_review";
};

export type ClassifyInput = {
  kind: "entry" | "document" | "chunk";
  title?: string | null;
  content: string;
  doc_title?: string | null;
  section_title?: string | null;
  language?: string | null;
  country?: string | null;
};

// ---- Anthropic call -----------------------------------------------------------

type RawClassification = Omit<Classification, "status">;

async function callAnthropic(
  v: Vocab,
  input: ClassifyInput,
  systemPrompt: string,
  correctiveNote?: string,
): Promise<RawClassification | null> {
  const userMsg = renderUserMessage(input) +
    (correctiveNote ? `\n\n[CORRECTION] ${correctiveNote}` : "");

  const resp = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": ANTHROPIC_API_KEY!,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: 1024,
      system: systemPrompt,
      tools: [buildTool(v)],
      tool_choice: { type: "tool", name: "classify" },
      messages: [{ role: "user", content: userMsg }],
    }),
  });
  if (!resp.ok) {
    console.error("classify api error:", resp.status, await resp.text());
    return null;
  }
  const json = await resp.json();
  for (const block of json.content ?? []) {
    if (block.type === "tool_use" && block.name === "classify") {
      return block.input as RawClassification;
    }
  }
  return null;
}

// ---- Server-side validation of classifier output ------------------------------

type ValidationResult = { ok: true } | { ok: false; reason: string };

function validateAgainstVocab(v: Vocab, c: RawClassification): ValidationResult {
  if (!c.domain || !v.topicsByDomain.has(c.domain)) {
    return { ok: false, reason: `unknown domain: ${c.domain}` };
  }
  const validTopics = new Set(v.topicsByDomain.get(c.domain)!.map(t => t.key));
  const badTopics = (c.topics ?? []).filter(t => !validTopics.has(t));
  if (badTopics.length > 0) {
    return { ok: false, reason: `topics not in domain ${c.domain}: ${badTopics.join(",")}` };
  }
  if (c.asset_classes && c.asset_classes.length > 0) {
    const validACs = new Set((v.assetClassesByDomain.get(c.domain) ?? []).map(a => a.key));
    const badACs = c.asset_classes.filter(a => !validACs.has(a));
    if (badACs.length > 0) {
      return { ok: false, reason: `asset_classes not in domain ${c.domain}: ${badACs.join(",")}` };
    }
  }
  if (!v.chunkTypes.includes(c.chunk_type)) {
    return { ok: false, reason: `unknown chunk_type: ${c.chunk_type}` };
  }
  return { ok: true };
}

// ---- Public API ---------------------------------------------------------------

export async function classify(input: ClassifyInput): Promise<Classification | null> {
  if (!ANTHROPIC_API_KEY) {
    console.error("classify: ANTHROPIC_API_KEY not set");
    return null;
  }
  try {
    const v = await getVocab();
    const systemPrompt = buildSystemPrompt(v);

    let raw = await callAnthropic(v, input, systemPrompt);
    if (!raw) return null;

    let validation = validateAgainstVocab(v, raw);
    if (!validation.ok) {
      console.warn("classify: invalid output, retrying once:", validation.reason);
      raw = await callAnthropic(
        v, input, systemPrompt,
        `Your previous response was invalid: ${validation.reason}. Re-classify, picking topics ONLY from the per-domain vocab.`,
      );
      if (!raw) return null;
      validation = validateAgainstVocab(v, raw);
    }

    // Final shape — even if still invalid, send back as needs_review
    const stillInvalid = !validation.ok;
    let asset_classes = raw.asset_classes ?? [];
    // Strip asset_classes for any non-real_estate domain (rule, not error)
    if (raw.domain !== "real_estate") asset_classes = [];

    const conf = Math.max(0, Math.min(1, Number(raw.confidence) || 0));
    return {
      domain: raw.domain,
      asset_classes: asset_classes.length > 0 ? asset_classes : null,
      topics: raw.topics ?? [],
      chunk_type: raw.chunk_type,
      tags: raw.tags ?? null,
      confidence: conf,
      reasoning: raw.reasoning,
      status: (!stillInvalid && conf >= CONFIDENCE_THRESHOLD) ? "auto" : "needs_review",
    };
  } catch (err) {
    console.error("classify error:", err);
    return null;
  }
}

function renderUserMessage(i: ClassifyInput): string {
  if (i.kind === "entry") {
    return [
      `Item type: knowledge entry`,
      `Title: ${i.title ?? "(no title)"}`,
      `Country: ${i.country ?? "?"}`,
      `Language: ${i.language ?? "?"}`,
      `---`,
      i.content.slice(0, 4000),
      `---`,
      `Classify per the system instructions.`,
    ].join("\n");
  }
  if (i.kind === "document") {
    return [
      `Item type: document (whole-document classification)`,
      `Title: ${i.title ?? "(no title)"}`,
      `Language: ${i.language ?? "?"}`,
      `Country: ${i.country ?? "?"}`,
      `---`,
      i.content.slice(0, 3000),
      `---`,
      `Classify the document as a whole.`,
    ].join("\n");
  }
  return [
    `Item type: document chunk`,
    `Parent document: ${i.doc_title ?? "?"}`,
    `Section: ${i.section_title ?? "(none)"}`,
    `Language: ${i.language ?? "?"}`,
    `---`,
    i.content.slice(0, 4000),
    `---`,
    `Classify per the system instructions.`,
  ].join("\n");
}
