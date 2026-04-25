// Shared real-estate knowledge classifier.
// Same vocab, same prompt, same tool_choice as the backfill batch (file 02).
// Used by all knowledge_ch.* / knowledge_global.* / knowledge_ae.* ingest paths.
//
// Implementation note: raw fetch (not @anthropic-ai/sdk) to match existing edge
// functions in this repo (submit-batch, process-batch-results, extract-from-document
// all use raw fetch directly against api.anthropic.com).

const ANTHROPIC_API_KEY = Deno.env.get("ANTHROPIC_API_KEY");
const MODEL = "claude-sonnet-4-6";
const CONFIDENCE_THRESHOLD = 0.75;

export const ASSET_CLASSES = [
  "residential", "office", "retail", "industrial", "hospitality", "healthcare",
  "mixed_use", "specialty", "agricultural", "land", "virtual",
] as const;

export const TOPICS = [
  "legal", "zoning", "valuation", "transactions", "rental", "investment",
  "financing", "taxation", "construction", "operations", "sustainability",
  "ownership", "marketing", "technology",
] as const;

export const CHUNK_TYPES = [
  "definition", "rule", "case_law", "data_point", "formula", "procedure",
  "template", "example", "qa_pair", "commentary", "warning", "metadata",
] as const;

const SYSTEM_PROMPT = `You are a real estate knowledge classifier. Classify a piece of text from a real estate knowledge base across three orthogonal axes.

# CRITICAL: AXIS DISCIPLINE
The three axes are NOT interchangeable. Each value belongs to exactly ONE axis.
Before returning, validate every value against ONLY its axis's enum:
  - asset_classes[]  → ONLY values from AXIS 1 below
  - topics[]         → ONLY values from AXIS 2 below
  - chunk_type       → ONLY a value from AXIS 3 below

# COMMON CONFUSIONS TO AVOID
Several terms describe both a topic AND an asset class — the axis is fixed regardless of how the content "feels":
  - "retail", "residential", "office", "industrial", "land", "hospitality", "healthcare", "mixed_use", "specialty", "agricultural", "virtual" → ALWAYS asset_classes, NEVER topics. A chunk about "retail real estate investment" gets asset_classes=['retail'] and topics=['investment','transactions'] — never topics=['retail'].
  - "marketing", "construction", "operations" → ALWAYS topics, NEVER chunk_type or asset_classes. A marketing FAQ gets topics=['marketing'] and chunk_type='qa_pair' — never chunk_type='marketing'.
  - "metadata", "rule", "definition", "data_point", etc. → ALWAYS chunk_type, NEVER topics.
If a needed concept doesn't fit any axis, omit it from the structured fields and put it in tags (e.g., 'tourism' is not an axis value — use tags=['tourism'] alongside topics=['hospitality']).

# AXIS 1 — ASSET CLASSES (physical property types ONLY, multi-label)
Allowed: residential, office, retail, industrial, hospitality, healthcare, mixed_use, specialty, agricultural, land, virtual.
- residential: single-family, multifamily, apartments, PPE, social/senior/student housing
- office: corporate, coworking, medical office, professional services
- retail: shops, malls, F&B, standalone restaurants
- industrial: warehouses, logistics, manufacturing, distribution, cold storage, flex
- hospitality: hotels, B&B, short-term rentals, serviced apartments
- healthcare: hospitals, clinics, EMS, senior care, life sciences buildings
- mixed_use: combinations not dominated by one class
- specialty: data centers, self-storage, entertainment, religious, civic, cemeteries, prisons, military, sports
- agricultural: farms, vineyards, forests, agricultural land
- land: undeveloped plots, building rights without buildings
- virtual: metaverse, tokenized RWA, NFT-deeded digital assets

If the knowledge applies generically across all asset classes (e.g. general contract law, civil code preamble), return an empty array.

# AXIS 2 — TOPICS (knowledge domains ONLY, multi-label, at least one)
Allowed: legal, zoning, valuation, transactions, rental, investment, financing, taxation, construction, operations, sustainability, ownership, marketing, technology.
Order by relevance.

# AXIS 3 — CHUNK TYPE (kind of statement ONLY, single-label)
Allowed: definition, rule, case_law, data_point, formula, procedure, template, example, qa_pair, commentary, warning, metadata.
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
1–5 short lowercase snake_case tags (e.g. 'lex_weber', 'minergie', 'cap_rate', 'ius_zone5').

# CONFIDENCE
Honest 0–1 self-assessment. ≥ 0.85 only when unambiguous.

# REASONING
1–2 sentences, in the source language.

Always output via the classify tool.`;

const TOOL = {
  name: "classify",
  description: "Classify a real estate knowledge item across the three taxonomy axes.",
  input_schema: {
    type: "object" as const,
    required: ["topics", "chunk_type", "confidence", "reasoning"],
    properties: {
      asset_classes: { type: "array", items: { type: "string", enum: ASSET_CLASSES } },
      topics:        { type: "array", minItems: 1, items: { type: "string", enum: TOPICS } },
      chunk_type:    { type: "string", enum: CHUNK_TYPES },
      tags:          { type: "array", maxItems: 5, items: { type: "string", pattern: "^[a-z0-9_]+$" } },
      confidence:    { type: "number", minimum: 0, maximum: 1 },
      reasoning:     { type: "string" },
    },
  },
};

export type Classification = {
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

export async function classify(input: ClassifyInput): Promise<Classification | null> {
  if (!ANTHROPIC_API_KEY) {
    console.error("classify: ANTHROPIC_API_KEY not set");
    return null;
  }
  try {
    const resp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: 1024,
        system: SYSTEM_PROMPT,
        tools: [TOOL],
        tool_choice: { type: "tool", name: "classify" },
        messages: [{ role: "user", content: renderUserMessage(input) }],
      }),
    });
    if (!resp.ok) {
      console.error("classify api error:", resp.status, await resp.text());
      return null;
    }
    const json = await resp.json();
    for (const block of json.content ?? []) {
      if (block.type === "tool_use" && block.name === "classify") {
        const c = block.input as Omit<Classification, "status">;
        return {
          ...c,
          asset_classes: c.asset_classes ?? null,
          tags: c.tags ?? null,
          status: c.confidence >= CONFIDENCE_THRESHOLD ? "auto" : "needs_review",
        };
      }
    }
    return null;
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
