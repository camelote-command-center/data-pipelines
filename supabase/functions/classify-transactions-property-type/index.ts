// classify-transactions-property-type — DeepSeek V3 batch classifier for the
// property_type column on gold_ch.core_transactions. Writes to
// silver_ch.transactions_property_type_llm. The gold matview LEFT JOINs that
// table and COALESCEs the rules CASE → LLM result.
//
// Scope: property_type only. Do NOT add other axes. Do NOT modify
// gold_ch.core_transactions structure here.
//
// Input (POST body, all fields optional):
//   {
//     "target_canton": "GE",   // filter; default = all NULLs
//     "limit": 50,             // batch size; default 50, max 200
//     "dry_run": false,        // if true, classify and return but do NOT write
//     "transaction_ids": [...] // reclassify these rows (max 200) and OVERWRITE
//                              // their existing llm row; ignores target_canton
//   }
//
// PPE guard (owner rule, 2026-11-14): a sale is PPE only when the notice text
// says so (word « PPE » or a PPE unit number like « 4.06 appartement ») or the
// plot is in the PPE register and the text doesn't sell the whole plot. The
// gold rules already type all of those "apartment", so a row reaching this
// classifier may only be "apartment" when ppe_indicator/LDTR is true or the text
// carries that evidence. Otherwise the model is asked once more with a note,
// and a second "apartment" becomes "other". COP (copropriété ordinaire) and
// B-F (bien-fonds) are never apartment by themselves.
//
// Idempotency: rows already present in silver_ch.transactions_property_type_llm
// are excluded by the query. Inserts use ON CONFLICT (transaction_id) DO NOTHING
// as a second safety net for concurrent invocations.
//
// Vocabulary (11-value enum, enforced by table CHECK and re-validated here):
//   apartment, house, building, commercial, parking, land, agricultural,
//   secondary, shared, DDP, other
//   NOTE: DDP is the only value with non-lowercase canonical casing (uppercase).

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.4";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const DEEPSEEK_API_KEY = Deno.env.get("DEEPSEEK_API_KEY");
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
const MODEL = "deepseek-chat";

const ALLOWED_TYPES = [
  "apartment", "house", "building", "commercial", "parking",
  "land", "agricultural", "secondary", "shared", "DDP", "other",
] as const;
type PropertyType = typeof ALLOWED_TYPES[number];
const ALLOWED_SET = new Set<string>(ALLOWED_TYPES);

// Same evidence rule as gold_ch.core_transactions and pipelines/transactions-fao/ppe.ts.
const PPE_UNITS =
  "appartement|studio|attique|duplex|triplex|loft|box|parking|place|garage|cave|grenier|bureaux?|local|locaux|arcade|dépôt|depot|chambre|hall|magasin|atelier|cabinet|salle|restaurant|réduit";
const PPE_EVIDENCE = new RegExp(
  `\\bPPE\\b|\\bUE\\s*\\d|(?<![\\d.])\\d{1,2}\\.\\d{2,3}\\s+(?:partie\\s+)?(?:${PPE_UNITS})(?![\\p{L}])`,
  "iu",
);

export function apartmentAllowed(r: Pick<CandidateRow, "ppe_indicator" | "is_ldtr" | "transaction_text">): boolean {
  return r.ppe_indicator === true || r.is_ldtr === true || PPE_EVIDENCE.test(r.transaction_text ?? "");
}

const GUARD_NOTE =
  "Note: this text has NO PPE evidence (no word « PPE », no PPE unit number) and PPE indicator is false. " +
  "So this is NOT an apartment: the whole plot (B-F) or an ordinary co-ownership share (COP) of it is sold. " +
  "Choose the type of what stands on the plot (house, building, commercial, parking, land, …). Return JSON only.";

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

type CandidateRow = {
  transaction_id: string;
  commune: string | null;
  canton_code: string | null;
  transaction_type: string | null;
  price: number | null;
  surface_m2: number | null;
  ppe_indicator: boolean | null;
  ppe_pourmille_raw: string | null;
  transaction_text: string | null;
  buildings_raw: unknown;
  is_ldtr: boolean | null;
  source_table: string | null;
  address: string | null;
};

type LLMResult = {
  property_type: PropertyType;
  confidence: number;
  reasoning: string;
};

function jsonResp(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function buildSystemPrompt(): string {
  return `You classify a Swiss real-estate transaction into EXACTLY ONE of 11 property types.

# OUTPUT FORMAT
Respond with a single JSON object: { "property_type": <one of the 10 keys>, "confidence": <0..1>, "reasoning": "<1-2 sentences, in French or English>" }.
Return ONLY the JSON object. No prose before or after.

# VOCABULARY (canonical, exactly these 11 keys)
- apartment      → single PPE unit (appartement/studio/attique/loft/duplex en PPE). LDTR units count as apartment. ONLY when the text says « PPE » or names a PPE unit number (e.g. « 4.06 appartement », « UE 2.05 »), or PPE indicator / LDTR is true.
- house          → detached/semi-detached single-family or two-family house (maison individuelle, villa, chalet, maison mitoyenne, maison jumelle).
- building       → entire multi-family building sold as a whole (immeuble locatif, immeuble de rendement, multi-family ≥3 units, no PPE split).
- commercial     → office/retail/industrial/hospitality (bureau, arcade, local commercial, restaurant, hôtel, hangar, usine, atelier, dépôt). Includes industrial use.
- parking        → garage box, parking space, place de parc, place de stationnement. Usually very low price (<200k) and small surface.
- land           → unbuilt parcel / building plot (terrain à bâtir, parcelle non bâtie, terrain vierge, bois et forêt non agricole).
- agricultural   → agricultural land or rural farm property (terrain agricole, exploitation agricole, ferme).
- secondary      → secondary residence / résidence secondaire (only when explicitly flagged; otherwise treat as apartment or house based on form).
- shared         → fractional / time-share / co-living arrangement (rare; only when text explicitly indicates time-share or co-living). A COP share is NOT "shared": classify it by what stands on the plot.
- DDP            → droit distinct et permanent / droit de superficie (building right: you own the building, not the land beneath it). Output as uppercase "DDP". Use only when the text explicitly references a DDP / droit de superficie.
- other          → anything that does not fit any of the above (e.g. mixed-use without dominant component, undetermined). Use sparingly.

# DECISION GUIDANCE
- The CASE rules upstream already caught every PPE sale (word PPE or a PPE unit number in the text, or the plot is in the PPE register) plus LDTR / villa / immeuble locatif / typology hints. Rows reaching you are AMBIGUOUS and, unless PPE indicator is true, they are NOT PPE units — do not answer "apartment" for them.
- "B-F" in transaction_text means "bien-fonds": the WHOLE plot is sold (with whatever stands on it). Classify by the buildings on the plot: house, building, commercial, parking, land…
- "COP" / "copropriété" / "part de copropriété" / a fraction like « 1/2 » or « 283/1000 » of a B-F means an ordinary co-ownership share of the whole plot (copropriété ordinaire), NOT a PPE unit. Classify by what stands on the plot, never "apartment".
- A /1000 share or a "PPE pourmille" number alone is NOT evidence of PPE (it is often a COP share).
- Price-range heuristic for Geneva (cross-check, not deciding):
  parking ≪ 200k; apartment 500k–3M; house 1M–8M; building 5M–50M+; commercial wide.
- If multiple buildings on one parcel and no PPE indicator → likely "building".
- If you genuinely cannot tell, classify by the dominant building on the plot (see below); otherwise "other" with confidence < 0.6. Never fall back to "apartment".
- confidence ≥ 0.85 only when one signal is unambiguous (e.g. "villa", "immeuble locatif", "place de parc N°", "PPE", explicit single-family house language).
- confidence 0.6–0.84 for indirect inference (commune type + price range + buyer profile).
- confidence < 0.6 means "other" or your best guess with low certainty.
- If the only structures on a parcel are utility installations (SIG, water, gas, electricity, transformer, cabine technique), or the structured building list is present but empty, or the text says the plot is unbuilt (non bâti, terrain, sans bâtiment) → "land", not "other".
- When building data is unknown and the text gives no use for the plot, answer "other" (confidence < 0.6) — do NOT guess "land" or "parking" from the price alone.
- If a parcel has MULTIPLE buildings of different uses, classify by the building with the LARGEST surface_m2 (e.g., 289m² house + 73m² multi-logement + garages → "house", because the 289m² house dominates). Only use "other" when no single building dominates AND no use category fits.
- Reserve "other" for genuinely uncategorizable cases (e.g., chapels, monuments, hangars not fitting commercial, fractional shares without context). When in doubt between "other" and any concrete category, prefer the concrete category and lower confidence to <0.6.

# EXAMPLES
text: "Achat. Anciens: X. Nouveaux: Y. Place de parc n°12, sous-sol, parcelle 1234"
→ {"property_type":"parking","confidence":0.93,"reasoning":"Place de parc explicitly numbered, sous-sol context."}

text: "Achat. Maison individuelle avec terrain, 6 pièces"
→ {"property_type":"house","confidence":0.95,"reasoning":"Maison individuelle clearly stated."}

text: "Achat. Parcelle 2345 B-F, PPE, lot 4.06 appartement, 52/1000."
→ {"property_type":"apartment","confidence":0.94,"reasoning":"PPE explicitly stated with unit number 4.06 appartement."}

text: "Achat. 1/2 COP de la parcelle 3456 B-F de Vernier. Bâtiment: habitation un logement."
→ {"property_type":"house","confidence":0.8,"reasoning":"Ordinary co-ownership share (COP) of a whole plot with a single-family house; not PPE."}

text: "Achat. Immeuble de rendement, 8 appartements + 2 arcades"
→ {"property_type":"building","confidence":0.94,"reasoning":"Whole multi-family income building."}

text: "Achat. Parcelle 4567, surface 2500m2, zone agricole"
→ {"property_type":"agricultural","confidence":0.92,"reasoning":"Zone agricole."}

text: "Achat. Bureau et arcade rue du Rhône"
→ {"property_type":"commercial","confidence":0.93,"reasoning":"Bureau + arcade commerciale."}`;
}

function buildUserMessage(r: CandidateRow): string {
  // Trim transaction_text aggressively — these notarial blurbs are mostly party names.
  // Keep first ~1500 chars which carry commune, price, transaction type, building/lot refs.
  const text = (r.transaction_text ?? "").slice(0, 1500);
  const buildingsHint = r.buildings_raw
    ? `buildings (structured): ${JSON.stringify(r.buildings_raw).slice(0, 600)}`
    : "buildings (structured): unknown — no building data (this does NOT mean the plot is unbuilt)";
  return [
    `Transaction ID: ${r.transaction_id}`,
    `Canton: ${r.canton_code ?? "?"}  Commune: ${r.commune ?? "?"}`,
    `Type: ${r.transaction_type ?? "?"}  Source: ${r.source_table ?? "?"}`,
    `Price (CHF): ${r.price ?? "?"}  Surface (m²): ${r.surface_m2 ?? "?"}`,
    `PPE indicator: ${r.ppe_indicator}  LDTR: ${r.is_ldtr}  Share (‰, not PPE evidence — may be a COP share): ${r.ppe_pourmille_raw ?? "none"}`,
    `Address: ${r.address ?? "?"}`,
    buildingsHint,
    `---`,
    `transaction_text:`,
    text,
    `---`,
    `Classify this transaction. Return JSON only.`,
  ].join("\n");
}

async function sha256Hex(s: string): Promise<string> {
  const buf = new TextEncoder().encode(s);
  const hash = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function callDeepSeek(systemPrompt: string, userMsg: string): Promise<LLMResult | null> {
  const resp = await fetch("https://api.deepseek.com/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${DEEPSEEK_API_KEY!}`,
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: 256,
      temperature: 0,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userMsg },
      ],
      response_format: { type: "json_object" },
    }),
  });
  if (!resp.ok) {
    console.error("deepseek api error:", resp.status, (await resp.text()).slice(0, 400));
    return null;
  }
  const json = await resp.json();
  const content = json?.choices?.[0]?.message?.content;
  if (!content || typeof content !== "string") {
    console.error("deepseek: empty content");
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(content);
  } catch (err) {
    console.error("deepseek: JSON parse failed:", err, content.slice(0, 200));
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const o = parsed as Record<string, unknown>;
  // Canonical casing: DDP is the only uppercase canonical value; everything else is lowercase.
  const ptRaw = typeof o.property_type === "string" ? o.property_type.trim() : "";
  const pt = ptRaw.toUpperCase() === "DDP" ? "DDP" : ptRaw.toLowerCase();
  const conf = typeof o.confidence === "number" ? o.confidence : Number(o.confidence);
  const reasoning = typeof o.reasoning === "string" ? o.reasoning : "";
  if (!ALLOWED_SET.has(pt)) {
    console.error("deepseek: invalid property_type:", pt);
    return null;
  }
  if (!Number.isFinite(conf) || conf < 0 || conf > 1) {
    console.error("deepseek: invalid confidence:", conf);
    return null;
  }
  return { property_type: pt as PropertyType, confidence: conf, reasoning };
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }
  if (!DEEPSEEK_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    return jsonResp({ ok: false, error: "missing env: DEEPSEEK_API_KEY / SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY" }, 500);
  }

  let body: Record<string, unknown> = {};
  if (req.method === "POST") {
    body = (await req.json().catch(() => ({}))) as Record<string, unknown>;
  }

  const targetCanton = typeof body.target_canton === "string" && body.target_canton.length > 0
    ? body.target_canton : null;
  const dryRun = body.dry_run === true;
  const ids = Array.isArray(body.transaction_ids)
    ? (body.transaction_ids as unknown[]).filter((x): x is string => typeof x === "string" && x.length > 0)
    : [];
  const reclassify = ids.length > 0;
  if (ids.length > MAX_LIMIT) return jsonResp({ ok: false, error: `transaction_ids: max ${MAX_LIMIT}` }, 400);
  let limit = Number(body.limit ?? DEFAULT_LIMIT);
  if (!Number.isFinite(limit) || limit <= 0) limit = DEFAULT_LIMIT;
  if (limit > MAX_LIMIT) limit = MAX_LIMIT;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // Fetch candidates via server-side anti-join RPC.
  // The RPC silver_ch.next_unclassified_transactions joins core_transactions LEFT JOIN
  // transactions_property_type_llm and returns the next `limit` rows ordered by
  // transaction_id where property_type IS NULL and llm row absent. This makes the
  // function self-paginating across concurrent invocations: each call always gets
  // the next un-classified rows, regardless of how many have already landed.

  // Reclassify mode: the listed rows, whatever their current llm row
  // (silver_ch.transactions_by_ids returns the same columns).
  const { data: candidates, error: candErr } = reclassify
    ? await supabase.schema("silver_ch").rpc("transactions_by_ids", { p_ids: ids })
    : await supabase
      .schema("silver_ch")
      .rpc("next_unclassified_transactions", {
        p_canton: targetCanton,
        p_limit: limit,
      });
  if (candErr) return jsonResp({ ok: false, error: `candidate fetch: ${candErr.message}` }, 500);

  const todo = ((candidates ?? []) as CandidateRow[]).slice(0, reclassify ? MAX_LIMIT : limit);
  if (todo.length === 0) {
    return jsonResp({ ok: true, classified: 0, skipped: 0, errors: 0, exhausted: true, target_canton: targetCanton, message: "no NULL candidates remain for the given filter" });
  }

  const systemPrompt = buildSystemPrompt();
  const results: Array<{ transaction_id: string; result: LLMResult | null; hash: string; guarded: boolean }> = [];

  // Parallel calls with bounded concurrency. DeepSeek tolerates parallelism well;
  // 8 in-flight keeps throughput high while staying within typical RL budgets.
  // ~1.5s wall-time per slot × ceil(100/8) ≈ 20s for a 100-row batch.
  const CONCURRENCY = 8;
  let cursor = 0;
  async function worker() {
    while (true) {
      const idx = cursor++;
      if (idx >= todo.length) return;
      const r = todo[idx];
      const userMsg = buildUserMessage(r);
      const hash = await sha256Hex(`${MODEL}|${r.transaction_id}|${userMsg}`);
      try {
        let out = await callDeepSeek(systemPrompt, userMsg);
        let guarded = false;
        if (out?.property_type === "apartment" && !apartmentAllowed(r)) {
          guarded = true;
          const retry = await callDeepSeek(systemPrompt, `${userMsg}\n${GUARD_NOTE}`);
          out = retry && retry.property_type !== "apartment"
            ? { ...retry, reasoning: `guard retry: ${retry.reasoning}` }
            : { property_type: "other", confidence: 0.3, reasoning: "guard: model said apartment without PPE evidence" };
        }
        results.push({ transaction_id: r.transaction_id, result: out, hash, guarded });
      } catch (err) {
        console.error("classify err for", r.transaction_id, err);
        results.push({ transaction_id: r.transaction_id, result: null, hash, guarded: false });
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, todo.length) }, () => worker()));

  const ok = results.filter((x) => x.result !== null);
  const failed = results.length - ok.length;
  const guardedCount = results.filter((x) => x.guarded).length;
  const dist: Record<string, number> = {};
  for (const x of ok) dist[x.result!.property_type] = (dist[x.result!.property_type] ?? 0) + 1;

  if (dryRun) {
    return jsonResp({
      ok: true,
      dry_run: true,
      reclassify,
      target_canton: targetCanton,
      requested: todo.length,
      classified: ok.length,
      errors: failed,
      guarded: guardedCount,
      distribution: dist,
      results: results.map((x) => ({
        transaction_id: x.transaction_id,
        property_type: x.result?.property_type ?? null,
        confidence: x.result?.confidence ?? null,
        reasoning: x.result?.reasoning ?? null,
      })),
    });
  }

  if (ok.length === 0) {
    return jsonResp({ ok: false, classified: 0, errors: failed, target_canton: targetCanton, message: "all classifier calls failed" }, 502);
  }

  const rows = ok.map((x) => ({
    transaction_id: x.transaction_id,
    property_type: x.result!.property_type,
    confidence: x.result!.confidence,
    reasoning: x.result!.reasoning,
    model: MODEL,
    input_text_hash: x.hash,
    ...(reclassify ? { classified_at: new Date().toISOString() } : {}),
  }));

  const { error: insErr } = await supabase
    .schema("silver_ch")
    .from("transactions_property_type_llm")
    .upsert(rows, { onConflict: "transaction_id", ignoreDuplicates: !reclassify });

  if (insErr) {
    return jsonResp({ ok: false, error: `insert: ${insErr.message}`, attempted: rows.length }, 500);
  }

  return jsonResp({
    ok: true,
    reclassify,
    guarded: guardedCount,
    target_canton: targetCanton,
    requested: todo.length,
    classified: rows.length,
    errors: failed,
    distribution: dist,
    sample: rows.slice(0, 5).map((r) => ({
      transaction_id: r.transaction_id,
      property_type: r.property_type,
      confidence: r.confidence,
    })),
  });
});
