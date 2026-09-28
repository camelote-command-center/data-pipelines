/**
 * Handänderung record parser.
 *
 * Primary path: a deterministic regex parser for the well-formed
 *   "<sellers> an <buyers>, Nr. <parcel>, <address>, <description>, <surface|WQ>"
 * grammar shared by SG/SZ/LU gazettes. Deterministic ⇒ unit-testable without the LLM.
 *
 * Fallback path: Claude Sonnet (claude-sonnet-5) for blocks the regex can't confidently
 * split — same DeepSeek/Sonnet-fallback shape as pipelines/transactions-fao, but the
 * primary here is regex (the grammar is far more regular than the FAO free text).
 */

import Anthropic from '@anthropic-ai/sdk';
import type { ParsedRecord, Party } from './types.js';

// ---------------------------------------------------------------------------
// Swiss number / quote helpers
// ---------------------------------------------------------------------------

/** "1'243" | "1’243" | "46 053" → 46053 ; null if no digits. */
export function parseSwissNumber(s: string | null | undefined): number | null {
  if (!s) return null;
  const digits = s.replace(/['’\s.]/g, '');
  if (!/^\d+$/.test(digits)) return null;
  return Number(digits);
}

const OWNERSHIP_MARKERS: Array<[RegExp, string]> = [
  [/\bStWE-WQ\b/i, 'StWE'],
  [/\bStWE\b/i, 'StWE'],
  [/\bMiteigentum\b|\bME\b/i, 'ME'],
  [/\bGesamteigentum\b|\bGE\b/i, 'GE'],
  [/\bBaurecht\b|\bBR\b/i, 'BR'],
  [/\bSonderrecht\b|\bSR\b/i, 'SR'],
];

/** Extract "1/2" / "100/1'000" style quotes. */
function extractQuote(raw: string): string | null {
  const m = raw.match(/(\d[\d'’]*\s*\/\s*\d[\d'’]*)/);
  return m ? m[1].replace(/\s+/g, '') : null;
}

function detectOwnershipForm(raw: string): string | null {
  for (const [re, form] of OWNERSHIP_MARKERS) if (re.test(raw)) return form;
  return null;
}

// ---------------------------------------------------------------------------
// Party parsing
// ---------------------------------------------------------------------------

/**
 * Split a party segment into {names, domicile} pairs.
 * Handles "Name, Domicile" and multi-party "A, Dom1, und B, Dom2, ME zu je 1/2".
 * Ownership markers/quotes are stripped here (surfaced separately by the caller).
 */
export function parseParties(raw: string): Party[] {
  const cleaned = raw
    .replace(/\bME zu je [\d'’/]+/gi, '')
    .replace(/\b(ME|GE|StWE(?:-WQ)?|BR|SR)\b[^,]*/gi, '')
    .replace(/\s{2,}/g, ' ')
    .trim();

  // Co-owners are joined with "und"; each is "Name, Domicile".
  const chunks = cleaned.split(/\s+und\s+/i).map((c) => c.trim()).filter(Boolean);
  const parties: Party[] = [];
  for (const chunk of chunks) {
    const parts = chunk.split(',').map((p) => p.trim()).filter(Boolean);
    if (parts.length === 0) continue;
    // Last comma-part is the domicile when there are ≥2 parts; else unknown.
    const domicile = parts.length >= 2 ? parts[parts.length - 1] : null;
    const names = (parts.length >= 2 ? parts.slice(0, -1) : parts).filter(Boolean);
    if (names.length) parties.push({ names, domicile });
  }
  return parties;
}

// ---------------------------------------------------------------------------
// Tail parsing (after "Nr. <parcel>,")
// ---------------------------------------------------------------------------

interface Tail {
  address: string | null;
  description: string | null;
  surface_m2: number | null;
  stwe_wq: string | null;
}

function parseTail(tail: string): Tail {
  const segments = tail.split(',').map((s) => s.trim()).filter(Boolean);
  const wqMatch = tail.match(/StWE-WQ\s*([\d'’]+\s*\/\s*[\d'’]+)/i);
  const stwe_wq = wqMatch ? wqMatch[1].replace(/\s+/g, '') : null;
  const surfMatch = tail.match(/([\d'’.\s]+)\s*m2\b/i);
  const surface_m2 = surfMatch ? parseSwissNumber(surfMatch[1]) : null;

  // First segment = address/Flurname; middle segments = description.
  const address = segments.length ? segments[0] : null;
  const descSegs = segments
    .slice(1)
    .filter((s) => !/m2\b/i.test(s) && !/StWE-WQ/i.test(s));
  const description = descSegs.length ? descSegs.join(', ') : null;

  return { address, description, surface_m2, stwe_wq };
}

// ---------------------------------------------------------------------------
// Regex primary parser
// ---------------------------------------------------------------------------

/** Splits on ", Nr. <parcel>," then on the first alienation pivot " an ". */
export function parseBlockRegex(text: string): ParsedRecord {
  const raw_text = text.trim();
  const base: ParsedRecord = {
    sellers: [],
    buyers: [],
    parcel_number: null,
    address: null,
    description: null,
    surface_m2: null,
    stwe_wq: null,
    ownership_form: null,
    quote: null,
    is_ownerless_event: false,
    raw_text,
    parse_method: 'regex',
    parse_confidence: 'high',
  };

  // Anchor on the Grundstück-Nr.
  const nrMatch = raw_text.match(/,?\s*Nr\.\s*([\dA-Za-z\-/.]+)\s*,?/);
  if (!nrMatch) return { ...base, parse_confidence: 'low' };
  const parcel_number = nrMatch[1];
  const left = raw_text.slice(0, nrMatch.index).trim();
  const tail = raw_text.slice((nrMatch.index ?? 0) + nrMatch[0].length).trim();

  // Alienator → acquirer pivot: first standalone " an " in the party segment.
  const anIdx = left.search(/\s+an\s+/);
  if (anIdx === -1) return { ...base, parcel_number, parse_confidence: 'low' };
  const sellerRaw = left.slice(0, anIdx).trim().replace(/,+$/, '');
  const buyerRaw = left.slice(anIdx).replace(/^\s+an\s+/, '').trim();

  const sellers = parseParties(sellerRaw);
  const buyers = parseParties(buyerRaw);
  const t = parseTail(tail);

  const ownership_form =
    detectOwnershipForm(buyerRaw) ?? detectOwnershipForm(sellerRaw) ?? (t.stwe_wq ? 'StWE' : null);
  const quote = extractQuote(buyerRaw) ?? extractQuote(sellerRaw) ?? t.stwe_wq;

  const confident = sellers.length > 0 && buyers.length > 0;
  return {
    ...base,
    sellers,
    buyers,
    parcel_number,
    address: t.address,
    description: t.description,
    surface_m2: t.surface_m2,
    stwe_wq: t.stwe_wq,
    ownership_form,
    quote,
    parse_confidence: confident ? 'high' : 'low',
  };
}

// ---------------------------------------------------------------------------
// LLM fallback (Claude Sonnet)
// ---------------------------------------------------------------------------

const MODEL = process.env.HANDAENDERUNGEN_MODEL ?? 'claude-sonnet-5';

const LLM_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    sellers: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          names: { type: 'array', items: { type: 'string' } },
          domicile: { type: ['string', 'null'] },
        },
        required: ['names', 'domicile'],
      },
    },
    buyers: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          names: { type: 'array', items: { type: 'string' } },
          domicile: { type: ['string', 'null'] },
        },
        required: ['names', 'domicile'],
      },
    },
    parcel_number: { type: ['string', 'null'] },
    address: { type: ['string', 'null'] },
    description: { type: ['string', 'null'] },
    surface_m2: { type: ['number', 'null'] },
    stwe_wq: { type: ['string', 'null'] },
    ownership_form: { type: ['string', 'null'] },
    quote: { type: ['string', 'null'] },
  },
  required: [
    'sellers',
    'buyers',
    'parcel_number',
    'address',
    'description',
    'surface_m2',
    'stwe_wq',
    'ownership_form',
    'quote',
  ],
} as const;

const SYSTEM = `You extract structured data from a single Swiss cantonal land-register transfer notice (Handänderung).
The grammar is: <alienator(s) + domicile> "an" <acquirer(s) + domicile>, Nr. <Grundstück-Nr>, <address/Flurname>, <description>, <surface m2 OR StWE-WQ quote>.
Abbreviations: ME=Miteigentum, GE=Gesamteigentum, StWE=Stockwerkeigentum, StWE-WQ=Wertquote, BR=Baurecht, SR=Sonderrecht.
Rules: never invent a price (none is published). ownership_form ∈ {ME,GE,StWE,BR,SR,Alleineigentum}. surface_m2 is an integer (Swiss thousands separators removed). Return only the fields in the schema.`;

/** Parse one block with the LLM. Returns null on API error so the caller can quarantine. */
export async function parseBlockLLM(
  text: string,
  client: Anthropic,
): Promise<ParsedRecord | null> {
  try {
    const resp = await client.messages.create({
      model: MODEL,
      max_tokens: 1024,
      system: SYSTEM,
      output_config: { format: { type: 'json_schema', schema: LLM_SCHEMA } },
      messages: [{ role: 'user', content: text.trim() }],
    } as Anthropic.MessageCreateParams);
    const block = resp.content.find((b) => b.type === 'text');
    if (!block || block.type !== 'text') return null;
    const j = JSON.parse(block.text);
    return {
      sellers: j.sellers ?? [],
      buyers: j.buyers ?? [],
      parcel_number: j.parcel_number ?? null,
      address: j.address ?? null,
      description: j.description ?? null,
      surface_m2: j.surface_m2 ?? null,
      stwe_wq: j.stwe_wq ?? null,
      ownership_form: j.ownership_form ?? null,
      quote: j.quote ?? null,
      is_ownerless_event: false, // set downstream by flagOwnerless()
      raw_text: text.trim(),
      parse_method: 'llm',
      parse_confidence: 'high',
    };
  } catch {
    return null;
  }
}

/**
 * Parse one block: regex first, LLM fallback when regex is low-confidence and a
 * client is available. Returns null only if both fail (⇒ quarantine).
 */
export async function parseBlock(
  text: string,
  client: Anthropic | null,
): Promise<ParsedRecord | null> {
  const regex = parseBlockRegex(text);
  if (regex.parse_confidence === 'high' || !client) return regex;
  const llm = await parseBlockLLM(text, client);
  return llm ?? regex; // keep the low-confidence regex row rather than dropping data
}
