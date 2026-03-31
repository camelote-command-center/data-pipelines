/**
 * FR Feuille Officielle — Shared Parsing Utilities
 *
 * Parses transaction text and building permit text from the Feuille officielle
 * du canton de Fribourg. Text is scraped from fo.fr.ch article pages.
 *
 * The parser is intentionally lenient: fields that cannot be extracted are set
 * to null, and the raw text is always preserved in raw_data for later
 * refinement.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ParsedTransaction {
  sellers: string;
  buyers: string;
  commune: string;
  parcel_numbers: string;
  address: string;
  property_type: string | null;
  surface_m2: number | null;
  previous_date: string | null;
  raw_text: string;
}

export interface ParsedPermit {
  applicant: string | null;
  description: string | null;
  commune: string;
  parcel_number: string | null;
  address: string | null;
  raw_text: string;
}

// ---------------------------------------------------------------------------
// Date parsing
// ---------------------------------------------------------------------------

/**
 * Parse a date string into ISO YYYY-MM-DD format.
 * Handles: DD.MM.YYYY, DD.MM.YY, YYYY-MM-DD
 */
export function parseDate(s: string): string | null {
  if (!s) return null;
  const trimmed = s.trim();

  // Already ISO
  const isoMatch = trimmed.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) return trimmed;

  // DD.MM.YYYY
  const dotMatch = trimmed.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (dotMatch) {
    const day = dotMatch[1].padStart(2, '0');
    const month = dotMatch[2].padStart(2, '0');
    const year = dotMatch[3];
    return `${year}-${month}-${day}`;
  }

  // DD.MM.YY
  const shortMatch = trimmed.match(/^(\d{1,2})\.(\d{1,2})\.(\d{2})$/);
  if (shortMatch) {
    const day = shortMatch[1].padStart(2, '0');
    const month = shortMatch[2].padStart(2, '0');
    const yearShort = parseInt(shortMatch[3], 10);
    const year = yearShort >= 50 ? `19${shortMatch[3]}` : `20${shortMatch[3]}`;
    return `${year}-${month}-${day}`;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Property type detection
// ---------------------------------------------------------------------------

const PROPERTY_TYPE_PATTERNS: [RegExp, string][] = [
  [/\bPPE\b/, 'PPE'],
  [/\bCOP\b/, 'COP'],
  [/\bB-F\b/, 'B-F'],
  [/\bterrain\s+b[aâ]ti\b/i, 'terrain bati'],
  [/\bterrain\s+non\s+b[aâ]ti\b/i, 'terrain non bati'],
  [/\bhabitation\s+individuelle\b/i, 'habitation individuelle'],
  [/\bappartement\b/i, 'appartement'],
  [/\bvilla\b/i, 'villa'],
  [/\bimmeuble\b/i, 'immeuble'],
  [/\brurale?\b/i, 'rural'],
  [/\bgarage\b/i, 'garage'],
  [/\bplace\s+de\s+parc\b/i, 'place de parc'],
  [/\bchalet\b/i, 'chalet'],
  [/\bparcelle\b/i, 'parcelle'],
  [/\bpart\s+de\s+copropri[eé]t[eé]\b/i, 'COP'],
  [/\bEigentumswohnung\b/i, 'PPE'],
  [/\bStockwerkeigentum\b/i, 'PPE'],
];

function detectPropertyType(text: string): string | null {
  for (const [pattern, type] of PROPERTY_TYPE_PATTERNS) {
    if (pattern.test(text)) return type;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Surface extraction
// ---------------------------------------------------------------------------

function extractSurface(text: string): number | null {
  // Match patterns like "1'234 m2", "1234 m²", "1 234m2"
  const match = text.match(
    /(\d[\d'\u2019\s]*\d)\s*m[2²]|(\d+)\s*m[2²]/,
  );
  if (!match) return null;
  const raw = (match[1] || match[2]).replace(/['\u2019\s]/g, '');
  const value = parseInt(raw, 10);
  return isNaN(value) ? null : value;
}

// ---------------------------------------------------------------------------
// Previous date extraction
// ---------------------------------------------------------------------------

function extractPreviousDate(text: string): string | null {
  // "(acquis le DD.MM.YYYY)" or "(erworben am DD.MM.YYYY)"
  const match = text.match(
    /\((?:acquis|erworben)\s+(?:le|am)\s+(\d{1,2}\.\d{1,2}\.\d{2,4})\)/i,
  );
  if (!match) return null;
  return parseDate(match[1]);
}

// ---------------------------------------------------------------------------
// Parcel number extraction
// ---------------------------------------------------------------------------

function extractParcelNumbers(text: string): string {
  // Match "No 496" or "No 1234" or "art. 123-456" style parcels
  const matches: string[] = [];

  // "No XXX" pattern (most common)
  const noMatches = text.matchAll(/No\s+(\d[\d\-\/]*)/g);
  for (const m of noMatches) {
    matches.push(`No ${m[1]}`);
  }

  // "art. X-Y PPE" pattern
  const artMatches = text.matchAll(/art\.\s*([\d\-]+)\s*PPE/g);
  for (const m of artMatches) {
    matches.push(`art. ${m[1]} PPE`);
  }

  return matches.join(', ') || '';
}

// ---------------------------------------------------------------------------
// Address extraction
// ---------------------------------------------------------------------------

function extractAddress(text: string): string {
  // Look for street patterns after parcel number: "No 496, route du Tot 13"
  const match = text.match(
    /No\s+\d[\d\-\/]*(?:\s*PPE)?[,;]\s*([^;]+?)(?:;\s*terrain|;\s*habitation|;\s*appartement|;\s*place|;\s*garage|;\s*immeuble|;\s*chalet|;\s*villa|;\s*sol|;\s*part|;\s*Wohn|;\s*Gebäude|$)/i,
  );
  if (match) {
    return match[1].trim().replace(/,$/, '');
  }
  return '';
}

// ---------------------------------------------------------------------------
// Transaction block splitting and parsing
// ---------------------------------------------------------------------------

/**
 * Split raw article text into individual transaction blocks.
 * Each transaction starts with "Alienateur" (FR) or "Verausserer" (DE).
 */
function splitTransactionBlocks(text: string): string[] {
  // Split on the start of each transaction entry
  // French: "Aliénateur", "Aliénatrice", "Aliénateurs"
  // German: "Veräusserer", "Veräusserin"
  const pattern =
    /(?=(?:Ali[eé]nateur(?:s|trice)?|Ver[aä]usserer(?:in)?)\s*:)/gi;
  const blocks = text.split(pattern).filter((b) => b.trim().length > 0);
  return blocks;
}

/**
 * Extract sellers from a transaction block.
 * French: text between "Aliénateur(s):" and "acquéreur(s):"
 * German: text between "Veräusserer/in:" and "Erwerber/in:"
 */
function extractSellers(block: string): string {
  const match = block.match(
    /(?:Ali[eé]nateur(?:s|trice)?|Ver[aä]usserer(?:in)?)\s*:\s*([\s\S]*?)(?:;\s*(?:acqu[eé]reur|Erwerber))/i,
  );
  if (match) {
    return match[1]
      .replace(/\s+/g, ' ')
      .trim()
      .replace(/;$/, '')
      .trim();
  }
  return '';
}

/**
 * Extract buyers from a transaction block.
 * French: text between "acquéreur(s):" and first ";"
 * German: text between "Erwerber/in:" and first ";"
 */
function extractBuyers(block: string): string {
  const match = block.match(
    /(?:acqu[eé]reur(?:s|e|es)?|Erwerber(?:in)?)\s*:\s*([\s\S]*?);\s*(?:No\s+\d|art\.\s*\d)/i,
  );
  if (match) {
    return match[1]
      .replace(/\s+/g, ' ')
      .trim()
      .replace(/;$/, '')
      .trim();
  }

  // Fallback: grab everything after acquéreur/Erwerber up to the first semicolon
  const fallback = block.match(
    /(?:acqu[eé]reur(?:s|e|es)?|Erwerber(?:in)?)\s*:\s*([^;]+)/i,
  );
  if (fallback) {
    return fallback[1].trim();
  }

  return '';
}

/**
 * Count the number of distinct individuals in a name string.
 * Splits on " et " / " und " / "," to count names.
 */
function countNames(nameStr: string): number {
  if (!nameStr) return 0;
  // Split on " et ", " und ", comma, or semicolon separators
  const parts = nameStr
    .split(/\s+et\s+|\s+und\s+|,\s*/)
    .map((p) => p.trim())
    .filter((p) => p.length > 0);
  // Each "part" that contains a name (has at least one letter) counts
  return parts.filter((p) => /[a-zA-ZÀ-ÿ]/.test(p)).length || 1;
}

/**
 * Parse a single transaction block into a ParsedTransaction.
 */
function parseOneTransaction(
  block: string,
  commune: string,
): ParsedTransaction {
  const sellers = extractSellers(block);
  const buyers = extractBuyers(block);
  const parcelNumbers = extractParcelNumbers(block);
  const address = extractAddress(block);
  const propertyType = detectPropertyType(block);
  const surface = extractSurface(block);
  const previousDate = extractPreviousDate(block);

  return {
    sellers,
    buyers,
    commune,
    parcel_numbers: parcelNumbers,
    address,
    property_type: propertyType,
    surface_m2: surface,
    previous_date: previousDate,
    raw_text: block.trim(),
  };
}

/**
 * Parse the full text of an article page into ParsedTransaction[].
 * The text typically has a commune header ("Commune de X") followed
 * by one or more transaction blocks.
 */
export function parseTransactionBlock(
  text: string,
  commune: string,
): ParsedTransaction[] {
  const blocks = splitTransactionBlocks(text);
  if (blocks.length === 0) return [];

  return blocks.map((block) => parseOneTransaction(block, commune));
}

/**
 * Extract the commune name from an article text.
 * Looks for "Commune de X" or "Gemeinde X" patterns.
 */
export function extractCommune(text: string): string | null {
  const match = text.match(/(?:Commune\s+de|Gemeinde)\s+([^\n\r]+)/i);
  if (match) return match[1].trim();
  return null;
}

/**
 * Count buyers in a parsed transaction.
 */
export function countBuyers(parsed: ParsedTransaction): number {
  return countNames(parsed.buyers);
}

/**
 * Count sellers in a parsed transaction.
 */
export function countSellers(parsed: ParsedTransaction): number {
  return countNames(parsed.sellers);
}

// ---------------------------------------------------------------------------
// Building permit parsing
// ---------------------------------------------------------------------------

/**
 * Parse building permit text from a category 21 article.
 * Permit text structure varies, so this is very lenient.
 */
export function parseBuildingPermit(
  text: string,
  commune: string,
): ParsedPermit {
  let applicant: string | null = null;
  let description: string | null = null;
  let parcelNumber: string | null = null;
  let address: string | null = null;

  // Try to extract applicant
  // Patterns: "Requérant(e)(s):", "Bauherr(schaft):", "Maître de l'ouvrage:"
  const applicantMatch = text.match(
    /(?:Requ[eé]rant(?:e|s)?|Bauherr(?:schaft)?|Ma[iî]tre\s+de\s+l['']ouvrage)\s*:\s*([^;\n]+)/i,
  );
  if (applicantMatch) {
    applicant = applicantMatch[1].trim();
  }

  // Try to extract description
  // Patterns: "Objet:", "Gegenstand:", "Nature des travaux:"
  const descMatch = text.match(
    /(?:Objet|Gegenstand|Nature\s+des\s+travaux)\s*:\s*([^;\n]+)/i,
  );
  if (descMatch) {
    description = descMatch[1].trim();
  }

  // Parcel number
  const parcelMatch = text.match(/(?:No|Parzelle|parcelle)\s+(\d[\d\-\/]*)/i);
  if (parcelMatch) {
    parcelNumber = parcelMatch[1];
  }

  // Address
  const addrMatch = text.match(
    /(?:Lieu|Ort|Adresse|Situation)\s*:\s*([^;\n]+)/i,
  );
  if (addrMatch) {
    address = addrMatch[1].trim();
  }

  // If no structured description found, use a cleaned version of the full text
  if (!description) {
    // Take the first meaningful line as description (skip headers)
    const lines = text
      .split('\n')
      .map((l) => l.trim())
      .filter((l) => l.length > 10);
    if (lines.length > 0) {
      description = lines.slice(0, 3).join(' ').substring(0, 500);
    }
  }

  return {
    applicant,
    description,
    commune,
    parcel_number: parcelNumber,
    address,
    raw_text: text.trim(),
  };
}
