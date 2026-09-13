/**
 * Deterministic owner post-processing for the FAO transactions parser.
 *
 * The gazette grammar is `NAME, DOMICILE, inscrit le DATE`. The LLM is asked for
 * separate Name and City fields but sometimes returns:
 *   - the domicile glued into the name:   "AYOM SA, CHENE-BOUGERIES" (city empty)
 *   - the domicile as a separate party:   "CHENE-BOUGERIES" (a phantom owner)
 *   - name and city swapped:              name "Vernier", city "ROSSI Charles"
 *   - a public body cut to its locality:  "Genève" for "Ville de Genève"
 * Each variant fragments one real owner into several entities.
 *
 * Rules (mirror silver_ch.apply_entity_merge_decisions() on re-LLM):
 *   1. SPLIT, deterministic: if the text after the LAST comma is a known Swiss
 *      locality, the name is the head. The domicile goes into `city` only when
 *      `city` is empty (never overwrite an address). The original is kept in
 *      `name_raw`. A legal-form tail ("société anonyme", "en liquidation") is not
 *      a locality, so it is never split off.
 *   2. FLAG, never guess: an owner whose whole name IS a locality is not dropped
 *      and not repaired here — it can be a phantom, a swap or a truncated public
 *      body, and the right fix differs. It is flagged; the parser quarantines it
 *      and the run goes red.
 */

export type Owner = { name?: string | null; city?: string | null; date?: string | null; [k: string]: unknown };

export type OwnerFlag = {
  side: 'old_owner_s' | 'new_owner_s';
  index: number;          // 0-based position in the owner array
  reason: 'party_bare_locality';
  name: string;
  city: string | null;
};

const LEADING_PREPOSITION = /^(?:à|a|au|aux|in|im|von|zu|bei)\s+/i;

/**
 * Street / address words that are never a domicile, even when a Swiss commune shares
 * the name. Verified against silver_ch.ref_communes 2026-09-13: "Rue" (FR) and
 * "Port" (BE) are communes. The LLM sometimes cuts an address across both fields
 * — name "PAYCHERE Cédric,rue", city "de Genève 109 à Thônex" — and treating "rue"
 * as a domicile would, with an empty city, write city = "rue". Never guess.
 */
const STREET_WORDS = new Set([
  'rue', 'port', 'route', 'rte', 'chemin', 'ch', 'avenue', 'av', 'place', 'pl', 'quai', 'boulevard', 'bd',
  'pont', 'ruelle', 'impasse', 'allee', 'promenade', 'sentier', 'strasse', 'weg', 'gasse', 'platz',
]);
const TRAILING_CANTON_PAREN = /\s*\((?:[A-Z]{2})\)\s*$/i;

/** Accent-stripped, lower-cased, prepositions and canton markers removed. Matches DB lower(unaccent()). */
export function normalizePlace(s: string | null | undefined): string {
  if (!s) return '';
  return s
    .normalize('NFD').replace(/\p{Diacritic}/gu, '')
    .replace(/œ/gi, 'oe').replace(/æ/gi, 'ae').replace(/ß/g, 'ss')
    .toLowerCase()
    .trim()
    .replace(LEADING_PREPOSITION, '')
    .replace(TRAILING_CANTON_PAREN, '')
    .trim();
}

export function makeLocalityMatcher(normalizedNames: Iterable<string>): (s: string | null | undefined) => boolean {
  const set = new Set<string>();
  for (const n of normalizedNames) {
    const k = normalizePlace(n);
    if (k) set.add(k);
  }
  return (s) => {
    const k = normalizePlace(s);
    return k.length > 0 && set.has(k);
  };
}

/** Display form of a split-off domicile: preposition removed, original casing kept. */
function domicileDisplay(tail: string): string {
  return tail.trim().replace(LEADING_PREPOSITION, '').trim();
}

export function normalizeOwners(
  owners: unknown,
  side: 'old_owner_s' | 'new_owner_s',
  isLocality: (s: string | null | undefined) => boolean,
): { owners: Owner[] | null; flags: OwnerFlag[]; splits: number } {
  if (!Array.isArray(owners)) return { owners: (owners as Owner[] | null) ?? null, flags: [], splits: 0 };

  const flags: OwnerFlag[] = [];
  let splits = 0;
  const isDomicile = (s: string | null | undefined) => !STREET_WORDS.has(normalizePlace(s)) && isLocality(s);

  const out = owners.map((raw, index) => {
    if (raw === null || typeof raw !== 'object') return raw as Owner;
    const o: Owner = { ...(raw as Owner) };
    const name = typeof o.name === 'string' ? o.name.trim() : null;
    if (!name) return o;

    let current = name;
    const lastComma = name.lastIndexOf(',');
    if (lastComma > 0) {
      const head = name.slice(0, lastComma).trim();
      const tail = name.slice(lastComma + 1).trim();
      if (head && isDomicile(tail)) {
        o.name_raw = o.name_raw ?? name;
        o.name = head;
        const city = typeof o.city === 'string' ? o.city.trim() : '';
        if (!city) o.city = domicileDisplay(tail);
        current = head;
        splits++;
      }
    }

    if (isDomicile(current)) {
      flags.push({
        side, index, reason: 'party_bare_locality',
        name: current, city: typeof o.city === 'string' ? o.city : null,
      });
    }
    return o;
  });

  return { owners: out, flags, splits };
}
