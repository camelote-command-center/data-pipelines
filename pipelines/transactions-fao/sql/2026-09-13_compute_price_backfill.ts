/**
 * Compute the price backfill with the parser's OWN validateParsedPrice(), so
 * every backfilled value is identical to what the fixed parser writes at runtime.
 * No LLM is invoked: the LLM input is passed as null, text is already stored.
 *
 *   npx tsx sql/2026-09-13_compute_price_backfill.ts <in.json> <out.json>
 *   in.json : [{ id, affaire, text }]  keyed by PK id — affaire_number is NOT unique  (bronze rows with an empty price)
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { validateParsedPrice } from '../price.js';

const [, , inPath, outPath] = process.argv;
const rows: { id: number; affaire: string | null; text: string }[] = JSON.parse(readFileSync(inPath, 'utf8'));
const out = { backfill: [] as { id: number; affaire: string | null; price: string }[],
              quarantine: [] as { id: number; affaire: string | null; reason: string; fragment: string }[],
              no_price_in_text: 0, implausible: [] as string[] };
for (const r of rows) {
  const v = validateParsedPrice(r.text, null);
  if (!v.ok) { out.implausible.push(String(r.id)); continue; }
  if (v.price !== null) out.backfill.push({ id: r.id, affaire: r.affaire, price: v.price });
  else if (v.unextracted) out.quarantine.push({ id: r.id, affaire: r.affaire, ...v.unextracted });
  else out.no_price_in_text++;
}
writeFileSync(outPath, JSON.stringify(out, null, 2));
console.log(`backfill=${out.backfill.length} quarantine=${out.quarantine.length} ` +
            `no_price_in_text=${out.no_price_in_text} implausible=${out.implausible.length}`);
