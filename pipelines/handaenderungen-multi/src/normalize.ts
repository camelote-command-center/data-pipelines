/**
 * Normalize a ParsedRecord + PublicationRef into a bronze_ch.transactions_national row.
 *
 * Follows the fo_fr_ch (Fribourg) convention exactly: core columns carry what maps
 * 1:1; everything source-specific (grundbuchkreis, parcel_number, ownership_form,
 * quote, is_ownerless_event, per-party domiciles, raw_text) goes into raw_data jsonb.
 * price/price_per_m2/previous_transaction_date stay null by source design.
 */

import { createHash } from 'node:crypto';
import type { BronzeTxnRow, ParsedRecord, Party, PublicationRef } from './types.js';

/** "Nom Prénom, à Domicile" — the human-readable party rendering stored in buyers/sellers. */
function renderParties(parties: Party[]): string | null {
  if (!parties.length) return null;
  return parties
    .map((p) => (p.domicile ? `${p.names.join(' ')}, ${p.domicile}` : p.names.join(' ')))
    .join('; ');
}

/**
 * Stable natural key derived from the dedupe tuple
 * (canton, source_organ, parcel_number, buyer_names, publication_date).
 * Same logical record → same source_id → idempotent UPSERT on (source_id, canton).
 */
export function makeSourceId(rec: ParsedRecord, pub: PublicationRef): string {
  const buyerKey = rec.buyers.map((b) => b.names.join(' ')).join('|').toLowerCase();
  const material = [
    pub.canton,
    pub.source_organ,
    rec.parcel_number ?? '',
    buyerKey,
    pub.publication_date,
  ].join('§');
  const h = createHash('sha1').update(material).digest('hex').slice(0, 10);
  return `${pub.canton}-${pub.publication_date}-${rec.parcel_number ?? 'na'}-${h}`;
}

export function toBronzeRow(rec: ParsedRecord, pub: PublicationRef): BronzeTxnRow {
  return {
    source_id: makeSourceId(rec, pub),
    source_url: pub.source_url,
    transaction_date: pub.publication_date, // no true tx date is published; use publication date
    address: rec.address,
    reason: rec.is_ownerless_event ? 'aneignung' : 'handaenderung',
    property_type: rec.description,
    price: null,
    surface_m2: rec.surface_m2,
    price_per_m2: null,
    nb_buyers: rec.buyers.length || null,
    buyers: renderParties(rec.buyers),
    nb_sellers: rec.sellers.length || null,
    sellers: renderParties(rec.sellers),
    previous_transaction_date: null,
    canton: pub.canton,
    source_file: pub.source_organ,
    raw_data: {
      grundbuchkreis: pub.grundbuchkreis,
      issue: pub.issue,
      parcel_number: rec.parcel_number,
      ownership_form: rec.ownership_form,
      quote: rec.quote,
      stwe_wq: rec.stwe_wq,
      is_ownerless_event: rec.is_ownerless_event,
      seller_domicile: rec.sellers.map((s) => s.domicile),
      buyer_domicile: rec.buyers.map((b) => b.domicile),
      parse_method: rec.parse_method,
      raw_text: rec.raw_text,
    },
  };
}
