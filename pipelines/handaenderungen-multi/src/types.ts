/**
 * Handänderungen (cantonal property-transfer notices) — shared types.
 *
 * One canton-parameterized ingest core, one adapter per source organ.
 * Cantons in scope: LU, SZ. VS is a documented NO-GO (see README §VS) — no adapter.
 */

/** Cantons this parser lands rows for. Disjoint from SG (paused) and BE (succession_events). */
export type Canton = 'LU' | 'SZ';

/** A single alienator or acquirer party block (may hold several co-owners). */
export interface Party {
  names: string[];
  domicile: string | null;
}

/**
 * A parsed Handänderung record, source-organ-agnostic.
 * `price` is intentionally absent — no cantonal gazette publishes the consideration.
 */
export interface ParsedRecord {
  sellers: Party[];
  buyers: Party[];
  parcel_number: string | null;
  address: string | null;
  description: string | null;
  surface_m2: number | null;
  /** Stockwerkeigentum-Wertquote, e.g. "100/1'000". */
  stwe_wq: string | null;
  /** ME | GE | StWE | BR | SR | Alleineigentum */
  ownership_form: string | null;
  /** Miteigentum/StWE quote, e.g. "1/2". */
  quote: string | null;
  is_ownerless_event: boolean;
  raw_text: string;
  parse_method: 'regex' | 'llm';
  /** 'low' routes the block to the LLM fallback when a client is available. */
  parse_confidence: 'high' | 'low';
}

/**
 * A row shaped for re-LLM `bronze_ch.transactions_national` (20 cols; id/created_at/
 * updated_at are DB-managed). Identical schema to lamap_db `ref.transactions_national`,
 * so bronze IS the FDW load surface — mirrors the `fo_fr_ch` (Fribourg) convention.
 * Natural key: UNIQUE(source_id, canton).
 */
export interface BronzeTxnRow {
  source_id: string;
  source_url: string | null;
  /** publication_date (ISO yyyy-mm-dd) or null. */
  transaction_date: string | null;
  address: string | null;
  reason: string | null;
  property_type: string | null;
  price: null; // never published
  surface_m2: number | null;
  price_per_m2: null; // no price ⇒ no per-m²
  nb_buyers: number | null;
  buyers: string | null; // text, "Name, domicile" joined by "; "
  nb_sellers: number | null;
  sellers: string | null;
  previous_transaction_date: null;
  canton: Canton;
  source_file: string; // organ slug, e.g. 'amtsblatt_sz'
  raw_data: Record<string, unknown>;
}

/** One Handänderungen publication located by an adapter. */
export interface PublicationRef {
  canton: Canton;
  source_organ: string; // slug, becomes BronzeTxnRow.source_file
  source_url: string | null;
  publication_date: string; // ISO yyyy-mm-dd
  issue: string | null; // e.g. "12" (SZ Amtsblatt-Nr.) / kreis-quarter id
  grundbuchkreis: string | null;
}

/** A raw Handänderung text block plus the publication it came from. */
export interface RawRecord {
  pub: PublicationRef;
  text: string;
}
