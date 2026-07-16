/**
 * Per-source adapter contract. The ingest core is canton-parameterized; each adapter
 * owns exactly how a source organ is discovered and fetched. Parse/normalize/flag/load
 * are shared and adapter-independent.
 */

import type { Canton, RawRecord } from '../types.js';

export interface SourceAdapter {
  canton: Canton;
  /** Organ slug, becomes BronzeTxnRow.source_file (e.g. 'amtsblatt_sz'). */
  organ: string;

  /**
   * Yield raw Handänderung text blocks with their publication metadata.
   * NOTE: every current source is access-gated (robots Disallow / subscription-only) —
   * see README. The live fetch path is intentionally a stub that throws until a
   * compliant source (PDF-Abo inbox, granted API, or consent) is wired in.
   *
   * @param opts.fixtureDir when set, read committed fixtures instead of the network —
   *        this is how the parser is exercised end-to-end today.
   */
  discover(opts: { fixtureDir?: string; since?: string }): Promise<RawRecord[]>;
}

/** Thrown by an adapter's live path while the source remains access-gated. */
export class AccessGatedError extends Error {
  constructor(organ: string, detail: string) {
    super(`[${organ}] live fetch not wired: ${detail}`);
    this.name = 'AccessGatedError';
  }
}
