/**
 * Schwyz adapter — Amtsblatt des Kantons Schwyz (cantonal, weekly, issue-numbered).
 *
 * Recon (see README §SZ): robots.txt on amtsblatt.sz.ch is `Disallow: /` — generic
 * scraping is not permitted. The platform offers a "Gesamtausgabe als PDF" subscription
 * and a Filter/Such-Abo (email intervals); the compliant path is to ingest that delivered
 * PDF, not to crawl. Handänderungen land at CANTONAL level (KA 16/25 confirms: private
 * Aneignungen of herrenlose Grundstücke are published here), not in Bezirk organs.
 *
 * Legal: SZ EGZGB has no reversion clause ⇒ sans-maître flag is live.
 */

import { join } from 'node:path';
import type { RawRecord } from '../types.js';
import { AccessGatedError, type SourceAdapter } from './types.js';
import { loadFixtureDir } from './fixtures.js';

export const schwyzAdapter: SourceAdapter = {
  canton: 'SZ',
  organ: 'amtsblatt_sz',

  async discover(opts): Promise<RawRecord[]> {
    if (opts.fixtureDir) {
      return loadFixtureDir(join(opts.fixtureDir, 'schwyz'), 'SZ', 'amtsblatt_sz');
    }
    // LIVE PATH — wire once a compliant source exists (Ilan action):
    //   1. Set up the "Gesamtausgabe als PDF" Abo (or a "Handänderungen" Filter-Abo)
    //      on amtsblatt.sz.ch → delivered to a dedicated ingest inbox.
    //   2. Fetch the delivered PDF from that inbox, layout-extract the Handänderungen
    //      rubric, and return { pub, text } per record.
    // Do NOT scrape amtsblatt.sz.ch directly (robots Disallow:/).
    throw new AccessGatedError(
      'amtsblatt_sz',
      'amtsblatt.sz.ch robots Disallow:/ — needs the Gesamtausgabe-PDF/Filter-Abo inbox ingest',
    );
  },
};
