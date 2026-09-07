/**
 * Lucerne adapter — Luzerner Kantonsblatt (weekly, Sat), rubric «Grundstückübertragungen».
 *
 * Recon (see README §LU): the online Kantonsblatt is free to read (the Galledia fee is
 * print-only), and no Kaufpreis is published. LU migrates to amtsblattportal.ch (SECO,
 * free, REST API) ~2028 — the DURABLE target. This adapter is shaped so the fetch layer
 * can be swapped to the amtsblattportal REST API (reusing the BE lane's ingest) with no
 * change to parse/normalize/load.
 *
 * ⚠️ The current reader host (www.kantonsblatt.lu.ch, Galledia) had its robots posture
 * UNVERIFIED at recon (TLS failure from the sandbox). Do NOT scrape it until robots is
 * confirmed to permit it, or amtsblattportal (permitted API) carries LU. Legal: LU EGZGB
 * (SRL 200) has no reversion clause ⇒ sans-maître flag is live.
 */

import { join } from 'node:path';
import type { RawRecord } from '../types.js';
import { AccessGatedError, type SourceAdapter } from './types.js';
import { loadFixtureDir } from './fixtures.js';

export const lucerneAdapter: SourceAdapter = {
  canton: 'LU',
  organ: 'kantonsblatt_lu',

  async discover(opts): Promise<RawRecord[]> {
    if (opts.fixtureDir) {
      return loadFixtureDir(join(opts.fixtureDir, 'lucerne'), 'LU', 'kantonsblatt_lu');
    }
    // LIVE PATH — two options, prefer the durable one:
    //   A. amtsblattportal.ch REST API once LU is on it (~2028) — reuse the BE lane's
    //      amtsblattportal ingest; filter to the LU Grundstückübertragungen rubric.
    //   B. Interim: the free online Kantonsblatt reader — ONLY if www.kantonsblatt.lu.ch
    //      robots is verified to permit automated access; otherwise subscription/consent.
    throw new AccessGatedError(
      'kantonsblatt_lu',
      'reader-host robots unverified; target amtsblattportal.ch API (2028) or confirm consent first',
    );
  },
};
