/**
 * Fixture reader shared by all adapters.
 *
 * A fixture is a JSON file of shape { pub: PublicationRef, blocks: string[] } holding
 * one located publication and its raw Handänderung text blocks. This is how the parser
 * is regression-tested and exercised end-to-end while live sources are access-gated.
 */

import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Canton, PublicationRef, RawRecord } from '../types.js';

interface Fixture {
  pub: PublicationRef;
  blocks: string[];
}

export function loadFixtureDir(dir: string, canton: Canton, organ: string): RawRecord[] {
  const out: RawRecord[] = [];
  for (const name of readdirSync(dir)) {
    if (!name.endsWith('.json')) continue;
    const fx = JSON.parse(readFileSync(join(dir, name), 'utf8')) as Fixture;
    // Trust the fixture's own canton/organ but assert they match the adapter.
    if (fx.pub.canton !== canton || fx.pub.source_organ !== organ) continue;
    for (const text of fx.blocks) out.push({ pub: fx.pub, text });
  }
  return out;
}
