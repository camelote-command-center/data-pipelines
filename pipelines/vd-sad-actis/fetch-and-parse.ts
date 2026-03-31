/**
 * fetch-and-parse.ts — ACTIS VD building permit backfill
 *
 * Iterates through numeric IDs on the ACTIS REST endpoint, parses the HTML
 * response into structured fields, and upserts into bronze.sad_national.
 *
 * Uses p-limit for concurrent requests (default: 10 parallel) with adaptive
 * throttling on errors.
 *
 * Usage:
 *   npx tsx fetch-and-parse.ts [start_id] [chunk_size]
 *
 * Environment:
 *   SUPABASE_URL              — Supabase project URL (required)
 *   SUPABASE_SERVICE_ROLE_KEY — service_role key (required)
 */

import { readFileSync, writeFileSync } from 'fs';
import { resolve } from 'path';
import { createClient } from '@supabase/supabase-js';
import proj4 from 'proj4';
import pLimit from 'p-limit';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const ACTIS_BASE = 'https://www.actis.vd.ch/rest/exp/idqry/9008/param';
const STATE_PATH = resolve(__dirname, 'state.json');
const BATCH_SIZE = 200;
const DEFAULT_CHUNK_SIZE = 20000;
const INITIAL_CONCURRENCY = 10;
const WORKER_DELAY_MS = 100; // delay between requests per worker

// EPSG:2056 (Swiss LV95) definition
proj4.defs(
  'EPSG:2056',
  '+proj=somerc +lat_0=46.9524055555556 +lon_0=7.43958333333333 ' +
    '+k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel ' +
    '+towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs'
);

// ---------------------------------------------------------------------------
// Supabase
// ---------------------------------------------------------------------------

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function parseActisDate(s: string): string | null {
  const m = s.match(/(\d{2})\.(\d{2})\.(\d{2})/);
  if (!m) return null;
  const yy = parseInt(m[3], 10);
  const year = yy >= 80 ? 1900 + yy : 2000 + yy;
  return `${year}-${m[2]}-${m[1]}`;
}

function convertCoordinates(easting: number, northing: number): string | null {
  try {
    const [lon, lat] = proj4('EPSG:2056', 'EPSG:4326', [easting, northing]);
    if (isNaN(lon) || isNaN(lat)) return null;
    return JSON.stringify({ type: 'Point', coordinates: [lon, lat] });
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// State management
// ---------------------------------------------------------------------------

interface State {
  last_processed_id: number;
  range_min: number;
  range_max: number;
  total_found: number;
  total_processed: number;
}

function readState(): State {
  try {
    return JSON.parse(readFileSync(STATE_PATH, 'utf-8'));
  } catch {
    return { last_processed_id: 0, range_min: 0, range_max: 0, total_found: 0, total_processed: 0 };
  }
}

function writeState(state: State): void {
  writeFileSync(STATE_PATH, JSON.stringify(state, null, 2) + '\n');
}

// ---------------------------------------------------------------------------
// Adaptive throttling
// ---------------------------------------------------------------------------

let currentConcurrency = INITIAL_CONCURRENCY;
let consecutiveErrors = 0;
let limiter = pLimit(currentConcurrency);

function reduceConcurrency(): void {
  const newConcurrency = Math.max(1, Math.floor(currentConcurrency / 2));
  if (newConcurrency !== currentConcurrency) {
    currentConcurrency = newConcurrency;
    limiter = pLimit(currentConcurrency);
    console.warn(`  [THROTTLE] Reducing concurrency to ${currentConcurrency}`);
  }
}

// ---------------------------------------------------------------------------
// Fetch with backoff
// ---------------------------------------------------------------------------

async function fetchActis(id: number): Promise<{ id: number; text: string | null; error: boolean }> {
  const url = `${ACTIS_BASE}/${id}`;
  try {
    const res = await fetch(url);

    // Rate limiting / blocking
    if (res.status === 429 || res.status === 403 || res.status === 503) {
      consecutiveErrors++;
      if (consecutiveErrors > 3) reduceConcurrency();
      const backoff = Math.min(2000 * consecutiveErrors, 30000);
      await sleep(backoff);
      return { id, text: null, error: true };
    }

    if (!res.ok) return { id, text: null, error: false };

    const buffer = await res.arrayBuffer();
    const text = new TextDecoder('latin1').decode(buffer);
    const clean = text
      .replace(/<[^>]*>/g, ' ')
      .replace(/&[^;]+;/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    consecutiveErrors = 0;
    if (clean.includes('No CAMAC')) return { id, text: clean, error: false };
    return { id, text: null, error: false };
  } catch (err) {
    consecutiveErrors++;
    if (consecutiveErrors > 5) reduceConcurrency();
    return { id, text: null, error: true };
  }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

interface ParsedPermit {
  source_id: string;
  canton: string;
  permit_type: string | null;
  status: string | null;
  description: string | null;
  applicant: string | null;
  owner: string | null;
  commune: string | null;
  address: string | null;
  parcel_number: string | null;
  zone: string | null;
  submission_date: string | null;
  publication_date: string | null;
  decision_date: string | null;
  display_start: string | null;
  display_end: string | null;
  geometry: string | null;
  source_url: string;
  source_system: string;
  raw_data: string;
}

function deriveStatus(permitType: string | null): string | null {
  if (!permitType) return null;
  const upper = permitType.toUpperCase();
  if (upper.includes('AVIS D\'ENQUETE') || upper.includes('AVIS D\'ENQUÊTE')) return 'pendant';
  if (upper.includes('PERMIS DÉLIVRÉ') || upper.includes('PERMIS DELIVRE')) return 'accordé';
  if (upper.includes('PERMIS DE CONSTRUIRE')) return 'accordé';
  if (upper.includes('PERMIS DE DÉMOLIR') || upper.includes('PERMIS DE DEMOLIR')) return 'accordé';
  return null;
}

function extractBetween(text: string, startLabel: string, endLabel: string): string | null {
  const startIdx = text.indexOf(startLabel);
  if (startIdx === -1) return null;
  const afterStart = startIdx + startLabel.length;
  const endIdx = text.indexOf(endLabel, afterStart);
  if (endIdx === -1) return null;
  const value = text.substring(afterStart, endIdx).trim();
  return value || null;
}

function parsePermit(rawText: string, urlId: number): ParsedPermit | null {
  const camacMatch = rawText.match(/No CAMAC\s*(\d+)/);
  if (!camacMatch) return null;
  const camacNumber = camacMatch[1];

  const permitTypeMatch = rawText.match(
    /(AVIS D['']ENQU[ÊE]TE|PERMIS DE CONSTRUIRE|PERMIS DE D[ÉE]MOLIR|PERMIS D[ÉE]LIVR[ÉE])/i
  );
  let permitType = permitTypeMatch ? permitTypeMatch[1] : null;

  const requestTypeMatch = rawText.match(
    /Demande de permis de (?:construire|d[ée]molir)\s*\(([A-Z]+)\)/i
  );
  const requestType = requestTypeMatch ? requestTypeMatch[1] : null;
  if (permitType && requestType) permitType = `${permitType} (${requestType})`;

  const communeMatch = rawText.match(
    /Commune\s*:\s*([A-Z\u00C0-\u024F\s-]+)\s*\(([^)]+)\)/i
  );
  const commune = communeMatch ? communeMatch[1].trim() : null;

  const periodMatch = rawText.match(
    /ouverte du\s*(\d{2}\.\d{2}\.\d{2})\s*au\s*(\d{2}\.\d{2}\.\d{2})/
  );
  const displayStart = periodMatch ? parseActisDate(periodMatch[1]) : null;
  const displayEnd = periodMatch ? parseActisDate(periodMatch[2]) : null;

  const parcelMatch = rawText.match(/Parcelle\(s\)\s*([\d,\s]+)/);
  const parcelNumber = parcelMatch
    ? parcelMatch[1].replace(/\s+/g, '').replace(/,+$/, '')
    : null;

  const coordMatch = rawText.match(/Coordonn[ée]+es\s*\(E\/N\)\s*(\d+)\s*\/\s*(\d+)/);
  let geometry: string | null = null;
  if (coordMatch) {
    geometry = convertCoordinates(parseInt(coordMatch[1], 10), parseInt(coordMatch[2], 10));
  }

  const nature = extractBetween(rawText, 'Nature des travaux:', 'Description de l\'ouvrage:')
    ?? extractBetween(rawText, 'Nature des travaux:', 'Description');
  const descriptionRaw = extractBetween(rawText, 'Description de l\'ouvrage:', 'Situation')
    ?? extractBetween(rawText, 'Description de l\'ouvrage:', 'Situation :');
  const address = extractBetween(rawText, 'Situation :', 'Note de Recens')
    ?? extractBetween(rawText, 'Situation :', 'Note');
  const ownerRaw = extractBetween(rawText, 'Propriétaire(s) :', 'Promettant')
    ?? extractBetween(rawText, 'Propri taire(s) :', 'Promettant');
  const architect = extractBetween(rawText, 'Auteur(s) des plans :', 'Demande de dérogation')
    ?? extractBetween(rawText, 'Auteur(s) des plans :', 'Demande de d rogation');

  let description: string | null = null;
  if (nature && descriptionRaw) description = `${nature} — ${descriptionRaw}`;
  else description = nature ?? descriptionRaw ?? null;

  const owner = ownerRaw?.replace(/^-+$/, '').trim() || null;
  const status = deriveStatus(permitType);

  return {
    source_id: camacNumber,
    canton: 'VD',
    permit_type: permitType,
    status,
    description,
    applicant: architect?.replace(/^-+$/, '').trim() || null,
    owner,
    commune,
    address: address?.replace(/^-+$/, '').trim() || null,
    parcel_number: parcelNumber,
    zone: null,
    submission_date: null,
    publication_date: displayStart,
    decision_date: null,
    display_start: displayStart,
    display_end: displayEnd,
    geometry,
    source_url: `${ACTIS_BASE}/${urlId}`,
    source_system: 'actis_vd',
    raw_data: JSON.stringify({ raw_text: rawText, url_id: urlId }),
  };
}

// ---------------------------------------------------------------------------
// Upsert
// ---------------------------------------------------------------------------

async function upsertBatch(batch: ParsedPermit[]): Promise<number> {
  if (batch.length === 0) return 0;
  const { error } = await supabase
    .schema('bronze')
    .from('sad_national')
    .upsert(batch as any[], { onConflict: 'source_id,canton' });
  if (error) {
    console.error(`  [DB ERROR] ${error.message}`);
    return 0;
  }
  return batch.length;
}

// ---------------------------------------------------------------------------
// Main — concurrent pipeline
// ---------------------------------------------------------------------------

async function main() {
  const state = readState();

  const args = process.argv.slice(2);
  const startId = args[0] ? parseInt(args[0], 10) : state.last_processed_id + 1;
  const chunkSize = args[1] ? parseInt(args[1], 10) : DEFAULT_CHUNK_SIZE;
  const endId = startId + chunkSize - 1;

  console.log('='.repeat(60));
  console.log('  ACTIS VD — Fetch & Parse (Concurrent Backfill)');
  console.log('='.repeat(60));
  console.log(`  Start ID:     ${startId}`);
  console.log(`  End ID:       ${endId}`);
  console.log(`  Chunk size:   ${chunkSize}`);
  console.log(`  Concurrency:  ${INITIAL_CONCURRENCY} parallel requests`);
  console.log(`  Batch size:   ${BATCH_SIZE} records per upsert`);
  console.log('');

  let totalProcessed = 0;
  let totalFound = 0;
  let totalInserted = 0;
  let totalErrors = 0;
  let batch: ParsedPermit[] = [];
  let lastSavedProcessed = 0;
  let lastSavedFound = 0;
  const t0 = Date.now();

  // Process in windows of 1000 IDs at a time for memory + progress tracking
  const WINDOW = 1000;

  for (let windowStart = startId; windowStart <= endId; windowStart += WINDOW) {
    const windowEnd = Math.min(windowStart + WINDOW - 1, endId);
    const ids: number[] = [];
    for (let id = windowStart; id <= windowEnd; id++) ids.push(id);

    // Fetch all IDs in this window concurrently
    const results = await Promise.all(
      ids.map((id) =>
        limiter(async () => {
          await sleep(WORKER_DELAY_MS);
          return fetchActis(id);
        })
      )
    );

    // Process results
    for (const { id, text, error } of results) {
      totalProcessed++;
      if (error) { totalErrors++; continue; }
      if (!text) continue;

      const permit = parsePermit(text, id);
      if (!permit) {
        totalErrors++;
        continue;
      }

      totalFound++;
      batch.push(permit);

      // Upsert when batch is full
      if (batch.length >= BATCH_SIZE) {
        const count = await upsertBatch(batch);
        totalInserted += count;
        if (count === 0) totalErrors += batch.length;
        batch = [];
      }
    }

    // Save state after each window
    state.last_processed_id = windowEnd;
    state.total_found += (totalFound - lastSavedFound);
    state.total_processed += (totalProcessed - lastSavedProcessed);
    lastSavedFound = totalFound;
    lastSavedProcessed = totalProcessed;
    writeState(state);

    // Progress log
    const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
    const rate = (totalProcessed / ((Date.now() - t0) / 1000)).toFixed(1);
    const remaining = endId - windowEnd;
    const eta = remaining > 0 ? ((remaining / parseFloat(rate)) / 60).toFixed(1) : '0';
    console.log(
      `  [${totalProcessed}/${chunkSize}] ID ${windowEnd} | found: ${totalFound} | inserted: ${totalInserted} | errors: ${totalErrors} | ${rate} req/s | ETA: ${eta} min | conc: ${currentConcurrency}`
    );
  }

  // Flush remaining batch
  if (batch.length > 0) {
    const count = await upsertBatch(batch);
    totalInserted += count;
    if (count === 0) totalErrors += batch.length;
  }

  // Final state save
  state.last_processed_id = endId;
  state.total_found += (totalFound - lastSavedFound);
  state.total_processed += (totalProcessed - lastSavedProcessed);
  writeState(state);

  const elapsed = ((Date.now() - t0) / 1000 / 60).toFixed(1);
  console.log('\n' + '='.repeat(60));
  console.log('  BACKFILL COMPLETE');
  console.log(`  IDs scanned:     ${totalProcessed}`);
  console.log(`  Valid permits:    ${totalFound}`);
  console.log(`  Inserted/updated: ${totalInserted}`);
  console.log(`  Errors:           ${totalErrors}`);
  console.log(`  Duration:         ${elapsed} min`);
  console.log(`  Avg rate:         ${(totalProcessed / (parseFloat(elapsed) * 60)).toFixed(1)} req/s`);
  console.log('='.repeat(60));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
