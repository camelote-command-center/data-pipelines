/**
 * NE SATAC PDF Enrichment — Enrich NE permits with data from SATAC PDFs
 *
 * Queries all NE permits missing applicant data, fetches the SATAC PDF from
 * source_url, extracts structured text, and updates each row with:
 *   - applicant (Requérant)
 *   - address (Situation)
 *   - parcel_number (Parcelle)
 *   - zone (Affectation de la zone)
 *   - raw_data enriched with architect, satac_no, pdf_text
 *
 * Uses p-limit for 10 concurrent PDF fetches with adaptive throttling.
 *
 * Usage:
 *   npx tsx enrich-from-pdfs.ts
 *
 * Environment:
 *   SUPABASE_URL              — Supabase project URL (required)
 *   SUPABASE_SERVICE_ROLE_KEY — service_role key (required)
 */

import { createClient } from '@supabase/supabase-js';
import pLimit from 'p-limit';
// @ts-ignore — pdf-parse v4 exports PDFParse class
import { PDFParse } from 'pdf-parse';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const CONCURRENCY = 10;
const BATCH_SIZE = 100;
const WORKER_DELAY_MS = 50;

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

// ---------------------------------------------------------------------------
// PDF text extraction
// ---------------------------------------------------------------------------

async function fetchPdfText(url: string): Promise<string | null> {
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const buffer = await res.arrayBuffer();
    const parser = new PDFParse(new Uint8Array(buffer));
    const result = await parser.getText();
    return result.text || null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

interface EnrichedFields {
  applicant: string | null;
  address: string | null;
  parcel_number: string | null;
  zone: string | null;
  architect: string | null;
  satac_no: string | null;
  permit_type_detail: string | null;
  description: string | null;
}

function parsePdfText(text: string): EnrichedFields {
  // Normalize whitespace but keep newlines for structure
  const norm = text.replace(/\r\n/g, '\n').replace(/[ \t]+/g, ' ');

  const applicant =
    norm.match(/Requ[ée]rant\(s\)\s*:\s*(.+?)(?=\nAuteur|\nDemande|$)/s)?.[1]?.trim() || null;

  const address =
    norm.match(/Situation\s*:\s*(.+?)(?=\nDescription|$)/s)?.[1]?.trim() || null;

  const parcelMatch = norm.match(/Parcelle\(s\)(?:\s*et coordonn[ée]es)?\s*:\s*([\d,\s]+)/);
  const parcel = parcelMatch
    ? parcelMatch[1].replace(/\s+/g, '').replace(/,+$/, '').replace(/-+$/, '')
    : null;

  const zone =
    norm.match(/Affectation de la zone\s*:\s*(.+?)(?=\nAutorisation|Particularit|$)/s)?.[1]?.trim() || null;

  const architect =
    norm.match(/Auteur\(s\) des plans\s*:\s*(.+?)(?=\nDemande|$)/s)?.[1]?.trim() || null;

  const satacNo =
    norm.match(/Dossier SATAC n[°o]\s*(\d+)/)?.[1] || null;

  const permitTypeDetail =
    norm.match(/Demande de permis de construire\s*:\s*(.+?)(?=\n|$)/)?.[1]?.trim() || null;

  const description =
    norm.match(/Description de l'ouvrage\s*:\s*(.+?)(?=\nRequ[ée]rant|$)/s)?.[1]?.trim() || null;

  return { applicant, address, parcel_number: parcel, zone, architect, satac_no: satacNo, permit_type_detail: permitTypeDetail, description };
}

// ---------------------------------------------------------------------------
// Adaptive throttling
// ---------------------------------------------------------------------------

let currentConcurrency = CONCURRENCY;
let consecutiveErrors = 0;
let limiter = pLimit(currentConcurrency);

function reduceConcurrency(): void {
  const next = Math.max(1, Math.floor(currentConcurrency / 2));
  if (next !== currentConcurrency) {
    currentConcurrency = next;
    limiter = pLimit(currentConcurrency);
    console.warn(`  [THROTTLE] Reducing concurrency to ${currentConcurrency}`);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  NE SATAC — PDF Enrichment');
  console.log('  Source: SATAC PDF documents');
  console.log('  Target: bronze.sad_national (UPDATE)');
  console.log('='.repeat(60));

  const t0 = Date.now();

  // 1. Query all NE permits missing applicant data with valid source_url
  console.log('\n  Fetching permits to enrich...');

  let allPermits: any[] = [];
  let offset = 0;
  const PAGE = 1000;

  while (true) {
    const { data, error } = await supabase
      .schema('bronze')
      .from('sad_national')
      .select('id, source_id, source_url, raw_data')
      .eq('canton', 'NE')
      .is('applicant', null)
      .not('source_url', 'is', null)
      .range(offset, offset + PAGE - 1);

    if (error) {
      console.error(`  Query error: ${error.message}`);
      break;
    }
    if (!data || data.length === 0) break;
    allPermits.push(...data);
    offset += data.length;
    if (data.length < PAGE) break;
  }

  console.log(`  Found ${allPermits.length} permits to enrich`);

  if (allPermits.length === 0) {
    console.log('  Nothing to do.');
    return;
  }

  // 2. Process in windows
  const WINDOW = 100;
  let totalProcessed = 0;
  let totalEnriched = 0;
  let totalErrors = 0;
  let totalNoPdf = 0;

  for (let i = 0; i < allPermits.length; i += WINDOW) {
    const window = allPermits.slice(i, i + WINDOW);

    const results = await Promise.all(
      window.map((permit) =>
        limiter(async () => {
          await sleep(WORKER_DELAY_MS);

          if (!permit.source_url) {
            return { permit, text: null, error: false };
          }

          try {
            const text = await fetchPdfText(permit.source_url);
            if (!text) {
              consecutiveErrors++;
              if (consecutiveErrors > 5) reduceConcurrency();
              return { permit, text: null, error: false };
            }
            consecutiveErrors = 0;
            return { permit, text, error: false };
          } catch {
            consecutiveErrors++;
            if (consecutiveErrors > 5) reduceConcurrency();
            return { permit, text: null, error: true };
          }
        })
      )
    );

    // Build update batch
    const updates: { id: number; fields: Record<string, any> }[] = [];

    for (const { permit, text, error } of results) {
      totalProcessed++;
      if (error) { totalErrors++; continue; }
      if (!text) { totalNoPdf++; continue; }

      const parsed = parsePdfText(text);

      // Only update if we extracted something useful
      if (!parsed.applicant && !parsed.address && !parsed.parcel_number && !parsed.zone) {
        totalNoPdf++;
        continue;
      }

      const existingRaw = permit.raw_data || {};
      const updateFields: Record<string, any> = {};

      if (parsed.applicant) updateFields.applicant = parsed.applicant;
      if (parsed.address) updateFields.address = parsed.address;
      if (parsed.parcel_number) updateFields.parcel_number = parsed.parcel_number;
      if (parsed.zone) updateFields.zone = parsed.zone;
      if (parsed.description) updateFields.description = parsed.description;

      updateFields.raw_data = {
        ...existingRaw,
        ...(parsed.architect ? { architect: parsed.architect } : {}),
        ...(parsed.satac_no ? { satac_no: parsed.satac_no } : {}),
        ...(parsed.permit_type_detail ? { permit_type_detail: parsed.permit_type_detail } : {}),
        pdf_text: text.substring(0, 5000), // cap raw text at 5KB
      };

      updates.push({ id: permit.id, fields: updateFields });
    }

    // Execute updates
    for (const { id, fields } of updates) {
      const { error } = await supabase
        .schema('bronze')
        .from('sad_national')
        .update(fields)
        .eq('id', id);

      if (error) {
        totalErrors++;
      } else {
        totalEnriched++;
      }
    }

    // Progress
    const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
    const rate = (totalProcessed / ((Date.now() - t0) / 1000)).toFixed(1);
    const remaining = allPermits.length - (i + WINDOW);
    const eta = remaining > 0 ? ((remaining / parseFloat(rate)) / 60).toFixed(1) : '0';
    console.log(
      `  [${totalProcessed}/${allPermits.length}] enriched: ${totalEnriched} | no-pdf: ${totalNoPdf} | errors: ${totalErrors} | ${rate}/s | ETA: ${eta} min | conc: ${currentConcurrency}`
    );
  }

  const elapsed = ((Date.now() - t0) / 1000 / 60).toFixed(1);
  console.log('\n' + '='.repeat(60));
  console.log('  ENRICHMENT COMPLETE');
  console.log(`  Permits processed:  ${totalProcessed}`);
  console.log(`  Enriched:           ${totalEnriched}`);
  console.log(`  No PDF / no data:   ${totalNoPdf}`);
  console.log(`  Errors:             ${totalErrors}`);
  console.log(`  Duration:           ${elapsed} min`);
  console.log('='.repeat(60));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
