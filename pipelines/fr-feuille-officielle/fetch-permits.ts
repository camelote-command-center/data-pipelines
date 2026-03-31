/**
 * FR Feuille Officielle — Building Permit Fetcher (Category 21)
 *
 * Fetches building permit articles from the Feuille officielle du canton de
 * Fribourg and upserts them into bronze.sad_national.
 *
 * Each article page contains a 3-column HTML table:
 *   Column 1: commune, address, LV95 coordinates
 *   Column 2: architect info
 *   Column 3: applicant, description, parcel (Art. XX RF), FRIAC ref
 *
 * Coordinates are converted from LV95 (EPSG:2056) → WGS84 (EPSG:4326).
 * FRIAC reference is used as the source_id.
 *
 * Usage:
 *   npx tsx fetch-permits.ts           # current year only
 *   npx tsx fetch-permits.ts 2025      # specific year
 *   npx tsx fetch-permits.ts 2025 2026 # multiple years
 *
 * Environment variables:
 *   SUPABASE_URL              - Supabase project URL (required)
 *   SUPABASE_SERVICE_ROLE_KEY - service_role key     (required)
 */

import { createClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';
import proj4 from 'proj4';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL = 'https://fo.fr.ch';
const CANTON = 'FR';
const SOURCE_SYSTEM = 'fo_fr_ch';
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 500;

// LV95 (Swiss CH1903+/LV95) → WGS84 projection
proj4.defs(
  'EPSG:2056',
  '+proj=somerc +lat_0=46.9524055555556 +lon_0=7.43958333333333 +k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel +towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs +type=crs',
);

// ---------------------------------------------------------------------------
// Supabase client
// ---------------------------------------------------------------------------

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error(
    'ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required',
  );
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchPage(url: string): Promise<string | null> {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      if (response.status === 404) return null;
      console.error(`  HTTP ${response.status} for ${url}`);
      return null;
    }
    return await response.text();
  } catch (err) {
    console.error(`  Fetch error for ${url}: ${err}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Archive crawling
// ---------------------------------------------------------------------------

function extractIssueNumbers(html: string, year: number): number[] {
  const $ = cheerio.load(html);
  const issues: number[] = [];
  const seen = new Set<number>();

  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (!href) return;
    const match = href.match(new RegExp(`/archive/${year}/(\\d+)(?:\\D|$)`));
    if (match) {
      const num = parseInt(match[1], 10);
      if (!seen.has(num)) {
        seen.add(num);
        issues.push(num);
      }
    }
  });

  return issues.sort((a, b) => a - b);
}

function extractNodeUrls(html: string): string[] {
  const $ = cheerio.load(html);
  const urls: string[] = [];
  const seen = new Set<string>();

  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (!href) return;
    const match = href.match(/\/node\/(\d+)/);
    if (match) {
      const url = `${BASE_URL}/node/${match[1]}`;
      if (!seen.has(url)) {
        seen.add(url);
        urls.push(url);
      }
    }
  });

  return urls;
}

// ---------------------------------------------------------------------------
// LV95 → WGS84 coordinate conversion
// ---------------------------------------------------------------------------

function convertLV95toWGS84(
  east: number,
  north: number,
): { lon: number; lat: number } | null {
  try {
    const [lon, lat] = proj4('EPSG:2056', 'EPSG:4326', [east, north]);
    if (lon >= 5.5 && lon <= 11 && lat >= 45.5 && lat <= 48) {
      return {
        lon: Math.round(lon * 1e7) / 1e7,
        lat: Math.round(lat * 1e7) / 1e7,
      };
    }
    return null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Permit parsing from 3-column HTML table
// ---------------------------------------------------------------------------

interface ParsedPermit {
  commune: string;
  address: string | null;
  geometry: string | null;
  architect: string | null;
  applicant: string | null;
  description: string | null;
  parcel_number: string | null;
  friac_ref: string | null;
  source_url: string;
  raw_data: Record<string, unknown>;
}

/**
 * Parse a single article page containing a 3-column permit table.
 * Returns one ParsedPermit per table row (excluding header rows).
 */
function parsePermitPage(html: string, nodeUrl: string): ParsedPermit[] {
  const $ = cheerio.load(html);
  const permits: ParsedPermit[] = [];

  let table = $('div.body table').first();
  if (!table.length) table = $('.field--name-body table').first();
  if (!table.length) table = $('article table').first();
  if (!table.length) table = $('table').first();
  if (!table.length) return permits;

  table.find('tr').each((_, tr) => {
    const tds = $(tr).find('> td');
    if (tds.length < 3) return;

    // Skip header rows (district headers have empty cols 2+3)
    const col2Text = $(tds[1]).text().trim();
    const col3Text = $(tds[2]).text().trim();
    if (!col2Text && !col3Text) return;

    // --- Column 1: commune, address, coordinates ---
    const col1Lines: string[] = [];
    $(tds[0])
      .find('p')
      .each((_, p) => {
        const text = $(p).text().trim();
        if (text) col1Lines.push(text);
      });
    if (col1Lines.length === 0) {
      const fallback = $(tds[0]).text().trim();
      col1Lines.push(
        ...fallback
          .split(/\n/)
          .map((l) => l.trim())
          .filter(Boolean),
      );
    }

    // Find coordinate line
    const coordPattern = /^(\d{7}\.?\d*)\/(\d{7}\.?\d*)$/;
    let geometry: string | null = null;
    let coordLineIdx = -1;
    for (let i = 0; i < col1Lines.length; i++) {
      const m = col1Lines[i].match(coordPattern);
      if (m) {
        coordLineIdx = i;
        const wgs = convertLV95toWGS84(parseFloat(m[1]), parseFloat(m[2]));
        if (wgs) {
          geometry = `SRID=4326;POINT(${wgs.lon} ${wgs.lat})`;
        }
        break;
      }
    }

    const nonCoordLines = col1Lines.filter((_, i) => i !== coordLineIdx);
    const addressPattern =
      /\d|rue |route |chemin |impasse |avenue |boulevard |place |passage |allée |sur-les-|im |weg|strasse|gasse/i;

    let commune = '';
    const addressParts: string[] = [];

    for (const line of nonCoordLines) {
      if (!commune) {
        commune = line;
      } else if (!addressPattern.test(line) && addressParts.length === 0) {
        // Sub-locality (village within multi-village commune)
        commune = `${commune} – ${line}`;
      } else {
        addressParts.push(line);
      }
    }

    const address = addressParts.length > 0 ? addressParts.join(', ') : null;

    // --- Column 2: architect ---
    const col2Lines: string[] = [];
    $(tds[1])
      .find('p')
      .each((_, p) => {
        const text = $(p).text().trim();
        if (text) col2Lines.push(text);
      });
    const architect = col2Lines.length > 0 ? col2Lines.join(', ') : null;

    // --- Column 3: applicant, description, parcel + FRIAC ---
    const col3Paragraphs: string[] = [];
    $(tds[2])
      .find('p')
      .each((_, p) => {
        const text = $(p).text().trim();
        if (text) col3Paragraphs.push(text);
      });

    let applicant: string | null = null;
    let description: string | null = null;
    let parcelNumber: string | null = null;
    let friacRef: string | null = null;

    if (col3Paragraphs.length >= 1) {
      applicant = col3Paragraphs[0];
    }

    // Last paragraph contains "Art. XX RF / Ref. FRIAC: YYYY-N-NNNNN-X"
    const lastP = col3Paragraphs[col3Paragraphs.length - 1] || '';

    const parcelMatch = lastP.match(/Art\.\s*([\d\s,]+)\s*RF/);
    if (parcelMatch) {
      parcelNumber = parcelMatch[1].replace(/\s+/g, ' ').trim();
    }

    const friacMatch = lastP.match(
      /Ref\.\s*FRIAC:\s*(\d+-\d+-\d+-[A-Z])/,
    );
    if (friacMatch) {
      friacRef = friacMatch[1];
    }

    // Description = middle paragraphs (between applicant and reference)
    if (col3Paragraphs.length >= 3) {
      const descParts = col3Paragraphs.slice(1, -1);
      description = descParts.join(' ').trim() || null;
    } else if (col3Paragraphs.length === 2) {
      const secondP = col3Paragraphs[1];
      const artIdx = secondP.search(/Art\.\s*[\d]/);
      if (artIdx > 0) {
        description = secondP.substring(0, artIdx).trim() || null;
      } else {
        description = secondP;
      }
    }

    // Clean up description
    if (description) {
      description =
        description
          .replace(/\s*Sans appel d[''']offres\s*$/i, '')
          .replace(/\.\s*$/, '')
          .trim() || null;
    }

    permits.push({
      commune,
      address,
      geometry,
      architect,
      applicant,
      description,
      parcel_number: parcelNumber,
      friac_ref: friacRef,
      source_url: nodeUrl,
      raw_data: {
        col1_lines: col1Lines,
        col2_lines: col2Lines,
        col3_paragraphs: col3Paragraphs,
      },
    });
  });

  return permits;
}

// ---------------------------------------------------------------------------
// SAD record type
// ---------------------------------------------------------------------------

interface SadNationalRecord {
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
  raw_data: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function processYear(year: number): Promise<SadNationalRecord[]> {
  const records: SadNationalRecord[] = [];

  console.log(`\n--- Year ${year} ---`);

  const archiveUrl = `${BASE_URL}/archive/${year}`;
  console.log(`  Fetching archive: ${archiveUrl}`);
  const archiveHtml = await fetchPage(archiveUrl);
  await sleep(RATE_LIMIT_MS);

  if (!archiveHtml) {
    console.log(`  No archive page for ${year}, skipping.`);
    return records;
  }

  let issues = extractIssueNumbers(archiveHtml, year);
  if (issues.length === 0) {
    console.log(
      `  No issue links found on archive page — trying 1-52 directly`,
    );
    issues = Array.from({ length: 52 }, (_, i) => i + 1);
  }
  console.log(`  Checking ${issues.length} issues`);

  for (const issue of issues) {
    const catUrl = `${BASE_URL}/archive/${year}/${issue}/21`;
    const catHtml = await fetchPage(catUrl);
    await sleep(RATE_LIMIT_MS);

    if (!catHtml) continue;

    const nodeUrls = extractNodeUrls(catHtml);
    if (nodeUrls.length === 0) continue;

    console.log(`  Issue ${issue}: ${nodeUrls.length} articles`);

    for (const nodeUrl of nodeUrls) {
      const articleHtml = await fetchPage(nodeUrl);
      await sleep(RATE_LIMIT_MS);
      if (!articleHtml) continue;

      const permits = parsePermitPage(articleHtml, nodeUrl);

      for (const permit of permits) {
        const sourceId = permit.friac_ref
          ? `FR-FRIAC-${permit.friac_ref}`
          : `FR-${year}-${issue}-${nodeUrl.match(/\/node\/(\d+)/)?.[1]}-${records.length}`;

        records.push({
          source_id: sourceId,
          canton: CANTON,
          permit_type: null,
          status: null,
          description: permit.description,
          applicant: permit.applicant,
          owner: null,
          commune: permit.commune,
          address: permit.address,
          parcel_number: permit.parcel_number,
          zone: null,
          submission_date: null,
          publication_date: null,
          decision_date: null,
          display_start: null,
          display_end: null,
          geometry: permit.geometry,
          source_url: permit.source_url,
          source_system: SOURCE_SYSTEM,
          raw_data: {
            year,
            issue,
            architect: permit.architect,
            ...permit.raw_data,
          },
        });
      }
    }
  }

  return records;
}

async function upsertBatch(records: SadNationalRecord[]): Promise<number> {
  const { error, count } = await supabase
    .schema('bronze')
    .from('sad_national')
    .upsert(records, { onConflict: 'source_id,canton', count: 'exact' });

  if (error) {
    console.error(`  Upsert error: ${error.message}`);
    return 0;
  }

  return count ?? records.length;
}

async function main() {
  console.log('='.repeat(60));
  console.log('  FR Feuille Officielle — Building Permit Pipeline');
  console.log('  Source: fo.fr.ch (category 21, 3-column table)');
  console.log('  Target: bronze.sad_national');
  console.log('='.repeat(60));

  const startTime = Date.now();

  const args = process.argv.slice(2);
  let years: number[];

  if (args.length > 0) {
    years = args.map((a) => parseInt(a, 10)).filter((n) => !isNaN(n));
  } else {
    years = [new Date().getFullYear()];
  }

  console.log(`  Years: ${years.join(', ')}`);

  const allRecords: SadNationalRecord[] = [];

  for (const year of years) {
    const records = await processYear(year);
    allRecords.push(...records);
  }

  console.log(`\n  Total permits parsed: ${allRecords.length}`);

  if (allRecords.length === 0) {
    console.log('  No permits to upsert. Exiting.');
    console.log('='.repeat(60));
    return;
  }

  // Deduplicate by source_id
  const seen = new Map<string, SadNationalRecord>();
  for (const rec of allRecords) {
    seen.set(rec.source_id, rec);
  }
  const deduped = Array.from(seen.values());
  if (deduped.length < allRecords.length) {
    console.log(`  Deduped: ${allRecords.length} → ${deduped.length}`);
  }

  let totalUpserted = 0;
  console.log(
    `\n  Upserting ${deduped.length} records (batch size: ${BATCH_SIZE})...`,
  );

  for (let i = 0; i < deduped.length; i += BATCH_SIZE) {
    const batch = deduped.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(deduped.length / BATCH_SIZE);

    const upserted = await upsertBatch(batch);
    totalUpserted += upserted;

    console.log(
      `  Batch ${batchNum}/${totalBatches}: ${upserted} rows upserted`,
    );
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  const stats = {
    with_friac: deduped.filter((r) => r.source_id.startsWith('FR-FRIAC-'))
      .length,
    with_commune: deduped.filter((r) => r.commune).length,
    with_address: deduped.filter((r) => r.address).length,
    with_geometry: deduped.filter((r) => r.geometry).length,
    with_applicant: deduped.filter((r) => r.applicant).length,
    with_description: deduped.filter((r) => r.description).length,
    with_parcel: deduped.filter((r) => r.parcel_number).length,
    with_architect: deduped.filter((r) => r.raw_data?.architect).length,
  };

  console.log(`\n${'='.repeat(60)}`);
  console.log('  IMPORT COMPLETE');
  console.log(`  Permits parsed:   ${deduped.length}`);
  console.log(`  Records upserted: ${totalUpserted}`);
  console.log(`  Duration:         ${elapsed}s`);
  console.log('  --- Field coverage ---');
  for (const [key, val] of Object.entries(stats)) {
    const pct = ((val / deduped.length) * 100).toFixed(0);
    console.log(`  ${key.padEnd(20)} ${val} (${pct}%)`);
  }
  console.log('='.repeat(60));

  if (totalUpserted === 0 && deduped.length > 0) {
    console.error('  FAILED: Zero rows upserted despite having records!');
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
