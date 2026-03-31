/**
 * Fribourg (FR) Excel Import → bronze.sad_national
 *
 * Reads all .xlsx files from the FRIBOURG folder, maps columns to the
 * sad_national schema, and upserts into Supabase.
 *
 * Usage:
 *   SUPABASE_URL=xxx SUPABASE_SERVICE_ROLE_KEY=xxx npx tsx import-fr-excel.ts
 */

import { createClient } from '@supabase/supabase-js';
import * as XLSX from 'xlsx';
import { readdirSync } from 'fs';
import { join } from 'path';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const DATA_DIR = '/Users/a/Desktop/TRANSACTION CANTON DE VAUD/FRIBOURG';
const CANTON = 'FR';
const SOURCE_SYSTEM = 'fr_excel';
const BATCH_SIZE = 100;

// ---------------------------------------------------------------------------
// Supabase
// ---------------------------------------------------------------------------

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function clean(val: unknown): string | null {
  if (val === undefined || val === null) return null;
  const s = String(val).trim();
  if (s === '' || s === '—' || s === '-' || s === 'N/A') return null;
  return s;
}

function cleanNum(val: unknown): number | null {
  if (val === undefined || val === null) return null;
  const n = Number(val);
  if (isNaN(n) || n === 0) return null;
  return n;
}

function parseDateDMY(val: unknown): string | null {
  const s = clean(val);
  if (!s) return null;
  // "06.03.2026" → "2026-03-06"
  const m = s.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]}`;
  // Already ISO
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  return null;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  FR Excel Import → bronze.sad_national');
  console.log('='.repeat(60));

  const files = readdirSync(DATA_DIR).filter((f) => f.endsWith('.xlsx'));
  console.log(`  Found ${files.length} files in ${DATA_DIR}\n`);

  let grandTotal = 0;
  let grandDupes = 0;

  for (const file of files) {
    const path = join(DATA_DIR, file);
    const wb = XLSX.readFile(path);
    const ws = wb.Sheets[wb.SheetNames[0]];
    const rows: Record<string, unknown>[] = XLSX.utils.sheet_to_json(ws);

    const allRecords = rows.map((row) => {
      const friac = clean(row['ID']);
      // FRIAC 2026-1-00223-S → FR-FRIAC-2026-1-00223-S
      const sourceId = friac
        ? `FR-${friac.replace(/\s+/g, '-')}`
        : `FR-${file}-${Math.random().toString(36).slice(2, 8)}`;

      return {
        source_id: sourceId,
        canton: CANTON,
        permit_type: clean(row['Type']),
        status: clean(row['Statut']),
        description: clean(row['Description']),
        applicant: clean(row['applicant']),
        owner: clean(row['Propriétaire']),
        commune: clean(row['Commune']),
        address: null as string | null,
        parcel_number: null as string | null,
        zone: clean(row["Type d'Usage du Sol"]),
        submission_date: parseDateDMY(row["Date d'Enquête"]),
        publication_date: null as string | null,
        decision_date: parseDateDMY(row["Date d'Approbation"]),
        display_start: null as string | null,
        display_end: null as string | null,
        geometry: null as string | null,
        source_url: null as string | null,
        source_system: SOURCE_SYSTEM,
        raw_data: row,
      };
    });

    // Deduplicate by source_id within file (keep last occurrence)
    const seen = new Map<string, typeof allRecords[0]>();
    for (const rec of allRecords) {
      seen.set(rec.source_id, rec);
    }
    const records = Array.from(seen.values());
    if (records.length < allRecords.length) {
      console.log(`    ${file}: deduped ${allRecords.length} → ${records.length} (${allRecords.length - records.length} in-file dupes)`)
    }

    // Upsert in batches
    let fileUpserted = 0;
    for (let i = 0; i < records.length; i += BATCH_SIZE) {
      const batch = records.slice(i, i + BATCH_SIZE);
      const { error, count } = await supabase
        .schema('bronze')
        .from('sad_national')
        .upsert(batch, { onConflict: 'source_id,canton', count: 'exact' });

      if (error) {
        console.error(`    ${file}: upsert error: ${error.message}`);
      } else {
        fileUpserted += count ?? batch.length;
      }
    }

    const dupes = rows.length - fileUpserted;
    grandTotal += fileUpserted;
    grandDupes += dupes > 0 ? dupes : 0;
    console.log(
      `  ${file}: ${rows.length} rows → ${fileUpserted} upserted${dupes > 0 ? ` (${dupes} dupes)` : ''}`,
    );
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`  TOTAL UPSERTED: ${grandTotal}`);
  if (grandDupes > 0) console.log(`  DUPLICATES:     ${grandDupes}`);
  console.log('='.repeat(60));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
