/**
 * Valais (VS) Excel Import → bronze.transactions_national
 *
 * Reads all .xlsx files from the VALAIS folder, maps columns to the
 * transactions_national schema, and upserts into Supabase.
 *
 * Usage:
 *   SUPABASE_URL=xxx SUPABASE_SERVICE_ROLE_KEY=xxx npx tsx import-vs-excel.ts
 */

import { createClient } from '@supabase/supabase-js';
import * as XLSX from 'xlsx';
import { readdirSync } from 'fs';
import { join } from 'path';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const DATA_DIR = '/Users/a/Desktop/TRANSACTION CANTON DE VAUD/VALAIS';
const CANTON = 'VS';
const SOURCE_SYSTEM = 'vs_excel';
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  VS Excel Import → bronze.transactions_national');
  console.log('='.repeat(60));

  const files = readdirSync(DATA_DIR).filter((f) => f.endsWith('.xlsx'));
  console.log(`  Found ${files.length} files in ${DATA_DIR}`);

  let grandTotal = 0;

  for (const file of files) {
    const path = join(DATA_DIR, file);
    const wb = XLSX.readFile(path);
    const ws = wb.Sheets[wb.SheetNames[0]];
    const rows: Record<string, unknown>[] = XLSX.utils.sheet_to_json(ws);

    console.log(`\n  ${file}: ${rows.length} rows`);

    const records = rows.map((row) => {
      const id = clean(row['ID']);
      const sourceId = id ? `VS-${id}` : `VS-${file}-${Math.random().toString(36).slice(2, 8)}`;

      return {
        source_id: sourceId,
        source_url: clean(row['ID de transaction']),
        transaction_date: clean(row['Date']),
        address: clean(row['Adresse']),
        reason: clean(row['Raison']),
        property_type: clean(row['Type de propriété']),
        price: cleanNum(row['Prix']),
        surface_m2: cleanNum(row['Surface']),
        price_per_m2: cleanNum(row['Prix/m²']),
        nb_buyers: cleanNum(row['Nb acheteurs']) ?? 0,
        buyers: clean(row['Acheteurs']),
        nb_sellers: cleanNum(row['Nb vendeurs']) ?? 0,
        sellers: clean(row['Vendeurs']),
        previous_transaction_date: clean(row['Transaction précédente']),
        canton: CANTON,
        source_file: file,
        raw_data: row,
      };
    });

    // Upsert in batches
    let fileUpserted = 0;
    for (let i = 0; i < records.length; i += BATCH_SIZE) {
      const batch = records.slice(i, i + BATCH_SIZE);
      const { error, count } = await supabase
        .schema('bronze')
        .from('transactions_national')
        .upsert(batch, { onConflict: 'source_id,canton', count: 'exact' });

      if (error) {
        console.error(`    Upsert error: ${error.message}`);
      } else {
        fileUpserted += count ?? batch.length;
      }
    }

    console.log(`    Upserted: ${fileUpserted}`);
    grandTotal += fileUpserted;
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`  TOTAL UPSERTED: ${grandTotal}`);
  console.log('='.repeat(60));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
