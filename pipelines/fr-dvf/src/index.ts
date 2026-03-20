import { fetchYearlyCSV } from './fetch-yearly.js';
import { parseCSV } from './parse-transform.js';
import { ingest } from './ingest.js';

function parseArgs(argv: string[]): { year?: number } {
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === '--year' && argv[i + 1]) {
      return { year: parseInt(argv[i + 1], 10) };
    }
  }
  return {};
}

async function main(): Promise<void> {
  console.log('=== France DVF — Demandes de Valeurs Foncières Update ===\n');

  const { year } = parseArgs(process.argv);

  // 1. Download
  const { csv, year: actualYear } = await fetchYearlyCSV(year);

  // 2. Parse & transform
  const records = parseCSV(csv);
  if (records.length === 0) {
    console.log('[Done] No records to process.');
    return;
  }

  // 3. Ingest
  const stats = await ingest(records);

  // 4. Summary
  console.log('\n=== Summary ===');
  console.log(`Year             : ${actualYear}`);
  console.log(`Total downloaded : ${records.length}`);
  console.log(`Inserted         : ${stats.inserted}`);
  console.log(`Skipped (dupes)  : ${stats.skipped}`);
  console.log(`Errors           : ${stats.errors}`);

  if (stats.errors > 0) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
