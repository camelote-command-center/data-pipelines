import { fetchMonthlyCSV } from './fetch-monthly.js';
import { parseCSV } from './parse-transform.js';
import { ingest } from './ingest.js';

async function main(): Promise<void> {
  console.log('=== UK HM Land Registry — Monthly Price Paid Update ===\n');

  // 1. Download
  const csvText = await fetchMonthlyCSV();

  // 2. Parse & transform
  const records = parseCSV(csvText);
  if (records.length === 0) {
    console.log('[Done] No records to process.');
    return;
  }

  // 3. Ingest
  const stats = await ingest(records);

  // 4. Summary
  console.log('\n=== Summary ===');
  console.log(`Total downloaded : ${records.length}`);
  console.log(`Inserted         : ${stats.inserted}`);
  console.log(`Updated          : ${stats.updated}`);
  console.log(`Deleted          : ${stats.deleted}`);
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
