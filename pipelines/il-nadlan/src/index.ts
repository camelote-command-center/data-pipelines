import { fetchAllDeals, fetchSettlementIndex, invalidateToken } from './api/nadlan-client.js';
import { transformDeal } from './parsers/transactions.js';
import { bulkInsertTransactions } from './ingestion/bulk-insert.js';
import {
  startIngestion,
  completeIngestion,
  dedupTransactions,
  promoteTransactions,
  refreshMarketData,
} from './ingestion/ingestion-log.js';

// --- Major Israeli cities with their settlement codes ---
const MAJOR_SETTLEMENTS: Array<{ id: string; name: string }> = [
  { id: '5000', name: 'תל אביב - יפו' },
  { id: '3000', name: 'ירושלים' },
  { id: '4000', name: 'חיפה' },
  { id: '7900', name: 'ראשון לציון' },
  { id: '6100', name: 'פתח תקווה' },
  { id: '7400', name: 'אשדוד' },
  { id: '6400', name: 'נתניה' },
  { id: '8300', name: 'באר שבע' },
  { id: '7700', name: 'חולון' },
  { id: '6200', name: 'בני ברק' },
  { id: '2800', name: 'רמת גן' },
  { id: '8400', name: 'אשקלון' },
  { id: '6600', name: 'רחובות' },
  { id: '2610', name: 'בת ים' },
  { id: '2630', name: 'הרצליה' },
  { id: '7000', name: 'כפר סבא' },
  { id: '6900', name: 'רעננה' },
  { id: '2640', name: 'מודיעין-מכבים-רעות' },
  { id: '1200', name: 'נצרת עילית (נוף הגליל)' },
  { id: '6800', name: 'הוד השרון' },
  { id: '2660', name: 'גבעתיים' },
  { id: '8700', name: 'אילת' },
  { id: '2620', name: 'רמת השרון' },
  { id: '7100', name: 'רמלה' },
  { id: '7200', name: 'לוד' },
  { id: '1061', name: 'טבריה' },
  { id: '1139', name: 'עפולה' },
  { id: '1034', name: 'עכו' },
  { id: '1311', name: 'קריית אתא' },
  { id: '1020', name: 'נהריה' },
];

function usage(): void {
  console.log(`
Israel Nadlan (Real Estate) Pipeline

Usage:
  npx tsx src/index.ts --source transactions --from <date> --to <date>
  npx tsx src/index.ts --source transactions --settlement <code> --from <date> --to <date>
  npx tsx src/index.ts --discover

Options:
  --source <name>       Data source (currently: transactions)
  --from <date>         Start date (YYYY-MM-DD)
  --to <date>           End date (YYYY-MM-DD)
  --settlement <code>   Specific settlement code (default: all major cities)
  --discover            List available settlements from the index

Examples:
  npx tsx src/index.ts --source transactions --from 2024-01-01 --to 2024-12-31
  npx tsx src/index.ts --source transactions --settlement 5000 --from 2024-06-01 --to 2024-12-31
  npx tsx src/index.ts --discover
`);
}

function parseArgs(argv: string[]): Record<string, string | boolean> {
  const args: Record<string, string | boolean> = {};
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--discover' || arg === '--help') {
      args[arg.replace('--', '')] = true;
    } else if (arg.startsWith('--') && i + 1 < argv.length) {
      args[arg.replace('--', '')] = argv[++i];
    }
  }
  return args;
}

async function discoverSettlements(): Promise<void> {
  console.log('[Discover] Fetching settlement index...');
  const settlements = await fetchSettlementIndex();
  console.log(`[Discover] Found ${settlements.length} settlements`);
  for (const s of settlements.slice(0, 50)) {
    console.log(`  ${s.id}: ${s.name}`);
  }
}

async function ingestTransactions(
  dateFrom: string,
  dateTo: string,
  settlementCode?: string,
): Promise<void> {
  const settlements = settlementCode
    ? [{ id: settlementCode, name: `Settlement ${settlementCode}` }]
    : MAJOR_SETTLEMENTS;

  console.log(`\n[Ingest] Processing ${settlements.length} settlements from ${dateFrom} to ${dateTo}\n`);

  let grandTotalFetched = 0;
  let grandTotalInserted = 0;
  let grandTotalSkipped = 0;
  let grandTotalFailed = 0;

  for (const settlement of settlements) {
    console.log(`\n--- ${settlement.name} (${settlement.id}) ---`);

    // 1. Start ingestion log
    let logId: string;
    try {
      logId = await startIngestion('nadlan.gov.il', settlement.name, dateFrom, dateTo, {
        settlement_code: settlement.id,
      });
    } catch (err: any) {
      console.error(`[Ingest] Failed to start ingestion for ${settlement.name}: ${err.message}`);
      continue;
    }

    let fetched = 0;
    let inserted = 0;
    let skipped = 0;
    let failed = 0;
    let errorMsg: string | undefined;

    try {
      // 2. Fetch all deals
      const allRecords: ReturnType<typeof transformDeal>[] = [];

      const result = await fetchAllDeals(settlement.id, async (items, page, total) => {
        const transformed = items.map(transformDeal);
        allRecords.push(...transformed);
        fetched += items.length;

        if (page % 10 === 0 || page === 1) {
          console.log(`  Page ${page}: ${items.length} items (${fetched}/${total} total)`);
        }
      });

      console.log(`  Fetched ${fetched} total deals`);

      // 3. Bulk insert
      if (allRecords.length > 0) {
        const insertResult = await bulkInsertTransactions(allRecords, logId);
        inserted = insertResult.inserted;
        skipped = insertResult.skipped;
        failed = insertResult.failed;
        if (insertResult.errors.length > 0) {
          errorMsg = insertResult.errors.slice(0, 3).join('; ');
        }
      }

      console.log(`  Inserted: ${inserted}, Skipped: ${skipped}, Failed: ${failed}`);
    } catch (err: any) {
      errorMsg = err.message;
      console.error(`  Error: ${err.message}`);
      // Refresh token on auth failures
      if (err.message.includes('405') || err.message.includes('token')) {
        invalidateToken();
      }
    }

    // 4. Complete ingestion log
    await completeIngestion(logId, fetched, inserted, 0, failed, errorMsg);

    grandTotalFetched += fetched;
    grandTotalInserted += inserted;
    grandTotalSkipped += skipped;
    grandTotalFailed += failed;
  }

  // 5. Dedup
  console.log('\n[Dedup] Running deduplication...');
  const dedupCount = await dedupTransactions();
  console.log(`[Dedup] Removed ${dedupCount} duplicates`);

  // 6. Promote (stub)
  console.log('[Promote] Running silver promotion...');
  const promoted = await promoteTransactions(dateFrom);
  console.log(`[Promote] Promoted ${promoted} rows`);

  // 7. Refresh market data (stub)
  console.log('[Refresh] Refreshing gold market data...');
  const refreshed = await refreshMarketData();
  console.log(`[Refresh] Refreshed ${refreshed} rows`);

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Settlements   : ${settlements.length}`);
  console.log(`Total fetched : ${grandTotalFetched}`);
  console.log(`Inserted      : ${grandTotalInserted}`);
  console.log(`Skipped       : ${grandTotalSkipped}`);
  console.log(`Failed        : ${grandTotalFailed}`);
  console.log(`Deduped       : ${dedupCount}`);

  if (grandTotalFailed > 0) {
    process.exit(1);
  }
}

async function main(): Promise<void> {
  console.log('=== Israel Nadlan — Real Estate Transaction Pipeline ===\n');

  const args = parseArgs(process.argv);

  if (args.help || Object.keys(args).length === 0) {
    usage();
    process.exit(0);
  }

  if (args.discover) {
    await discoverSettlements();
    return;
  }

  const source = args.source as string;
  const dateFrom = args.from as string;
  const dateTo = args.to as string;
  const settlement = args.settlement as string | undefined;

  if (!source) {
    console.error('Error: --source is required');
    process.exit(1);
  }

  if (!dateFrom || !dateTo) {
    console.error('Error: --from and --to dates are required');
    process.exit(1);
  }

  switch (source) {
    case 'transactions':
      await ingestTransactions(dateFrom, dateTo, settlement);
      break;
    default:
      console.error(`Unknown source: ${source}. Available: transactions`);
      process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
