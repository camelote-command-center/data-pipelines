import { ingestSource } from './ingestion/api-ingestion.js';
import { DubaiPulseClient } from './api/dubai-pulse-client.js';
import { ENDPOINTS } from './api/endpoints.js';

const ALL_SOURCES = ['transactions', 'rentals', 'projects', 'valuations', 'land', 'buildings', 'units', 'brokers', 'developers'];

function usage(): void {
  console.log(`
Dubai Data Pipeline — DLD Ingestion

Usage:
  npx tsx src/index.ts --source <name> --from <date> --to <date> [--csv <path>]
  npx tsx src/index.ts --all --from <date> --to <date>
  npx tsx src/index.ts --discover

Options:
  --source <name>   Data source: ${Object.keys(ENDPOINTS).join(', ')}
  --from <date>     Start date (YYYY-MM-DD)
  --to <date>       End date (YYYY-MM-DD)
  --csv <path>      Use local CSV file instead of API
  --all             Ingest all sources via API
  --discover        Discover available API endpoints

Examples:
  npx tsx src/index.ts --source transactions --from 2024-01-01 --to 2024-12-31
  npx tsx src/index.ts --source transactions --csv ./data/transactions_2024.csv --from 2024-01-01 --to 2024-12-31
  npx tsx src/index.ts --all --from 2024-01-01 --to 2024-12-31
`);
}

function parseArgs(argv: string[]): Record<string, string | boolean> {
  const args: Record<string, string | boolean> = {};
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--discover' || arg === '--all' || arg === '--help') {
      args[arg.replace('--', '')] = true;
    } else if (arg.startsWith('--') && i + 1 < argv.length) {
      args[arg.replace('--', '')] = argv[++i];
    }
  }
  return args;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv);

  if (args.help || Object.keys(args).length === 0) {
    usage();
    process.exit(0);
  }

  if (args.discover) {
    const client = new DubaiPulseClient();
    await client.discoverEndpoints();
    return;
  }

  const dateFrom = args.from as string;
  const dateTo = args.to as string;

  if (!dateFrom || !dateTo) {
    console.error('Error: --from and --to dates are required');
    process.exit(1);
  }

  const sources = args.all ? ALL_SOURCES : [args.source as string];

  if (!args.all && !args.source) {
    console.error('Error: --source or --all is required');
    process.exit(1);
  }

  const mode = args.csv ? 'csv' as const : 'api' as const;
  const csvPath = args.csv as string | undefined;

  let hasFailure = false;

  for (const source of sources) {
    try {
      await ingestSource({ source, dateFrom, dateTo, mode, csvPath });
    } catch (error: any) {
      console.error(`\n[ERROR] ${source} ingestion failed: ${error.message}\n`);
      hasFailure = true;
      // Continue with other sources if running --all
      if (!args.all) process.exit(1);
    }
  }

  if (hasFailure) {
    console.error('\nSome ingestions failed. Check logs above.');
    process.exit(1);
  }

  console.log('\nAll ingestions completed successfully.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
