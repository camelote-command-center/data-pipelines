import { Readable } from 'node:stream';
import { parse } from 'csv-parse';
import { fetchYearlyCSV } from './fetch-yearly.js';
import { transformRow, type DVFRecord } from './parse-transform.js';
import { ingest, type IngestStats } from './ingest.js';

const STREAM_BATCH_SIZE = 5000;

function parseArgs(argv: string[]): { year?: number } {
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === '--year' && argv[i + 1]) {
      return { year: parseInt(argv[i + 1], 10) };
    }
  }
  return {};
}

function addStats(into: IngestStats, from: IngestStats): void {
  into.inserted += from.inserted;
  into.skipped += from.skipped;
  into.errors += from.errors;
}

async function main(): Promise<void> {
  console.log('=== France DVF — Demandes de Valeurs Foncières Update ===\n');

  const { year } = parseArgs(process.argv);

  // 1. Open streaming download
  const { stream, year: actualYear } = await fetchYearlyCSV(year);

  // 2. Stream-parse CSV → batched transform → ingest
  const nodeStream = Readable.fromWeb(stream as any);
  const parser = nodeStream.pipe(
    parse({
      delimiter: '|',
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
    }),
  );

  const totalStats: IngestStats = { inserted: 0, skipped: 0, errors: 0 };
  let totalRows = 0;
  let totalRecords = 0;
  let buffer: DVFRecord[] = [];

  for await (const row of parser) {
    totalRows++;
    const record = transformRow(row as Record<string, string>);
    if (!record) continue;
    buffer.push(record);
    totalRecords++;

    if (buffer.length >= STREAM_BATCH_SIZE) {
      const batch = buffer;
      buffer = [];
      addStats(totalStats, await ingest(batch));
    }
  }

  if (buffer.length > 0) {
    addStats(totalStats, await ingest(buffer));
    buffer = [];
  }

  // 3. Summary
  console.log('\n=== Summary ===');
  console.log(`Year             : ${actualYear}`);
  console.log(`Rows read        : ${totalRows}`);
  console.log(`Records produced : ${totalRecords}`);
  console.log(`Inserted         : ${totalStats.inserted}`);
  console.log(`Skipped (dupes)  : ${totalStats.skipped}`);
  console.log(`Errors           : ${totalStats.errors}`);

  if (totalStats.errors > 0) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
