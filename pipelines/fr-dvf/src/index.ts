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

/**
 * Read enough bytes from the stream to see the first newline (header line),
 * pick the more frequent of '|' or ',' as the delimiter, and return a new
 * Readable that yields the buffered prefix followed by the rest of the source.
 */
async function sniffDelimiter(
  source: Readable,
): Promise<{ delimiter: string; prefixed: Readable }> {
  const MAX_PEEK = 64 * 1024;
  return new Promise((resolve, reject) => {
    const buffered: Buffer[] = [];
    let totalLen = 0;
    let done = false;

    const onData = (chunk: Buffer) => {
      if (done) return;
      buffered.push(chunk);
      totalLen += chunk.length;
      const joined = Buffer.concat(buffered).toString('utf8');
      const nl = joined.indexOf('\n');
      if (nl === -1 && totalLen < MAX_PEEK) return;

      done = true;
      source.off('data', onData);
      source.off('error', onError);
      source.pause();

      const headerLine = nl !== -1 ? joined.slice(0, nl) : joined;
      const pipeCount = (headerLine.match(/\|/g) ?? []).length;
      const commaCount = (headerLine.match(/,/g) ?? []).length;
      const delimiter = pipeCount >= commaCount && pipeCount > 0 ? '|' : ',';

      // Put the buffered bytes back so downstream consumers see them.
      for (let i = buffered.length - 1; i >= 0; i--) source.unshift(buffered[i]);
      resolve({ delimiter, prefixed: source });
    };
    const onError = (err: Error) => {
      if (done) return;
      done = true;
      reject(err);
    };

    source.on('data', onData);
    source.on('error', onError);
  });
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

  // 2. Sniff delimiter from the first line, then stream-parse.
  // DVF historically used '|' but 2025 onward uses ','. We peek the header
  // line off the front of the stream and pick whichever delimiter occurs more.
  const nodeStream = Readable.fromWeb(stream as any);
  const { delimiter, prefixed } = await sniffDelimiter(nodeStream);
  console.log(`[Parse] Detected delimiter: ${JSON.stringify(delimiter)}`);

  const parser = prefixed.pipe(
    parse({
      delimiter,
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
