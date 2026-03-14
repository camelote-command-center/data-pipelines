import { createReadStream } from 'node:fs';
import { parse } from 'csv-parse';

export async function parseCsv(filePath: string): Promise<Record<string, any>[]> {
  return new Promise((resolve, reject) => {
    const rows: Record<string, any>[] = [];

    const parser = createReadStream(filePath).pipe(
      parse({
        columns: true,
        skip_empty_lines: true,
        trim: true,
        bom: true,
        relax_column_count: true,
      }),
    );

    parser.on('data', (row) => rows.push(row));
    parser.on('end', () => {
      console.log(`[CSV] Parsed ${rows.length} rows from ${filePath}`);
      resolve(rows);
    });
    parser.on('error', reject);
  });
}
