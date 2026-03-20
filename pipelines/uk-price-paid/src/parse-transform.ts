import { parse } from 'csv-parse/sync';

export interface PricePaidRecord {
  transaction_id: string;
  price: number;
  transaction_date: string; // ISO date
  postcode: string | null;
  property_type: string | null;
  new_build: boolean;
  tenure: string | null;
  paon: string | null;
  saon: string | null;
  street: string | null;
  locality: string | null;
  town: string | null;
  district: string | null;
  county: string | null;
  ppd_category: string | null;
  record_status: string; // A, C, D
  country: string;
  admin_level_1: string;
}

function emptyToNull(val: string): string | null {
  return val.trim() === '' ? null : val.trim();
}

function parseDate(raw: string): string {
  // Format: "2024-01-15 00:00" → "2024-01-15"
  return raw.trim().split(' ')[0];
}

export function parseCSV(csvText: string): PricePaidRecord[] {
  const rows: string[][] = parse(csvText, {
    delimiter: ',',
    quote: '"',
    relax_column_count: true,
  });

  const records: PricePaidRecord[] = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (row.length < 16) {
      console.warn(`[Parse] Row ${i + 1}: only ${row.length} columns, skipping`);
      continue;
    }

    try {
      records.push({
        transaction_id: row[0].replace(/[{}]/g, '').trim(),
        price: parseInt(row[1].trim(), 10),
        transaction_date: parseDate(row[2]),
        postcode: emptyToNull(row[3]),
        property_type: emptyToNull(row[4]),
        new_build: row[5].trim().toUpperCase() === 'Y',
        tenure: emptyToNull(row[6]),
        paon: emptyToNull(row[7]),
        saon: emptyToNull(row[8]),
        street: emptyToNull(row[9]),
        locality: emptyToNull(row[10]),
        town: emptyToNull(row[11]),
        district: emptyToNull(row[12]),
        county: emptyToNull(row[13]),
        ppd_category: emptyToNull(row[14]),
        record_status: row[15].trim().toUpperCase(),
        country: 'gb',
        admin_level_1: 'england_wales',
      });
    } catch (err: any) {
      console.warn(`[Parse] Row ${i + 1} failed: ${err.message}`);
    }
  }

  console.log(`[Parse] Parsed ${records.length} records from CSV`);
  return records;
}
