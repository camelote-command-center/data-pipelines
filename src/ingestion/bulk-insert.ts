import pg from 'pg';

const BATCH_SIZE = 1000;

export interface InsertResult {
  inserted: number;
  failed: number;
  errors: string[];
}

// Extract host from SUPABASE_URL (e.g. https://xyz.supabase.co → db.xyz.supabase.co)
function getDbHost(): string {
  const url = process.env.SUPABASE_URL!;
  const ref = url.replace('https://', '').replace('.supabase.co', '');
  return `db.${ref}.supabase.co`;
}

let _pool: pg.Pool | null = null;
function getPool(): pg.Pool {
  if (!_pool) {
    _pool = new pg.Pool({
      host: getDbHost(),
      port: 5432,
      database: 'postgres',
      user: 'postgres',
      password: process.env.DB_PASSWORD || 'SUpolkmn098$',
      ssl: { rejectUnauthorized: false },
      max: 5,
    });
  }
  return _pool;
}

export async function bulkInsert(
  table: string,
  records: Record<string, any>[],
  ingestionLogId: string,
): Promise<InsertResult> {
  if (records.length === 0) return { inserted: 0, failed: 0, errors: [] };

  const pool = getPool();
  let inserted = 0;
  let failed = 0;
  const errors: string[] = [];

  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE).map((r) => ({
      ...r,
      ingestion_log_id: ingestionLogId,
      country: 'ae',
      admin_level_1: 'dubai',
    }));

    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(records.length / BATCH_SIZE);

    try {
      // Build parameterized INSERT from the first record's keys
      const columns = Object.keys(batch[0]);
      const colList = columns.map((c) => `"${c}"`).join(', ');

      // Build VALUES placeholders: ($1, $2, ...), ($N+1, $N+2, ...), ...
      const values: any[] = [];
      const rowPlaceholders: string[] = [];
      for (const row of batch) {
        const placeholders: string[] = [];
        for (const col of columns) {
          const val = (row as Record<string, any>)[col];
          values.push(col === 'raw_data' ? JSON.stringify(val) : val);
          placeholders.push(`$${values.length}`);
        }
        rowPlaceholders.push(`(${placeholders.join(', ')})`);
      }

      const sql = `INSERT INTO ${table} (${colList}) VALUES ${rowPlaceholders.join(', ')}`;
      await pool.query(sql, values);

      inserted += batch.length;
      console.log(`[BulkInsert] Batch ${batchNum}/${totalBatches}: ${batch.length} rows inserted`);
    } catch (err: any) {
      console.error(`[BulkInsert] Batch ${batchNum}/${totalBatches} failed: ${err.message}`);
      errors.push(`Batch ${batchNum}: ${err.message}`);
      failed += batch.length;
    }
  }

  console.log(`[BulkInsert] ${table}: inserted=${inserted} failed=${failed}`);
  return { inserted, failed, errors };
}

export async function closePool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
}
