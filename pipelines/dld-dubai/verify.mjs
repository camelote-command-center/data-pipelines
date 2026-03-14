import pg from 'pg';
const { Client } = pg;

const client = new Client({
  host: 'db.znrvddgmczdqoucmykij.supabase.co',
  port: 5432,
  database: 'postgres',
  user: 'postgres',
  password: 'SUpolkmn098$',
  ssl: { rejectUnauthorized: false },
});

await client.connect();

// Check transactions columns
const r = await client.query("SELECT column_name FROM information_schema.columns WHERE table_schema='bronze_ae' AND table_name='transactions' ORDER BY ordinal_position");
console.log('transactions columns:', r.rows.map(x => x.column_name));

// Test start_ingestion
const t = await client.query("SELECT bronze_ae.start_ingestion('test', 'dubai', '2025-01-01', '2025-01-01', '{}'::jsonb)");
console.log('start_ingestion returned:', t.rows[0]);

// Clean up
await client.query("DELETE FROM bronze_ae.ingestion_log WHERE source = 'test'");
console.log('Cleaned up test row');

// Count existing transactions
const c2 = await client.query("SELECT count(*) FROM bronze_ae.transactions");
console.log('Existing transaction count:', c2.rows[0].count);

await client.end();
