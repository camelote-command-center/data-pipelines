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
console.log('Connected');

const wrappers = [
  `CREATE OR REPLACE FUNCTION public.start_ingestion(
    source text, emirate text, date_from date, date_to date, metadata_jsonb jsonb DEFAULT '{}'
  ) RETURNS uuid LANGUAGE sql AS $$
    SELECT bronze_ae.start_ingestion(source, emirate, date_from, date_to, metadata_jsonb);
  $$`,

  `CREATE OR REPLACE FUNCTION public.complete_ingestion(
    log_id uuid, p_records_fetched integer, p_records_inserted integer,
    p_records_updated integer, p_records_failed integer, p_error text DEFAULT NULL
  ) RETURNS void LANGUAGE sql AS $$
    SELECT bronze_ae.complete_ingestion(log_id, p_records_fetched, p_records_inserted, p_records_updated, p_records_failed, p_error);
  $$`,

  `CREATE OR REPLACE FUNCTION public.dedup_transactions()
  RETURNS void LANGUAGE sql AS $$
    SELECT bronze_ae.dedup_transactions();
  $$`,

  `CREATE OR REPLACE FUNCTION public.dedup_rentals()
  RETURNS void LANGUAGE sql AS $$
    SELECT bronze_ae.dedup_rentals();
  $$`,

  `CREATE OR REPLACE FUNCTION public.promote_transactions(since timestamptz)
  RETURNS void LANGUAGE sql AS $$
    SELECT silver_ae.promote_transactions(since);
  $$`,

  `CREATE OR REPLACE FUNCTION public.promote_rentals(since timestamptz)
  RETURNS void LANGUAGE sql AS $$
    SELECT silver_ae.promote_rentals(since);
  $$`,

  `CREATE OR REPLACE FUNCTION public.refresh_market_data_from_transactions(months_back integer)
  RETURNS void LANGUAGE sql AS $$
    SELECT gold_ae.refresh_market_data_from_transactions(months_back);
  $$`,

  `CREATE OR REPLACE FUNCTION public.refresh_market_data_from_rentals(months_back integer)
  RETURNS void LANGUAGE sql AS $$
    SELECT gold_ae.refresh_market_data_from_rentals(months_back);
  $$`,
];

for (const sql of wrappers) {
  const name = sql.match(/public\.(\w+)/)?.[1];
  try {
    await client.query(sql);
    console.log(`  OK: public.${name}`);
  } catch (e) {
    console.error(`  FAIL: public.${name}: ${e.message}`);
  }
}

await client.end();
console.log('Done');
