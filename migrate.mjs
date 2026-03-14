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

async function run(label, sql) {
  console.log(`\n--- ${label} ---`);
  try {
    await client.query(sql);
    console.log(`  OK`);
  } catch (err) {
    console.error(`  ERROR: ${err.message}`);
    throw err;
  }
}

async function main() {
  await client.connect();
  console.log('Connected to:', (await client.query('SELECT current_database()')).rows[0].current_database);

  // ===== PART A: SCHEMAS =====
  await run('Part A: Create schemas', `
    CREATE SCHEMA IF NOT EXISTS bronze_ae;
    CREATE SCHEMA IF NOT EXISTS silver_ae;
    CREATE SCHEMA IF NOT EXISTS gold_ae;
    CREATE SCHEMA IF NOT EXISTS knowledge_ae;
    CREATE SCHEMA IF NOT EXISTS ref;
  `);

  // ===== PART B: TABLES =====
  await run('Part B.1: ingestion_log', `
    CREATE TABLE IF NOT EXISTS bronze_ae.ingestion_log (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      source text NOT NULL,
      emirate text,
      date_from date,
      date_to date,
      metadata jsonb DEFAULT '{}',
      status text DEFAULT 'running',
      records_fetched integer DEFAULT 0,
      records_inserted integer DEFAULT 0,
      records_updated integer DEFAULT 0,
      records_failed integer DEFAULT 0,
      error_message text,
      started_at timestamptz DEFAULT now(),
      completed_at timestamptz
    );
  `);

  await run('Part B.2: transactions', `
    CREATE TABLE IF NOT EXISTS bronze_ae.transactions (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      transaction_number text,
      transaction_date date,
      transaction_type text,
      transaction_sub_type text,
      registration_type text,
      is_freehold boolean,
      usage text,
      area text,
      property_type text,
      property_sub_type text,
      amount numeric,
      transaction_size_sqm numeric,
      property_size_sqm numeric,
      rooms text,
      parking text,
      nearest_metro text,
      nearest_mall text,
      nearest_landmark text,
      buyer_count integer,
      seller_count integer,
      master_project text,
      project text
    );
    CREATE INDEX IF NOT EXISTS idx_transactions_date ON bronze_ae.transactions(transaction_date);
    CREATE INDEX IF NOT EXISTS idx_transactions_area ON bronze_ae.transactions(area);
    CREATE INDEX IF NOT EXISTS idx_transactions_log ON bronze_ae.transactions(ingestion_log_id);
  `);

  await run('Part B.3: rentals', `
    CREATE TABLE IF NOT EXISTS bronze_ae.rentals (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      registration_date date,
      start_date date,
      end_date date,
      version text,
      area text,
      contract_amount numeric,
      annual_amount numeric,
      is_freehold boolean,
      property_size_sqm numeric,
      property_type text,
      property_sub_type text,
      rooms text,
      usage text,
      nearest_metro text,
      nearest_mall text,
      nearest_landmark text,
      parking text,
      unit_count integer,
      master_project text,
      project text
    );
    CREATE INDEX IF NOT EXISTS idx_rentals_reg_date ON bronze_ae.rentals(registration_date);
    CREATE INDEX IF NOT EXISTS idx_rentals_area ON bronze_ae.rentals(area);
    CREATE INDEX IF NOT EXISTS idx_rentals_log ON bronze_ae.rentals(ingestion_log_id);
  `);

  await run('Part B.4: projects', `
    CREATE TABLE IF NOT EXISTS bronze_ae.projects (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      project_name text,
      master_project text,
      developer text,
      area text,
      status text,
      start_date date,
      completion_date date,
      percentage_completed numeric,
      is_freehold boolean,
      usage text
    );
    CREATE INDEX IF NOT EXISTS idx_projects_area ON bronze_ae.projects(area);
    CREATE INDEX IF NOT EXISTS idx_projects_log ON bronze_ae.projects(ingestion_log_id);
  `);

  await run('Part B.5: valuations', `
    CREATE TABLE IF NOT EXISTS bronze_ae.valuations (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      valuation_date date,
      area text,
      property_type text,
      property_sub_type text,
      usage text,
      valuation_amount numeric,
      property_size_sqm numeric,
      master_project text,
      project text
    );
    CREATE INDEX IF NOT EXISTS idx_valuations_date ON bronze_ae.valuations(valuation_date);
    CREATE INDEX IF NOT EXISTS idx_valuations_log ON bronze_ae.valuations(ingestion_log_id);
  `);

  await run('Part B.6: properties_land', `
    CREATE TABLE IF NOT EXISTS bronze_ae.properties_land (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      land_number text,
      area text,
      land_type text,
      land_size_sqm numeric,
      is_freehold boolean,
      usage text,
      master_project text,
      project text
    );
    CREATE INDEX IF NOT EXISTS idx_land_area ON bronze_ae.properties_land(area);
    CREATE INDEX IF NOT EXISTS idx_land_log ON bronze_ae.properties_land(ingestion_log_id);
  `);

  await run('Part B.7: properties_buildings', `
    CREATE TABLE IF NOT EXISTS bronze_ae.properties_buildings (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      building_name text,
      area text,
      building_type text,
      floors integer,
      units_count integer,
      is_freehold boolean,
      usage text,
      master_project text,
      project text,
      nearest_metro text,
      nearest_mall text,
      nearest_landmark text
    );
    CREATE INDEX IF NOT EXISTS idx_buildings_area ON bronze_ae.properties_buildings(area);
    CREATE INDEX IF NOT EXISTS idx_buildings_log ON bronze_ae.properties_buildings(ingestion_log_id);
  `);

  await run('Part B.8: properties_units', `
    CREATE TABLE IF NOT EXISTS bronze_ae.properties_units (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      unit_number text,
      building_name text,
      area text,
      unit_type text,
      unit_sub_type text,
      unit_size_sqm numeric,
      rooms text,
      parking text,
      floor text,
      is_freehold boolean,
      usage text,
      master_project text,
      project text
    );
    CREATE INDEX IF NOT EXISTS idx_units_area ON bronze_ae.properties_units(area);
    CREATE INDEX IF NOT EXISTS idx_units_log ON bronze_ae.properties_units(ingestion_log_id);
  `);

  await run('Part B.9: brokers', `
    CREATE TABLE IF NOT EXISTS bronze_ae.brokers (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      broker_name text,
      broker_name_ar text,
      license_number text,
      license_type text,
      license_expiry date,
      company text,
      status text
    );
    CREATE INDEX IF NOT EXISTS idx_brokers_log ON bronze_ae.brokers(ingestion_log_id);
  `);

  await run('Part B.10: developers', `
    CREATE TABLE IF NOT EXISTS bronze_ae.developers (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      developer_name text,
      developer_name_ar text,
      license_number text,
      status text
    );
    CREATE INDEX IF NOT EXISTS idx_developers_log ON bronze_ae.developers(ingestion_log_id);
  `);

  await run('Part B.11: rental_index', `
    CREATE TABLE IF NOT EXISTS bronze_ae.rental_index (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      area text,
      year integer,
      quarter text,
      property_type text,
      usage text,
      index_value numeric,
      average_rent numeric
    );
    CREATE INDEX IF NOT EXISTS idx_rental_index_log ON bronze_ae.rental_index(ingestion_log_id);
  `);

  await run('Part B.12: sales_index', `
    CREATE TABLE IF NOT EXISTS bronze_ae.sales_index (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now(),
      country text DEFAULT 'ae',
      admin_level_1 text DEFAULT 'dubai',
      raw_data jsonb DEFAULT '{}',
      ingestion_log_id uuid REFERENCES bronze_ae.ingestion_log(id),
      area text,
      year integer,
      quarter text,
      property_type text,
      usage text,
      index_value numeric,
      average_price numeric
    );
    CREATE INDEX IF NOT EXISTS idx_sales_index_log ON bronze_ae.sales_index(ingestion_log_id);
  `);

  // ===== PART C: DROP & RECREATE FUNCTIONS =====
  await run('Part C.0: Drop existing functions', `
    DROP FUNCTION IF EXISTS bronze_ae.start_ingestion(text, text, date, date, jsonb);
    DROP FUNCTION IF EXISTS bronze_ae.complete_ingestion(uuid, integer, integer, integer, integer, text);
    DROP FUNCTION IF EXISTS bronze_ae.dedup_transactions();
    DROP FUNCTION IF EXISTS bronze_ae.dedup_rentals();
    DROP FUNCTION IF EXISTS silver_ae.promote_transactions(timestamptz);
    DROP FUNCTION IF EXISTS silver_ae.promote_rentals(timestamptz);
    DROP FUNCTION IF EXISTS gold_ae.refresh_market_data_from_transactions(integer);
    DROP FUNCTION IF EXISTS gold_ae.refresh_market_data_from_rentals(integer);
  `);

  await run('Part C.1: start_ingestion', `
    CREATE OR REPLACE FUNCTION bronze_ae.start_ingestion(
      source text,
      emirate text,
      date_from date,
      date_to date,
      metadata_jsonb jsonb DEFAULT '{}'
    ) RETURNS uuid
    LANGUAGE plpgsql AS $$
    DECLARE
      log_id uuid;
    BEGIN
      INSERT INTO bronze_ae.ingestion_log (source, emirate, date_from, date_to, metadata, status, started_at)
      VALUES (source, emirate, date_from, date_to, metadata_jsonb, 'running', now())
      RETURNING id INTO log_id;
      RETURN log_id;
    END;
    $$;
  `);

  await run('Part C.2: complete_ingestion', `
    CREATE OR REPLACE FUNCTION bronze_ae.complete_ingestion(
      log_id uuid,
      p_records_fetched integer,
      p_records_inserted integer,
      p_records_updated integer,
      p_records_failed integer,
      p_error text DEFAULT NULL
    ) RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
      UPDATE bronze_ae.ingestion_log SET
        status = CASE WHEN p_error IS NOT NULL THEN 'failed' ELSE 'completed' END,
        records_fetched = p_records_fetched,
        records_inserted = p_records_inserted,
        records_updated = p_records_updated,
        records_failed = p_records_failed,
        error_message = p_error,
        completed_at = now(),
        updated_at = now()
      WHERE id = log_id;
    END;
    $$;
  `);

  await run('Part C.3: dedup_transactions', `
    CREATE OR REPLACE FUNCTION bronze_ae.dedup_transactions()
    RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
      DELETE FROM bronze_ae.transactions a
      USING bronze_ae.transactions b
      WHERE a.id < b.id
        AND a.transaction_number IS NOT DISTINCT FROM b.transaction_number
        AND a.transaction_date IS NOT DISTINCT FROM b.transaction_date;
    END;
    $$;
  `);

  await run('Part C.4: dedup_rentals', `
    CREATE OR REPLACE FUNCTION bronze_ae.dedup_rentals()
    RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
      DELETE FROM bronze_ae.rentals a
      USING bronze_ae.rentals b
      WHERE a.id < b.id
        AND a.registration_date IS NOT DISTINCT FROM b.registration_date
        AND a.area IS NOT DISTINCT FROM b.area
        AND a.contract_amount IS NOT DISTINCT FROM b.contract_amount
        AND a.start_date IS NOT DISTINCT FROM b.start_date;
    END;
    $$;
  `);

  await run('Part C.5: promote_transactions (stub)', `
    CREATE OR REPLACE FUNCTION silver_ae.promote_transactions(since timestamptz)
    RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
      RAISE NOTICE 'promote_transactions stub — since: %', since;
    END;
    $$;
  `);

  await run('Part C.6: promote_rentals (stub)', `
    CREATE OR REPLACE FUNCTION silver_ae.promote_rentals(since timestamptz)
    RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
      RAISE NOTICE 'promote_rentals stub — since: %', since;
    END;
    $$;
  `);

  await run('Part C.7: refresh_market_data_from_transactions (stub)', `
    CREATE OR REPLACE FUNCTION gold_ae.refresh_market_data_from_transactions(months_back integer)
    RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
      RAISE NOTICE 'refresh_market_data_from_transactions stub — months_back: %', months_back;
    END;
    $$;
  `);

  await run('Part C.8: refresh_market_data_from_rentals (stub)', `
    CREATE OR REPLACE FUNCTION gold_ae.refresh_market_data_from_rentals(months_back integer)
    RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
      RAISE NOTICE 'refresh_market_data_from_rentals stub — months_back: %', months_back;
    END;
    $$;
  `);

  // ===== VERIFY =====
  console.log('\n===== VERIFICATION =====');

  const schemas = await client.query(`
    SELECT schema_name FROM information_schema.schemata
    WHERE schema_name IN ('bronze_ae','silver_ae','gold_ae','knowledge_ae','ref')
    ORDER BY 1
  `);
  console.log('\nSchemas:', schemas.rows.map(r => r.schema_name));

  const tables = await client.query(`
    SELECT table_schema, table_name FROM information_schema.tables
    WHERE table_schema = 'bronze_ae'
    ORDER BY 2
  `);
  console.log('\nTables:');
  tables.rows.forEach(r => console.log(`  ${r.table_schema}.${r.table_name}`));

  const funcs = await client.query(`
    SELECT routine_schema, routine_name FROM information_schema.routines
    WHERE routine_schema IN ('bronze_ae','silver_ae','gold_ae')
    ORDER BY 1, 2
  `);
  console.log('\nFunctions:');
  funcs.rows.forEach(r => console.log(`  ${r.routine_schema}.${r.routine_name}`));

  await client.end();
  console.log('\nMigration complete!');
}

main().catch(err => { console.error(err); process.exit(1); });
