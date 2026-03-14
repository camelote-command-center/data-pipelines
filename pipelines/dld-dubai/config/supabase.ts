import { createClient, SupabaseClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables');
}

// Use public schema for all PostgREST operations.
// RPCs are public wrappers that delegate to bronze_ae/silver_ae/gold_ae.
// Table inserts go through the REST API with explicit schema header.
export const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

// Direct Postgres connection details for raw SQL if needed
export const SUPABASE_URL_VALUE = SUPABASE_URL;
export const SUPABASE_KEY_VALUE = SUPABASE_SERVICE_ROLE_KEY;
