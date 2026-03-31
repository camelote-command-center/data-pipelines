/**
 * Reads permits JSON from stdin/file and generates SQL INSERT statements
 * that can be executed via Supabase MCP.
 * Outputs batch SQL statements to stdout.
 */
import { readFileSync } from 'fs';

const data = JSON.parse(readFileSync('/tmp/fr-permits.json', 'utf-8'));
console.error(`Read ${data.length} permits from JSON`);

// Escape single quotes for SQL
function esc(val: string | null): string {
  if (val === null || val === undefined) return 'NULL';
  return "'" + val.replace(/'/g, "''") + "'";
}

function escJson(val: any): string {
  if (val === null || val === undefined) return 'NULL';
  return "'" + JSON.stringify(val).replace(/'/g, "''") + "'::jsonb";
}

// Generate SQL in batches of 50
const BATCH = 50;
for (let i = 0; i < data.length; i += BATCH) {
  const batch = data.slice(i, i + BATCH);
  const values = batch.map((r: any) =>
    `(${esc(r.source_id)}, ${esc(r.canton)}, ${esc(r.permit_type)}, ${esc(r.status)}, ${esc(r.description)}, ${esc(r.applicant)}, ${esc(r.owner)}, ${esc(r.commune)}, ${esc(r.address)}, ${esc(r.parcel_number)}, ${esc(r.zone)}, ${r.submission_date ? esc(r.submission_date) : 'NULL'}, ${r.publication_date ? esc(r.publication_date) : 'NULL'}, ${r.decision_date ? esc(r.decision_date) : 'NULL'}, ${r.display_start ? esc(r.display_start) : 'NULL'}, ${r.display_end ? esc(r.display_end) : 'NULL'}, ${r.geometry ? esc(r.geometry) : 'NULL'}, ${esc(r.source_url)}, ${esc(r.source_system)}, ${escJson(r.raw_data)})`
  ).join(',\n');

  const sql = `INSERT INTO bronze.sad_national (source_id, canton, permit_type, status, description, applicant, owner, commune, address, parcel_number, zone, submission_date, publication_date, decision_date, display_start, display_end, geometry, source_url, source_system, raw_data)
VALUES
${values}
ON CONFLICT (source_id, canton) DO UPDATE SET
  description = EXCLUDED.description,
  applicant = EXCLUDED.applicant,
  commune = EXCLUDED.commune,
  address = EXCLUDED.address,
  parcel_number = EXCLUDED.parcel_number,
  geometry = EXCLUDED.geometry,
  source_url = EXCLUDED.source_url,
  raw_data = EXCLUDED.raw_data;`;

  // Write each batch SQL to a separate file
  const batchNum = Math.floor(i / BATCH);
  const fs = require('fs');
  fs.writeFileSync(`/tmp/fr-batch-${batchNum}.sql`, sql);
}

const totalBatches = Math.ceil(data.length / BATCH);
console.error(`Generated ${totalBatches} SQL batch files in /tmp/fr-batch-*.sql`);
console.log(totalBatches); // output batch count to stdout
