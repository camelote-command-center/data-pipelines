import { readFileSync, writeFileSync } from 'fs';

const data = JSON.parse(readFileSync('/tmp/fr-permits.json', 'utf-8'));

function esc(val: string | null): string {
  if (val === null || val === undefined) return 'NULL';
  return "'" + val.replace(/'/g, "''") + "'";
}
function escJson(val: any): string {
  if (val === null || val === undefined) return 'NULL';
  return "'" + JSON.stringify(val).replace(/\\/g, '\\\\').replace(/'/g, "''") + "'::jsonb";
}

const BATCH = 10;
const totalBatches = Math.ceil(data.length / BATCH);

for (let i = 0; i < data.length; i += BATCH) {
  const batch = data.slice(i, i + BATCH);
  const values = batch.map((r: any) =>
    `(${esc(r.source_id)}, ${esc(r.canton)}, ${esc(r.permit_type)}, ${esc(r.status)}, ${esc(r.description)}, ${esc(r.applicant)}, ${esc(r.owner)}, ${esc(r.commune)}, ${esc(r.address)}, ${esc(r.parcel_number)}, ${esc(r.zone)}, ${r.submission_date ? esc(r.submission_date) : 'NULL'}, ${r.publication_date ? esc(r.publication_date) : 'NULL'}, ${r.decision_date ? esc(r.decision_date) : 'NULL'}, ${r.display_start ? esc(r.display_start) : 'NULL'}, ${r.display_end ? esc(r.display_end) : 'NULL'}, ${r.geometry ? esc(r.geometry) : 'NULL'}, ${esc(r.source_url)}, ${esc(r.source_system)}, ${escJson(r.raw_data)})`
  ).join(',\n');

  const sql = `INSERT INTO bronze.sad_national (source_id, canton, permit_type, status, description, applicant, owner, commune, address, parcel_number, zone, submission_date, publication_date, decision_date, display_start, display_end, geometry, source_url, source_system, raw_data)
VALUES ${values}
ON CONFLICT (source_id, canton) DO UPDATE SET description=EXCLUDED.description, applicant=EXCLUDED.applicant, commune=EXCLUDED.commune, address=EXCLUDED.address, parcel_number=EXCLUDED.parcel_number, geometry=EXCLUDED.geometry, source_url=EXCLUDED.source_url, raw_data=EXCLUDED.raw_data;`;

  writeFileSync(`/tmp/fr-b${Math.floor(i / BATCH)}.sql`, sql);
}

console.log(totalBatches);
