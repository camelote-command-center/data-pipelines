/**
 * NE SITN WFS Pipeline — Neuchatel Building Permits
 *
 * Fetches building permits from the SITN (Systeme d'information du territoire
 * neuchatelois) WFS service and upserts them into bronze.sad_national.
 *
 * Two layers are fetched:
 *   - at034_autorisation_construire_pendant  (pending / enquete publique)
 *   - at034_autorisation_construire_apres    (after decision)
 *
 * The WFS service returns GML3 in EPSG:2056 (Swiss LV95). Coordinates are
 * reprojected to EPSG:4326 (WGS84) before insertion.
 *
 * Environment variables:
 *   SUPABASE_URL              - Supabase project URL (required)
 *   SUPABASE_SERVICE_ROLE_KEY - service_role key     (required)
 */

import { createClient } from '@supabase/supabase-js';
import { XMLParser } from 'fast-xml-parser';
import proj4 from 'proj4';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const WFS_BASE_URL =
  'https://sitn.ne.ch/services/wms?service=WFS&version=1.1.0&request=GetFeature';

const LAYERS = [
  { name: 'at034_autorisation_construire_pendant', status: 'pendant' },
  { name: 'at034_autorisation_construire_apres', status: 'apres_decision' },
] as const;

const CANTON = 'NE';
const SOURCE_SYSTEM = 'sitn_wfs';
const BATCH_SIZE = 100;
const RATE_LIMIT_MS = 500;

// ---------------------------------------------------------------------------
// Supabase client
// ---------------------------------------------------------------------------

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// ---------------------------------------------------------------------------
// proj4 — EPSG:2056 (Swiss LV95) definition
// ---------------------------------------------------------------------------

proj4.defs(
  'EPSG:2056',
  '+proj=somerc +lat_0=46.9524055555556 +lon_0=7.43958333333333 ' +
    '+k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel ' +
    '+towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs',
);

function convertToWGS84(easting: number, northing: number): [number, number] {
  const [lon, lat] = proj4('EPSG:2056', 'EPSG:4326', [easting, northing]);
  return [lon, lat];
}

// ---------------------------------------------------------------------------
// XML parser
// ---------------------------------------------------------------------------

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  removeNSPrefix: true, // strips namespace prefixes (ms:cadastre -> cadastre)
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Extract the URL from the HTML-encoded lien_avis field.
 * The field contains something like:
 *   &lt;a href=&quot;https://sitn.ne.ch/satac_document?id=UUID&amp;type=at034&amp;doctype=avis&quot;&gt;Lien vers AVIS&lt;/a&gt;
 * After XML parsing the entities are already decoded, so we look for href="...".
 */
function extractUrlFromLienAvis(raw: unknown): string | null {
  if (!raw || typeof raw !== 'string') return null;
  const match = raw.match(/href="([^"]+)"/);
  if (match) return match[1];
  // Also try with single quotes
  const match2 = raw.match(/href='([^']+)'/);
  return match2 ? match2[1] : null;
}

/** Safely convert a value to a string or return null. */
function toStringOrNull(v: unknown): string | null {
  if (v === undefined || v === null || v === '') return null;
  return String(v);
}

/** Safely convert a value to an ISO date string (YYYY-MM-DD) or return null. */
function toDateOrNull(v: unknown): string | null {
  if (v === undefined || v === null || v === '') return null;
  const s = String(v).trim();
  // Already ISO format YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  return null;
}

// ---------------------------------------------------------------------------
// WFS fetching
// ---------------------------------------------------------------------------

interface RawFeature {
  [key: string]: unknown;
}

async function fetchWfsLayer(layerName: string): Promise<RawFeature[]> {
  const url = `${WFS_BASE_URL}&typeName=${layerName}`;
  console.log(`  Fetching ${layerName}...`);
  console.log(`  URL: ${url}`);

  const response = await fetch(url);
  if (!response.ok) {
    console.error(`  HTTP ${response.status} for ${layerName}`);
    return [];
  }

  const xml = await response.text();
  if (!xml || xml.trim().length === 0) {
    console.log(`  Empty response for ${layerName}`);
    return [];
  }

  const parsed = parser.parse(xml);

  // Navigate the GML structure:
  // FeatureCollection > featureMember (array or single) > <layerName> (the feature)
  const featureCollection = parsed?.FeatureCollection;
  if (!featureCollection) {
    console.log(`  No FeatureCollection found for ${layerName}`);
    return [];
  }

  let featureMembers = featureCollection.featureMember;
  if (!featureMembers) {
    console.log(`  No featureMember elements for ${layerName}`);
    return [];
  }

  // Normalise to array
  if (!Array.isArray(featureMembers)) {
    featureMembers = [featureMembers];
  }

  const features: RawFeature[] = [];
  for (const member of featureMembers) {
    // The feature is nested under the layer name (without namespace prefix
    // since removeNSPrefix is true)
    const feature = member[layerName];
    if (feature) {
      features.push(feature as RawFeature);
    }
  }

  console.log(`  Found ${features.length} features in ${layerName}`);
  return features;
}

// ---------------------------------------------------------------------------
// Feature mapping
// ---------------------------------------------------------------------------

interface SadNationalRecord {
  source_id: string;
  canton: string;
  permit_type: string | null;
  status: string;
  description: string | null;
  applicant: string | null;
  owner: string | null;
  commune: string | null;
  address: string | null;
  parcel_number: string | null;
  zone: string | null;
  submission_date: string | null;
  publication_date: string | null;
  decision_date: string | null;
  display_start: string | null;
  display_end: string | null;
  geometry: string | null;
  source_url: string | null;
  source_system: string;
  raw_data: Record<string, unknown>;
}

function extractCoordinates(
  feature: RawFeature,
): [number, number] | null {
  try {
    // geom > Point > pos (contains "easting northing")
    const geom = feature.geom as Record<string, unknown> | undefined;
    if (!geom) return null;

    const point = geom.Point as Record<string, unknown> | undefined;
    if (!point) return null;

    const pos = point.pos;
    if (!pos || typeof pos !== 'string') return null;

    const parts = pos.trim().split(/\s+/);
    if (parts.length < 2) return null;

    const easting = parseFloat(parts[0]);
    const northing = parseFloat(parts[1]);
    if (isNaN(easting) || isNaN(northing)) return null;

    return [easting, northing];
  } catch {
    return null;
  }
}

function buildRawData(feature: RawFeature): Record<string, unknown> {
  const raw: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(feature)) {
    // Skip geometry and internal attributes
    if (key === 'geom' || key.startsWith('@_')) continue;
    raw[key] = value;
  }
  return raw;
}

function mapFeature(
  feature: RawFeature,
  status: string,
): SadNationalRecord | null {
  const instanceId = toStringOrNull(feature.instance_id);
  if (!instanceId) {
    console.warn('  Skipping feature without instance_id');
    return null;
  }

  // Build description from type_construction and description_ouvrage
  const typeConstruction = toStringOrNull(feature.type_construction);
  const descriptionOuvrage = toStringOrNull(feature.description_ouvrage);
  let description: string | null = null;
  if (typeConstruction && descriptionOuvrage) {
    description = `${typeConstruction} — ${descriptionOuvrage}`;
  } else {
    description = typeConstruction || descriptionOuvrage;
  }

  // Convert coordinates
  let geometry: string | null = null;
  const coords = extractCoordinates(feature);
  if (coords) {
    const [lon, lat] = convertToWGS84(coords[0], coords[1]);
    geometry = JSON.stringify({ type: 'Point', coordinates: [lon, lat] });
  }

  // Extract source URL from lien_avis
  const sourceUrl = extractUrlFromLienAvis(feature.lien_avis);

  return {
    source_id: instanceId,
    canton: CANTON,
    permit_type: toStringOrNull(feature.demande_permis),
    status,
    description,
    applicant: null,
    owner: null,
    commune: toStringOrNull(feature.cadastre),
    address: toStringOrNull(feature.situation),
    parcel_number: null,
    zone: null,
    submission_date: null,
    publication_date: null,
    decision_date: null,
    display_start: toDateOrNull(feature.debut_enquete),
    display_end: toDateOrNull(feature.fin_enquete),
    geometry,
    source_url: sourceUrl,
    source_system: SOURCE_SYSTEM,
    raw_data: buildRawData(feature),
  };
}

// ---------------------------------------------------------------------------
// Upsert
// ---------------------------------------------------------------------------

async function upsertBatch(records: SadNationalRecord[]): Promise<number> {
  const { error, count } = await supabase
    .schema('bronze')
    .from('sad_national')
    .upsert(records, { onConflict: 'source_id,canton', count: 'exact' });

  if (error) {
    console.error(`  Upsert error: ${error.message}`);
    return 0;
  }

  return count ?? records.length;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  NE SITN WFS Pipeline — Neuchatel Building Permits');
  console.log('  Target: bronze.sad_national');
  console.log('='.repeat(60));

  const startTime = Date.now();
  let totalFetched = 0;
  let totalUpserted = 0;
  let totalErrors = 0;
  const allRecords: SadNationalRecord[] = [];

  // Fetch both layers
  for (let i = 0; i < LAYERS.length; i++) {
    const layer = LAYERS[i];

    if (i > 0) {
      console.log(`\n  Rate-limiting: waiting ${RATE_LIMIT_MS}ms...`);
      await sleep(RATE_LIMIT_MS);
    }

    const features = await fetchWfsLayer(layer.name);
    totalFetched += features.length;

    for (const feature of features) {
      const record = mapFeature(feature, layer.status);
      if (record) {
        allRecords.push(record);
      } else {
        totalErrors++;
      }
    }
  }

  console.log(`\n  Total features fetched: ${totalFetched}`);
  console.log(`  Mapped records (before dedup): ${allRecords.length}`);
  console.log(`  Mapping errors: ${totalErrors}`);

  // Deduplicate by source_id — same permit can appear in both layers.
  // Prefer "apres_decision" over "pendant" when duplicates exist.
  const deduped = new Map<string, SadNationalRecord>();
  for (const record of allRecords) {
    const existing = deduped.get(record.source_id);
    if (!existing || record.status === 'apres_decision') {
      deduped.set(record.source_id, record);
    }
  }
  const uniqueRecords = Array.from(deduped.values());
  const dupCount = allRecords.length - uniqueRecords.length;
  console.log(`  Duplicates removed: ${dupCount}`);
  console.log(`  Unique records: ${uniqueRecords.length}`);

  if (uniqueRecords.length === 0) {
    console.log('  No records to upsert. Exiting.');
    console.log('='.repeat(60));
    return;
  }

  // Upsert in batches
  console.log(`\n  Upserting ${uniqueRecords.length} records (batch size: ${BATCH_SIZE})...`);

  for (let i = 0; i < uniqueRecords.length; i += BATCH_SIZE) {
    const batch = uniqueRecords.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(uniqueRecords.length / BATCH_SIZE);

    const upserted = await upsertBatch(batch);
    totalUpserted += upserted;

    console.log(
      `  Batch ${batchNum}/${totalBatches}: ${upserted} rows upserted`,
    );
  }

  // Summary
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log(`\n${'='.repeat(60)}`);
  console.log('  IMPORT COMPLETE');
  console.log(`  Features fetched:  ${totalFetched}`);
  console.log(`  Records upserted:  ${totalUpserted}`);
  console.log(`  Errors:            ${totalErrors}`);
  console.log(`  Duration:          ${elapsed}s`);
  console.log('='.repeat(60));

  if (totalUpserted === 0 && allRecords.length > 0) {
    console.error('  FAILED: Zero rows upserted despite having records!');
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
