/**
 * Lolla → listings_ads Promotion Script
 *
 * Replaces the broken SQL promote_lolla_batch() function.
 * Reads from bronze.lolla_ads, maps all fields correctly, inserts into
 * listings_ads + media table, and marks bronze rows as promoted.
 *
 * Usage:
 *   npx tsx promote.ts                  # promote 25 ads (default)
 *   BATCH_SIZE=50 npx tsx promote.ts    # promote 50
 *   FIX_EXISTING=1 npx tsx promote.ts   # re-sync already promoted listings
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { normalizePhone } from './shared.js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const ADMIN_USER_ID = '8a20194c-c560-49c8-bfb7-1d3f727ceba6';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '25', 10);
const FIX_EXISTING = process.env.FIX_EXISTING === '1';

const XOXO_URL = process.env.SUPABASE_URL;
const XOXO_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!XOXO_URL || !XOXO_KEY) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required');
  process.exit(1);
}

const xoxo: SupabaseClient = createClient(XOXO_URL, XOXO_KEY);

// ---------------------------------------------------------------------------
// Category mapping (lolla → helveti slug)
// ---------------------------------------------------------------------------

const CATEGORY_MAP: Record<string, string> = {
  'escort-girls': 'girls',
  'escort-trans': 'trans',
  'massage-sensuel': 'massage',
  'massage': 'massage',
  'webcams': 'tv',
  'sm-bdsm': 'sm',
  'sex-phone': 'girls',
  'location-d': 'salon',
};

// ---------------------------------------------------------------------------
// Ethnicity → origin_id mapping
// ---------------------------------------------------------------------------

const ORIGIN_MAP: Record<string, number> = {
  'européenne': 2,      // Caucasian
  'suisse': 1,          // Swiss
  'latino': 9,          // Latin
  'asiatique': 5,       // Asian
  'africaine': 6,       // African
  'ebony': 7,           // Ebony
  'arabe': 8,           // Middle Eastern
  'indienne': 5,        // Asian (closest)
  'métisse': 10,        // Other
  'brésilienne': 9,     // Latin
};

// ---------------------------------------------------------------------------
// Canton text → canton_id mapping (built dynamically)
// ---------------------------------------------------------------------------

let cantonMap: Map<string, number> = new Map();
let cityMap: Map<string, Map<string, number>> = new Map(); // canton_id → city_name → city_id

async function loadMappings() {
  // Load cantons
  const { data: cantons } = await xoxo.from('cantons').select('id, name');
  for (const c of cantons || []) {
    cantonMap.set(c.name.toLowerCase(), c.id);
  }

  // Load cities grouped by canton
  const { data: cities } = await xoxo.from('cities').select('id, name, canton_id');
  for (const c of cities || []) {
    if (!cityMap.has(String(c.canton_id))) {
      cityMap.set(String(c.canton_id), new Map());
    }
    cityMap.get(String(c.canton_id))!.set(c.name.toLowerCase(), c.id);
  }

  console.log(`  Loaded ${cantonMap.size} cantons, ${(cities || []).length} cities`);
}

// Lolla uses French canton names; DB uses English/German
const CANTON_ALIASES: Record<string, string> = {
  'genève': 'geneva',
  'neuchâtel': 'neuchatel',
  'zürich': 'zürich',
  'berne': 'bern',
  'lucerne': 'luzern',
  'graubünden': 'grisons',
  'tessin': 'ticino',
  'thurgovie': 'thurgau',
  'argovie': 'aargau',
  'schaffhouse': 'schaffhausen',
  'saint-gall': 'st. gallen',
};

function resolveCantonId(cantonText: string | null): number | null {
  if (!cantonText) return null;
  // Format: "Vaud (VD)" or just "Vaud"
  let name = cantonText.replace(/\s*\([^)]+\)\s*$/, '').trim().toLowerCase();
  // Try alias first
  if (CANTON_ALIASES[name]) name = CANTON_ALIASES[name];
  return cantonMap.get(name) ?? null;
}

function resolveCityId(cityName: string | null, cantonId: number | null): number | null {
  if (!cityName || !cantonId) return null;
  const cities = cityMap.get(String(cantonId));
  if (!cities) return null;
  const name = cityName.toLowerCase();
  // Direct match
  const direct = cities.get(name);
  if (direct) return direct;
  // Strip canton suffix: "Romont FR" → "Romont"
  const stripped = name.replace(/\s+[a-z]{2}$/i, '').trim();
  if (stripped !== name) {
    const match = cities.get(stripped);
    if (match) return match;
  }
  // Partial match: try to find a city that starts with our name
  for (const [cName, cId] of cities) {
    if (cName.startsWith(name) || name.startsWith(cName)) return cId;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Age text → integer
// ---------------------------------------------------------------------------

function parseAge(ageText: string | null): number | null {
  if (!ageText) return null;
  // "18-22 ans" → 20, "35+ ans" → 35, "25 ans" → 25
  const range = ageText.match(/(\d+)\s*-\s*(\d+)/);
  if (range) return Math.round((parseInt(range[1]) + parseInt(range[2])) / 2);
  const single = ageText.match(/(\d+)/);
  if (single) return parseInt(single[1]);
  return null;
}

// ---------------------------------------------------------------------------
// Height text → height_id
// ---------------------------------------------------------------------------

function parseHeightId(heightText: string | null): number | null {
  if (!heightText) return null;
  // "1.65 m" → 165cm → id 4 (161-170)
  const m = heightText.match(/(\d)\.(\d{2})/);
  if (!m) return null;
  const cm = parseInt(m[1]) * 100 + parseInt(m[2]);
  if (cm < 140) return 1;
  if (cm <= 150) return 2;
  if (cm <= 160) return 3;
  if (cm <= 170) return 4;
  if (cm <= 180) return 5;
  if (cm <= 190) return 6;
  if (cm <= 200) return 7;
  return 8; // 201+
}

// ---------------------------------------------------------------------------
// Languages array → spoken_languages format
// ---------------------------------------------------------------------------

function formatLanguages(langs: string[]): { language: string; rate: number }[] {
  return langs.map((l) => ({ language: l, rate: 3 })); // rate 3 = fluent default
}

// ---------------------------------------------------------------------------
// Build mobility array from incall/outcall
// ---------------------------------------------------------------------------

function buildMobility(incall: boolean, outcall: boolean): string[] {
  const m: string[] = [];
  if (incall) m.push('Incall');
  if (outcall) m.push('Outcall');
  return m;
}

// ---------------------------------------------------------------------------
// Generate slug
// ---------------------------------------------------------------------------

function generateSlug(nickname: string, city: string | null, id: string): string {
  const base = (nickname || 'profile')
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .slice(0, 40);
  const citySlug = city
    ? city.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-').slice(0, 20)
    : 'ch';
  return `${base}-${citySlug}-${id.slice(0, 8)}`;
}

// ---------------------------------------------------------------------------
// Main promote logic
// ---------------------------------------------------------------------------

async function promoteAds() {
  await loadMappings();

  let bronzeAds;

  if (FIX_EXISTING) {
    // Re-sync already promoted listings to fix missing data
    console.log('  FIX_EXISTING mode: re-syncing promoted listings');
    const { data, error } = await xoxo
      .schema('bronze')
      .from('lolla_ads')
      .select('*')
      .not('promoted_at', 'is', null)
      .not('phone', 'is', null)
      .not('canton', 'is', null);
    if (error) { console.error(error); return; }
    bronzeAds = data || [];
  } else {
    // Find unpromoted ads with enough data
    const { data, error } = await xoxo
      .schema('bronze')
      .from('lolla_ads')
      .select('*')
      .is('promoted_at', null)
      .not('phone', 'is', null)
      .not('canton', 'is', null)
      .not('category', 'eq', 'webcams')  // skip webcams - no useful data
      .limit(BATCH_SIZE);
    if (error) { console.error(error); return; }
    bronzeAds = data || [];
  }

  console.log(`  Found ${bronzeAds.length} ads to ${FIX_EXISTING ? 'fix' : 'promote'}`);

  let promoted = 0;
  let skippedNoCity = 0;
  let skippedNoCat = 0;

  for (const ad of bronzeAds) {
    const cantonId = resolveCantonId(ad.canton);
    if (!cantonId) { skippedNoCity++; continue; }

    const cityId = resolveCityId(ad.city, cantonId);
    if (!cityId) { skippedNoCity++; continue; }

    const category = CATEGORY_MAP[ad.category];
    if (!category) { skippedNoCat++; continue; }

    const phone = normalizePhone(ad.phone);
    if (!phone) continue;

    // Clean whatsapp: re-normalize from phone (since bronze data is corrupted)
    // For now, just use the phone number as whatsapp if whatsapp field exists
    const whatsapp = ad.whatsapp ? phone : null;

    const age = parseAge(ad.age);
    const heightId = parseHeightId(ad.height);
    const originId = ORIGIN_MAP[(ad.ethnicity || '').toLowerCase()] ?? null;

    if (FIX_EXISTING && ad.promoted_listing_id) {
      // Update existing listing
      const listingId = ad.promoted_listing_id;

      const { error: updateErr } = await xoxo
        .from('listings_ads')
        .update({
          professional_phone: phone,
          whatsapp_number: whatsapp,
          age,
          height_id: heightId,
          origin_id: originId,
          spoken_languages: formatLanguages(ad.languages || []),
          mobility: buildMobility(ad.incall, ad.outcall),
          services: ad.services || [],
          avatar_url: ad.photo_urls?.[0] || null,
          description: ad.description,
          category,
          canton_id: cantonId,
          city_id: cityId,
          updated_at: new Date().toISOString(),
        })
        .eq('id', listingId);

      if (updateErr) {
        console.error(`  Update listing ${listingId} error: ${updateErr.message}`);
        continue;
      }

      // Ensure media rows exist
      await ensureMedia(listingId, ad.photo_urls || []);
      promoted++;
    } else {
      // Insert new listing
      const { data: inserted, error: insertErr } = await xoxo
        .from('listings_ads')
        .insert({
          nickname: ad.nickname,
          professional_phone: phone,
          whatsapp_number: whatsapp,
          description: ad.description,
          category,
          canton_id: cantonId,
          city_id: cityId,
          age,
          height_id: heightId,
          origin_id: originId,
          spoken_languages: formatLanguages(ad.languages || []),
          mobility: buildMobility(ad.incall, ad.outcall),
          services: ad.services || [],
          avatar_url: ad.photo_urls?.[0] || null,
          status: 'active',
          approved: true,
          hidden: false,
          owner_id: ADMIN_USER_ID,
          slug: '', // placeholder, set after we have the ID
        })
        .select('id')
        .single();

      if (insertErr) {
        console.error(`  Insert listing error: ${insertErr.message}`);
        continue;
      }

      const listingId = inserted.id;

      // Set proper slug with the generated ID
      const slug = generateSlug(ad.nickname || '', ad.city, listingId);
      await xoxo
        .from('listings_ads')
        .update({ slug })
        .eq('id', listingId);

      // Insert media rows (photos)
      await ensureMedia(listingId, ad.photo_urls || []);

      // Mark bronze row as promoted
      await xoxo
        .schema('bronze')
        .from('lolla_ads')
        .update({
          promoted_at: new Date().toISOString(),
          promoted_listing_id: listingId,
        })
        .eq('lolla_id', ad.lolla_id);

      promoted++;
    }

    if (promoted % 10 === 0) {
      console.log(`  Progress: ${promoted}/${bronzeAds.length}`);
    }
  }

  console.log(`\n  Done: ${promoted} ${FIX_EXISTING ? 'fixed' : 'promoted'}, ${skippedNoCity} skipped (no city), ${skippedNoCat} skipped (no category)`);
  // Output JSON for workflow parsing
  console.log(JSON.stringify({ promoted_count: promoted, skipped_no_city: skippedNoCity, skipped_no_category: skippedNoCat }));
}

// ---------------------------------------------------------------------------
// Media helper: ensure photos exist in media table
// ---------------------------------------------------------------------------

async function ensureMedia(listingId: string, photoUrls: string[]) {
  if (!photoUrls || photoUrls.length === 0) return;

  // Check existing media for this listing
  const { data: existing } = await xoxo
    .from('media')
    .select('url')
    .eq('listing_id', listingId);

  const existingUrls = new Set((existing || []).map((m: any) => m.url));

  const newPhotos = photoUrls.filter((url) => !existingUrls.has(url));
  if (newPhotos.length === 0) return;

  const isFirstPhoto = (existing || []).length === 0;

  const rows = newPhotos.map((url, i) => ({
    listing_id: listingId,
    profile_id: null,
    type: (isFirstPhoto && i === 0) ? 'avatar' : 'photo',
    url,
    status: 'approved',
  }));

  const { error } = await xoxo
    .from('media')
    .insert(rows);

  if (error) {
    console.error(`  Media insert error for ${listingId}: ${error.message}`);
  }
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

promoteAds().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
