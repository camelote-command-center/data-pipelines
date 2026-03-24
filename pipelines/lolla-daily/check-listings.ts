/**
 * Check promoted listings against lolla.ch
 *
 * - Fetches each listing's source_url on lolla.ch
 * - If 404/gone: hides the listing (hidden=true)
 * - If still live: checks if city/canton changed, updates if so
 *
 * Usage:
 *   npx tsx check-listings.ts
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';
import * as cheerio from 'cheerio';

const XOXO_URL = process.env.SUPABASE_URL;
const XOXO_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!XOXO_URL || !XOXO_KEY) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required');
  process.exit(1);
}

const xoxo: SupabaseClient = createClient(XOXO_URL, XOXO_KEY);

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// Load canton/city mappings
let cantonMap: Map<string, number> = new Map();
let cityMap: Map<string, Map<string, number>> = new Map();

const CANTON_ALIASES: Record<string, string> = {
  'genève': 'geneva', 'neuchâtel': 'neuchatel', 'berne': 'bern',
  'lucerne': 'luzern', 'tessin': 'ticino', 'thurgovie': 'thurgau',
  'argovie': 'aargau', 'schaffhouse': 'schaffhausen', 'saint-gall': 'st. gallen',
};

async function loadMappings() {
  const { data: cantons } = await xoxo.from('cantons').select('id, name');
  for (const c of cantons || []) cantonMap.set(c.name.toLowerCase(), c.id);

  const { data: cities } = await xoxo.from('cities').select('id, name, canton_id');
  for (const c of cities || []) {
    const key = String(c.canton_id);
    if (!cityMap.has(key)) cityMap.set(key, new Map());
    cityMap.get(key)!.set(c.name.toLowerCase(), c.id);
  }
}

function resolveCantonId(cantonText: string | null): number | null {
  if (!cantonText) return null;
  let name = cantonText.replace(/\s*\([^)]+\)\s*$/, '').trim().toLowerCase();
  if (CANTON_ALIASES[name]) name = CANTON_ALIASES[name];
  return cantonMap.get(name) ?? null;
}

function resolveCityId(cityName: string | null, cantonId: number | null): number | null {
  if (!cityName || !cantonId) return null;
  const cities = cityMap.get(String(cantonId));
  if (!cities) return null;
  const name = cityName.toLowerCase();
  return cities.get(name) ?? cities.get(name.replace(/\s+[a-z]{2}$/i, '').trim()) ?? null;
}

async function main() {
  console.log('=== Checking promoted listings against lolla.ch ===\n');
  await loadMappings();

  // Get all promoted bronze ads
  const { data: bronzeAds, error } = await xoxo
    .schema('bronze')
    .from('lolla_ads')
    .select('lolla_id, source_url, promoted_listing_id, canton, city')
    .not('promoted_listing_id', 'is', null);

  if (error) { console.error(error); return; }
  console.log(`Found ${bronzeAds.length} promoted listings to check\n`);

  let hidden = 0;
  let updated = 0;
  let stillLive = 0;
  let errors = 0;

  for (let i = 0; i < bronzeAds.length; i++) {
    const ad = bronzeAds[i];
    const listingId = ad.promoted_listing_id;

    try {
      const res = await fetch(ad.source_url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
          'Accept': 'text/html',
        },
        redirect: 'follow',
      });

      if (!res.ok || res.status === 404) {
        // Ad is gone - hide the listing
        console.log(`  [${i + 1}/${bronzeAds.length}] GONE (${res.status}): ${ad.source_url.slice(-50)}`);
        await xoxo
          .from('listings_ads')
          .update({ hidden: true, status: 'inactive' })
          .eq('id', listingId);
        hidden++;
      } else {
        // Ad still live - check if city/canton changed
        const html = await res.text();

        // Check if page is actually an ad (not a redirect to homepage)
        if (!html.includes('AdOnePseudo') && !html.includes('ItemView')) {
          console.log(`  [${i + 1}/${bronzeAds.length}] GONE (redirect): ${ad.source_url.slice(-50)}`);
          await xoxo
            .from('listings_ads')
            .update({ hidden: true, status: 'inactive' })
            .eq('id', listingId);
          hidden++;
        } else {
          const $ = cheerio.load(html);
          const cantonText = $('.AdOneCityCantonText').first().text().trim() || null;
          const cityText = $('.AdOneCityName').first().text().trim() || null;

          const newCantonId = resolveCantonId(cantonText);
          const newCityId = resolveCityId(cityText, newCantonId);

          // Check if location changed
          const oldCantonId = resolveCantonId(ad.canton);
          const oldCityId = resolveCityId(ad.city, oldCantonId);

          if (newCantonId && newCityId && (newCantonId !== oldCantonId || newCityId !== oldCityId)) {
            console.log(`  [${i + 1}/${bronzeAds.length}] MOVED: ${ad.canton}/${ad.city} → ${cantonText}/${cityText}`);
            await xoxo
              .from('listings_ads')
              .update({ canton_id: newCantonId, city_id: newCityId })
              .eq('id', listingId);

            // Also update bronze
            await xoxo
              .schema('bronze')
              .from('lolla_ads')
              .update({ canton: cantonText, city: cityText })
              .eq('lolla_id', ad.lolla_id);

            updated++;
          } else {
            stillLive++;
          }
        }
      }
    } catch (err) {
      console.error(`  [${i + 1}/${bronzeAds.length}] ERROR: ${err}`);
      errors++;
    }

    await sleep(800);
  }

  console.log(`\n=== Check Complete ===`);
  console.log(`Still live: ${stillLive}`);
  console.log(`Hidden (gone): ${hidden}`);
  console.log(`Updated (moved): ${updated}`);
  console.log(`Errors: ${errors}`);
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
