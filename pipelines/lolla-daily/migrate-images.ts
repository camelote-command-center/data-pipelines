/**
 * Lolla Image Migration
 *
 * Downloads images from lolla.ch and uploads to Supabase Storage
 * with SEO-friendly filenames. Updates media rows + avatar_url.
 *
 * Lolla hotlink-protects images (serves empty body to external referrers),
 * so we must self-host via Supabase Storage.
 *
 * Usage:
 *   npx tsx migrate-images.ts
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';

const XOXO_URL = process.env.SUPABASE_URL;
const XOXO_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!XOXO_URL || !XOXO_KEY) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required');
  process.exit(1);
}

const xoxo: SupabaseClient = createClient(XOXO_URL, XOXO_KEY);

const BUCKET = 'listing-media';
const STORAGE_BASE = `${XOXO_URL}/storage/v1/object/public/${BUCKET}`;
const RATE_LIMIT_MS = 300;

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// SEO-friendly filename from listing metadata
// ---------------------------------------------------------------------------

function slugify(text: string): string {
  return text
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // strip accents
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 50);
}

function buildImageName(
  nickname: string | null,
  city: string | null,
  category: string | null,
  index: number,
  ext: string,
): string {
  // e.g. "escort-girl-lausanne-aysha-1.jpg"
  const parts: string[] = [];
  if (category) parts.push(slugify(category));
  if (city) parts.push(slugify(city));
  if (nickname) parts.push(slugify(nickname));
  parts.push(String(index + 1));
  return parts.join('-') + ext;
}

// ---------------------------------------------------------------------------
// Download image from lolla with proper headers
// ---------------------------------------------------------------------------

async function downloadImage(url: string): Promise<{ buffer: Buffer; contentType: string } | null> {
  // Lolla hotlink-protects SizeD (full res). SizeC (596x1080) is accessible.
  // Try SizeC first, fall back to SizeB if needed.
  const sizesToTry = ['SizeC', 'SizeB'];

  for (const size of sizesToTry) {
    const tryUrl = url.replace(/\/Size[A-Z]\//i, `/${size}/`);
    try {
      const res = await fetch(tryUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
          'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        },
      });

      if (!res.ok) continue;

      const buffer = Buffer.from(await res.arrayBuffer());
      if (buffer.length < 1000) continue; // empty/tiny = hotlink blocked

      const contentType = res.headers.get('content-type') || 'image/jpeg';
      return { buffer, contentType };
    } catch {
      continue;
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Upload to Supabase Storage
// ---------------------------------------------------------------------------

async function uploadToStorage(
  listingId: string,
  fileName: string,
  buffer: Buffer,
  contentType: string,
  isAvatar: boolean,
): Promise<string | null> {
  const folder = isAvatar ? 'avatar' : 'photo';
  const path = `${listingId}/${folder}/${fileName}`;

  const { error } = await xoxo.storage
    .from(BUCKET)
    .upload(path, buffer, {
      contentType,
      upsert: true,
    });

  if (error) {
    console.error(`    Upload error ${path}: ${error.message}`);
    return null;
  }

  return `${STORAGE_BASE}/${path}`;
}

// ---------------------------------------------------------------------------
// Main migration
// ---------------------------------------------------------------------------

async function main() {
  console.log('=== Lolla Image Migration ===');
  console.log('Downloading from lolla.ch → uploading to Supabase Storage\n');

  // Get all media rows pointing to lolla.ch
  const { data: lollaMedia, error: mediaErr } = await xoxo
    .from('media')
    .select('id, listing_id, url, type')
    .like('url', '%lolla.ch%')
    .order('listing_id');

  if (mediaErr) {
    console.error('Failed to query media:', mediaErr);
    process.exit(1);
  }

  console.log(`Found ${lollaMedia.length} lolla media rows to migrate\n`);

  // Group by listing_id
  const byListing = new Map<string, typeof lollaMedia>();
  for (const m of lollaMedia) {
    if (!m.listing_id) continue;
    const arr = byListing.get(m.listing_id) || [];
    arr.push(m);
    byListing.set(m.listing_id, arr);
  }

  // Get listing metadata for SEO filenames
  const listingIds = Array.from(byListing.keys());
  const { data: listings } = await xoxo
    .from('listings_ads')
    .select('id, nickname, category, city_id, canton_id')
    .in('id', listingIds);

  // Get city names
  const cityIds = [...new Set((listings || []).map((l: any) => l.city_id).filter(Boolean))];
  const { data: cities } = await xoxo
    .from('cities')
    .select('id, name')
    .in('id', cityIds);
  const cityMap = new Map((cities || []).map((c: any) => [c.id, c.name]));

  const listingMap = new Map((listings || []).map((l: any) => [l.id, l]));

  let migrated = 0;
  let failed = 0;
  let listingCount = 0;

  for (const [listingId, mediaRows] of byListing) {
    listingCount++;
    const listing = listingMap.get(listingId);
    const cityName = listing?.city_id ? cityMap.get(listing.city_id) : null;
    const nickname = listing?.nickname || null;
    const category = listing?.category || null;

    console.log(`[${listingCount}/${byListing.size}] ${nickname || listingId} (${mediaRows.length} images)`);

    let avatarUrl: string | null = null;

    for (let i = 0; i < mediaRows.length; i++) {
      const m = mediaRows[i];
      const isAvatar = i === 0;

      // Determine file extension from URL
      const urlExt = m.url.match(/\.(jpg|jpeg|png|webp|gif)/i);
      const ext = urlExt ? `.${urlExt[1].toLowerCase()}` : '.jpg';

      const fileName = buildImageName(nickname, cityName, category, i, ext);

      // Download
      const img = await downloadImage(m.url);
      if (!img) {
        console.log(`    [${i + 1}/${mediaRows.length}] FAILED download: ${m.url.slice(-30)}`);
        failed++;
        await sleep(RATE_LIMIT_MS);
        continue;
      }

      // Upload
      const storageUrl = await uploadToStorage(listingId, fileName, img.buffer, img.contentType, isAvatar);
      if (!storageUrl) {
        failed++;
        continue;
      }

      // Update media row
      await xoxo
        .from('media')
        .update({ url: storageUrl })
        .eq('id', m.id);

      if (isAvatar) avatarUrl = storageUrl;
      migrated++;

      await sleep(RATE_LIMIT_MS);
    }

    // Update listing avatar_url
    if (avatarUrl) {
      await xoxo
        .from('listings_ads')
        .update({ avatar_url: avatarUrl })
        .eq('id', listingId);
    }
  }

  console.log(`\n=== Migration Complete ===`);
  console.log(`Migrated: ${migrated}`);
  console.log(`Failed:   ${failed}`);
  console.log(`Listings: ${byListing.size}`);
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
