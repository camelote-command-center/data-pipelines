/**
 * Lolla.ch Daily Scraper
 *
 * Scrapes classified ads from lolla.ch, upserts into bronze.lolla_ads on xoxo
 * Supabase, and syncs to public.contacts_leads (deduped on phone).
 *
 * Workflow:
 *   1. Fetch sitemap.xml to get all ad URLs
 *   2. Fetch each ad detail page (plain HTML, no auth needed)
 *   3. Parse HTML with Cheerio for structured fields
 *   4. Use Claude to extract services from free-text descriptions
 *   5. Upsert into bronze.lolla_ads on lolla_id conflict
 *   6. Sync unique contacts to public.contacts_leads (dedup on phone)
 *   7. Log to camelote_data.acquisition_logs
 *
 * Modes:
 *   - SEED_MODE=1 — first run, scrape ~80% of ads randomly
 *   - Default     — daily, scrape all new/updated ads
 *
 * Environment variables:
 *   SUPABASE_URL              — xoxo Supabase project URL (required)
 *   SUPABASE_SERVICE_ROLE_KEY — xoxo service_role key     (required)
 *   CMD_SUPABASE_URL          — camelote_data URL         (required)
 *   CMD_SUPABASE_KEY          — camelote_data service key (required)
 *   ANTHROPIC_API_KEY         — Anthropic API key         (required)
 *   SEED_MODE                 — set to "1" for initial 80% seed scrape
 *   DATASET_ID                — dataset UUID in camelote_data (optional)
 *
 * Usage:
 *   npx tsx fetch-lolla.ts
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';
import * as cheerio from 'cheerio';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL = 'https://www.lolla.ch';
const SITEMAP_URL = `${BASE_URL}/sitemap.xml`;
const RATE_LIMIT_MS = 800;       // between page fetches
const BATCH_SIZE = 50;           // upsert batch size
const FETCH_TIMEOUT_MS = 30_000;
const FETCH_RETRIES = 3;
const FETCH_BACKOFF_MS = [3_000, 8_000, 15_000];
const SEED_SAMPLE_RATE = 0.8;    // 80% for seed mode
const DATASET_CODE = 'lolla_daily';

// ---------------------------------------------------------------------------
// Env validation
// ---------------------------------------------------------------------------

const XOXO_URL = process.env.SUPABASE_URL;
const XOXO_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CMD_URL = process.env.CMD_SUPABASE_URL;
const CMD_KEY = process.env.CMD_SUPABASE_KEY;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY || null;
const SEED_MODE = process.env.SEED_MODE === '1';
const DATASET_ID = process.env.DATASET_ID || null;

if (!XOXO_URL || !XOXO_KEY) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required');
  process.exit(1);
}
if (!CMD_URL || !CMD_KEY) {
  console.error('ERROR: CMD_SUPABASE_URL and CMD_SUPABASE_KEY are required');
  process.exit(1);
}
if (!ANTHROPIC_KEY) {
  console.warn('WARNING: ANTHROPIC_API_KEY not set — service extraction will be skipped');
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

const xoxo: SupabaseClient = createClient(XOXO_URL, XOXO_KEY);
const cmd: SupabaseClient = createClient(CMD_URL, CMD_KEY);
const anthropic = ANTHROPIC_KEY ? new Anthropic({ apiKey: ANTHROPIC_KEY }) : null;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url: string, retries = FETCH_RETRIES): Promise<string> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

      const res = await fetch(url, {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'fr-CH,fr;q=0.9,en;q=0.8',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
      return await res.text();
    } catch (err) {
      if (attempt < retries) {
        const delay = FETCH_BACKOFF_MS[attempt] ?? 15_000;
        console.log(`  Fetch failed (attempt ${attempt + 1}/${retries + 1}): ${err}`);
        await sleep(delay);
      } else {
        throw err;
      }
    }
  }
  throw new Error(`fetchWithRetry failed after ${retries + 1} attempts: ${url}`);
}

// ---------------------------------------------------------------------------
// Lolla → xoxo category mapping
// ---------------------------------------------------------------------------

const CATEGORY_MAP: Record<string, string> = {
  'escort-girls': 'girls',
  'escort-trans': 'trans',
  'massage-sensuel': 'massage',
  'massage': 'massage',
  'webcams': 'tv',
  'sm-bdsm': 'sm',
  'sex-phone': 'girls',       // closest match
  'location-d': 'salon',      // apartment rental → salon
};

function mapCategory(lollaCategory: string): string {
  return CATEGORY_MAP[lollaCategory] || 'girls';
}

// ---------------------------------------------------------------------------
// Step 1: Fetch sitemap and extract ad URLs
// ---------------------------------------------------------------------------

interface AdUrl {
  url: string;
  lollaId: number;
  slug: string;
  category: string;
}

async function fetchAdUrls(): Promise<AdUrl[]> {
  console.log('  Fetching sitemap...');
  const xml = await fetchWithRetry(SITEMAP_URL);
  const $ = cheerio.load(xml, { xmlMode: true });

  const adUrls: AdUrl[] = [];

  $('url > loc').each((_, el) => {
    const loc = $(el).text().trim();
    // Ad detail URLs end with numeric ID: /ad/...-{numericID}
    const idMatch = loc.match(/\/ad\/(.+)-(\d+)$/);
    if (!idMatch) return;

    const slug = loc.replace(`${BASE_URL}/`, '');
    const lollaId = parseInt(idMatch[2], 10);
    const slugBody = idMatch[1]; // everything between /ad/ and -ID

    // Detect category from slug prefix
    let category = 'unknown';
    const KNOWN_CATS = ['escort-girls', 'escort-trans', 'massage-sensuel', 'sm-bdsm', 'sex-phone', 'location-d'];
    for (const cat of KNOWN_CATS) {
      if (slugBody.startsWith(cat)) { category = cat; break; }
    }
    if (category === 'unknown' && slugBody.startsWith('webcams')) category = 'webcams';

    adUrls.push({ url: loc, lollaId, slug, category });
  });

  console.log(`  Sitemap: ${adUrls.length} ad URLs found`);
  return adUrls;
}

// ---------------------------------------------------------------------------
// Step 2: Parse an individual ad page
// ---------------------------------------------------------------------------

interface ScrapedAd {
  lolla_id: number;
  slug: string;
  category: string;
  nickname: string | null;
  phone: string | null;
  whatsapp: string | null;
  canton: string | null;
  city: string | null;
  postal_code: string | null;
  age: string | null;
  ethnicity: string | null;
  height: string | null;
  languages: string[];
  description: string | null;
  services: string[];
  services_raw: string | null;
  incall: boolean;
  outcall: boolean;
  photo_urls: string[];
  video_urls: string[];
  is_certified: boolean;
  availability: string | null;
  onlyfans_url: string | null;
  source_url: string;
}

function normalizePhone(raw: string): string | null {
  if (!raw) return null;
  // Strip everything except digits and leading +
  let cleaned = raw.replace(/[^\d+]/g, '');
  // Convert 0041... to +41...
  if (cleaned.startsWith('0041')) cleaned = '+41' + cleaned.slice(4);
  // Convert 041... to +41...
  else if (cleaned.startsWith('041') && cleaned.length > 10) cleaned = '+41' + cleaned.slice(3);
  // Convert 07x... to +417x...
  else if (cleaned.startsWith('0') && cleaned.length === 10) cleaned = '+41' + cleaned.slice(1);
  // Already has +
  else if (!cleaned.startsWith('+') && cleaned.length >= 10) cleaned = '+' + cleaned;
  return cleaned || null;
}

function parseAdPage(html: string, adUrl: AdUrl): ScrapedAd | null {
  const $ = cheerio.load(html);

  // ---------- Nickname ----------
  const nickname = $('.AdOnePseudo').first().text().trim() || null;

  // ---------- Phone ----------
  let phone: string | null = null;
  // Primary: tel: link on the classic phone button
  const phoneHref = $('[class*="ItemViewPhone_ButtonClassic"]').attr('href');
  if (phoneHref) {
    phone = normalizePhone(phoneHref.replace('tel:', ''));
  }
  // Fallback: any tel: link
  if (!phone) {
    $('a[href^="tel:"]').each((_, el) => {
      if (phone) return;
      const href = $(el).attr('href') || '';
      phone = normalizePhone(href.replace('tel:', ''));
    });
  }

  // ---------- WhatsApp ----------
  let whatsapp: string | null = null;
  const waOnclick = $('[class*="ItemViewPhone_ButtonWhatsApp"]').attr('onclick');
  if (waOnclick) {
    const waMatch = waOnclick.match(/phone=([^&"']+)/);
    if (waMatch) whatsapp = normalizePhone(waMatch[1]);
  }

  // ---------- Canton ----------
  let canton: string | null = null;
  const cantonText = $('.AdOneCityCantonText').first().text().trim(); // "Vaud (VD)"
  if (cantonText) {
    canton = cantonText; // keep full text, e.g. "Vaud (VD)"
  }

  // ---------- City ----------
  const city = $('.AdOneCityName').first().text().trim() || null;

  // ---------- Postal code ----------
  let postalCode: string | null = null;
  const postalText = $('.AdOneCityCode').first().text().trim(); // "- 1006"
  if (postalText) {
    const m = postalText.match(/(\d{4})/);
    if (m) postalCode = m[1];
  }

  // ---------- Age ----------
  const age = $('.AdOneAge').first().text().trim() || null;

  // ---------- Ethnicity ----------
  const ethnicity = $('.AdOneEthnic').first().text().trim() || null;

  // ---------- Height ----------
  let height: string | null = null;
  const fullText = $('body').text();
  const heightMatch = fullText.match(/(\d\.\d{2}\s*m)/);
  if (heightMatch) height = heightMatch[1];

  // ---------- Languages ----------
  const languages: string[] = [];
  $('.ItemView_SpokenLanguage').each((_, el) => {
    const lang = $(el).text().trim();
    if (lang && !languages.includes(lang)) languages.push(lang);
  });

  // ---------- Description ----------
  const description = $('.AdOneDescription').text().trim() || null;

  // ---------- Services raw (embedded in description) ----------
  let servicesRaw: string | null = null;
  // Services are in the free-text description; pass description to Claude later
  if (description) servicesRaw = description;

  // ---------- Incall / Outcall ----------
  const descHtml = $('.AdOneDescription').html() || '';
  const incallLabels: string[] = [];
  $('.AdOneDescriptionCall').each((_, el) => {
    incallLabels.push($(el).text().trim().toLowerCase());
  });
  const incall = incallLabels.some((l) => l.includes('incall')) ||
                 /r[ée]ception|incall|re[çc]ois/i.test(descHtml);
  const outcall = incallLabels.some((l) => l.includes('outcall')) ||
                  /d[ée]placement|outcall|se d[ée]place/i.test(descHtml);

  // ---------- Photos ----------
  const photoUrls: string[] = [];
  // Main image
  const mainImg = $('img[class*="ItemViewImg0"]').attr('src');
  if (mainImg) {
    const full = mainImg.startsWith('http') ? mainImg : BASE_URL + mainImg;
    photoUrls.push(full.replace(/\/Size[A-Z]\//, '/SizeD/'));
  }
  // Gallery images
  $('img[class*="ItemViewImg"]').each((_, el) => {
    let src = $(el).attr('src') || '';
    if (!src || src.includes('Icon') || src.includes('Images')) return;
    if (!src.startsWith('http')) src = BASE_URL + src;
    src = src.replace(/\/Size[A-Z]\//, '/SizeD/');
    if (!photoUrls.includes(src)) photoUrls.push(src);
  });

  // ---------- Videos ----------
  const videoUrls: string[] = [];
  $('video source, a[href*="/Video/"]').each((_, el) => {
    let src = $(el).attr('src') || $(el).attr('href') || '';
    if (!src.startsWith('http')) src = BASE_URL + src;
    if (!videoUrls.includes(src)) videoUrls.push(src);
  });

  // ---------- Certified ----------
  const isCertified = $('.CertifiedImg').length > 0;

  // ---------- Availability ----------
  let availability: string | null = null;
  const availMatch = fullText.match(/(\d{1,2}h\s*-\s*\d{1,2}h)/);
  if (availMatch) availability = availMatch[1];

  // ---------- OnlyFans ----------
  let onlyfansUrl: string | null = null;
  $('a[href*="onlyfans.com"]').each((_, el) => {
    onlyfansUrl = $(el).attr('href') || null;
  });

  // ---------- Category from page (override URL-based) ----------
  const categoryTitle = $('[class*="AdOneCategoryImg"]').attr('title');
  let category = adUrl.category;
  if (categoryTitle) {
    const ct = categoryTitle.toLowerCase();
    if (ct.includes('escort girl')) category = 'escort-girls';
    else if (ct.includes('trans')) category = 'escort-trans';
    else if (ct.includes('massage')) category = 'massage-sensuel';
    else if (ct.includes('webcam')) category = 'webcams';
    else if (ct.includes('sm') || ct.includes('bdsm')) category = 'sm-bdsm';
  }

  // Skip if no useful data at all
  if (!nickname && !phone) return null;

  return {
    lolla_id: adUrl.lollaId,
    slug: adUrl.slug,
    category: adUrl.category,
    nickname,
    phone,
    whatsapp,
    canton,
    city,
    postal_code: postalCode,
    age,
    ethnicity,
    height,
    languages,
    description,
    services: [],          // populated later by Claude
    services_raw: servicesRaw,
    incall,
    outcall,
    photo_urls: photoUrls,
    video_urls: videoUrls,
    is_certified: isCertified,
    availability,
    onlyfans_url: onlyfansUrl,
    source_url: adUrl.url,
  };
}

// ---------------------------------------------------------------------------
// Step 3: Claude service extraction (batch for efficiency)
// ---------------------------------------------------------------------------

const SERVICE_PROMPT = `You are parsing classified ads from a Swiss escort/adult services website.
Given the ad description and raw services text below, extract a clean list of services offered.

Return ONLY a JSON array of service names in French, e.g.:
["Massage érotique", "GFE", "Duo", "Domination"]

If no services can be determined, return an empty array: []

Rules:
- Normalize names: consistent capitalization, no duplicates
- Include common abbreviations as-is (GFE, OWO, CIF, etc.)
- Keep it concise — one service per array element
- Return ONLY the JSON array, nothing else`;

async function extractServicesWithClaude(
  description: string | null,
  servicesRaw: string | null,
): Promise<string[]> {
  if (!description && !servicesRaw) return [];
  if (!anthropic) return []; // Claude not available

  const text = [
    description ? `Description:\n${description.slice(0, 2000)}` : '',
    servicesRaw ? `\nServices section:\n${servicesRaw.slice(0, 1000)}` : '',
  ].filter(Boolean).join('\n');

  try {
    const response = await anthropic!.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 512,
      messages: [
        { role: 'user', content: `${SERVICE_PROMPT}\n\n${text}` },
      ],
    });

    const resultText = response.content[0].type === 'text' ? response.content[0].text : '[]';
    const cleaned = resultText.replace(/^```json?\n?/m, '').replace(/\n?```$/m, '').trim();
    const parsed = JSON.parse(cleaned);
    return Array.isArray(parsed) ? parsed : [];
  } catch (err) {
    console.error(`  Claude service extraction error: ${err}`);
    return [];
  }
}

// ---------------------------------------------------------------------------
// Step 4: Upsert into bronze.lolla_ads
// ---------------------------------------------------------------------------

async function upsertBronzeAds(records: ScrapedAd[]): Promise<number> {
  if (records.length === 0) return 0;
  let totalUpserted = 0;
  const totalBatches = Math.ceil(records.length / BATCH_SIZE);

  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;

    const { error, count } = await xoxo
      .schema('bronze')
      .from('lolla_ads')
      .upsert(
        batch.map((r) => ({
          ...r,
          scraped_at: new Date().toISOString(),
        })),
        { onConflict: 'lolla_id', count: 'exact' },
      );

    if (error) {
      console.error(`  Bronze batch ${batchNum}/${totalBatches} error: ${error.message}`);
      continue;
    }

    const upserted = count ?? batch.length;
    totalUpserted += upserted;
    console.log(`  Bronze batch ${batchNum}/${totalBatches}: ${upserted} rows`);
  }

  return totalUpserted;
}

// ---------------------------------------------------------------------------
// Step 5: Sync to contacts_leads (dedup on phone)
// ---------------------------------------------------------------------------

async function syncToContactsLeads(records: ScrapedAd[]): Promise<{ new: number; updated: number }> {
  // Dedup by phone — keep latest per phone number
  const byPhone = new Map<string, ScrapedAd>();
  for (const r of records) {
    if (!r.phone) continue;
    const existing = byPhone.get(r.phone);
    if (!existing || r.lolla_id > existing.lolla_id) {
      byPhone.set(r.phone, r);
    }
  }

  const leads = Array.from(byPhone.values());
  console.log(`  Contacts: ${leads.length} unique phone numbers from ${records.length} ads`);

  let newCount = 0;
  let updatedCount = 0;

  for (let i = 0; i < leads.length; i += BATCH_SIZE) {
    const batch = leads.slice(i, i + BATCH_SIZE);

    for (const ad of batch) {
      // Check if already exists
      const { data: existing } = await xoxo
        .from('contacts_leads')
        .select('id, phone_number')
        .eq('phone_number', ad.phone!)
        .maybeSingle();

      const leadData = {
        nickname: ad.nickname,
        phone_number: ad.phone,
        whatsapp_number: ad.whatsapp || ad.phone,
        canton: ad.canton,
        city: ad.city,
        category: mapCategory(ad.category),
        status: 'new',
        description: ad.description?.slice(0, 2000) || null,
        source_url: ad.source_url,
      };

      if (existing) {
        // Update location + photos (the provider may have moved)
        const { error } = await xoxo
          .from('contacts_leads')
          .update({
            canton: leadData.canton,
            city: leadData.city,
            description: leadData.description,
            source_url: leadData.source_url,
            nickname: leadData.nickname,
          })
          .eq('id', existing.id);

        if (!error) updatedCount++;
      } else {
        const { error } = await xoxo
          .from('contacts_leads')
          .insert(leadData);

        if (error) {
          // May fail on unique constraint — that's fine
          if (!error.message.includes('duplicate')) {
            console.error(`  Contact insert error: ${error.message}`);
          }
        } else {
          newCount++;
        }
      }
    }
  }

  return { new: newCount, updated: updatedCount };
}

// ---------------------------------------------------------------------------
// Step 6: Acquisition logging (camelote_data)
// ---------------------------------------------------------------------------

async function logStart(): Promise<string | null> {
  if (!DATASET_ID) return null;

  const { data, error } = await cmd
    .from('acquisition_logs')
    .insert({
      dataset_id: DATASET_ID,
      started_at: new Date().toISOString(),
      status: 'running',
      triggered_by: process.env.GITHUB_ACTIONS ? 'github_action' : 'manual',
      notes: `[${DATASET_CODE}] ${SEED_MODE ? 'Seed run (80%)' : 'Daily incremental'}`,
    })
    .select('id')
    .single();

  if (error) {
    console.error(`  acquisition_logs insert error: ${error.message}`);
    return null;
  }
  return data?.id || null;
}

async function logEnd(
  logId: string | null,
  stats: {
    fetched: number;
    new: number;
    updated: number;
    errors: number;
    durationMs: number;
  },
  errorMsg?: string,
): Promise<void> {
  if (logId) {
    await cmd
      .from('acquisition_logs')
      .update({
        completed_at: new Date().toISOString(),
        duration_seconds: Math.round(stats.durationMs / 1000),
        status: errorMsg ? 'error' : 'success',
        records_fetched: stats.fetched,
        records_new: stats.new,
        records_updated: stats.updated,
        error_message: errorMsg || null,
      })
      .eq('id', logId);
  }

  // Also update datasets row
  if (DATASET_ID) {
    await cmd
      .from('datasets')
      .update({
        last_acquired_at: new Date().toISOString(),
        record_count: stats.fetched,
        status: errorMsg ? 'error' : 'active',
      })
      .eq('id', DATASET_ID);
  }
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  Lolla.ch Daily Scraper');
  console.log(`  Mode: ${SEED_MODE ? 'SEED (80% sample)' : 'DAILY (incremental)'}`);
  console.log('  Source: lolla.ch');
  console.log('  Target: bronze.lolla_ads + contacts_leads');
  console.log('='.repeat(60));

  const startTime = Date.now();
  const logId = await logStart();

  try {
    // 1. Get all ad URLs from sitemap
    let adUrls = await fetchAdUrls();

    if (adUrls.length === 0) {
      console.log('  No ads found in sitemap. Exiting.');
      await logEnd(logId, { fetched: 0, new: 0, updated: 0, errors: 0, durationMs: Date.now() - startTime });
      return;
    }

    // 2. In seed mode, randomly sample 80%
    if (SEED_MODE) {
      adUrls = adUrls.filter(() => Math.random() < SEED_SAMPLE_RATE);
      console.log(`  Seed mode: sampling ${adUrls.length} ads (~80%)`);
    } else {
      // Daily mode: check which ads we already have
      const { data: existingIds } = await xoxo
        .schema('bronze')
        .from('lolla_ads')
        .select('lolla_id');

      const existingSet = new Set((existingIds || []).map((r: any) => r.lolla_id));
      const newAds = adUrls.filter((a) => !existingSet.has(a.lollaId));

      // Also re-scrape a random 10% of existing ads to catch updates (location changes)
      const existingAds = adUrls.filter((a) => existingSet.has(a.lollaId));
      const rescrapeAds = existingAds.filter(() => Math.random() < 0.10);

      adUrls = [...newAds, ...rescrapeAds];
      console.log(`  Daily mode: ${newAds.length} new + ${rescrapeAds.length} re-scrape = ${adUrls.length} total`);
    }

    // 3. Fetch and parse each ad
    const scraped: ScrapedAd[] = [];
    let fetchErrors = 0;

    for (let i = 0; i < adUrls.length; i++) {
      const adUrl = adUrls[i];
      if (i % 50 === 0) {
        console.log(`  Fetching ${i + 1}/${adUrls.length}...`);
      }

      try {
        const html = await fetchWithRetry(adUrl.url);
        const ad = parseAdPage(html, adUrl);
        if (ad) scraped.push(ad);
      } catch (err) {
        fetchErrors++;
        if (fetchErrors % 10 === 0) {
          console.error(`  Fetch errors so far: ${fetchErrors}`);
        }
      }

      await sleep(RATE_LIMIT_MS);
    }

    console.log(`\n  Scraped: ${scraped.length} ads, fetch errors: ${fetchErrors}`);

    if (scraped.length === 0) {
      console.log('  No ads scraped. Exiting.');
      await logEnd(logId, { fetched: 0, new: 0, updated: 0, errors: fetchErrors, durationMs: Date.now() - startTime });
      return;
    }

    // 4. Extract services with Claude (batch — only for ads that have description/services)
    console.log('\n  Extracting services with Claude...');
    let claudeCalls = 0;
    for (const ad of scraped) {
      if (ad.description || ad.services_raw) {
        ad.services = await extractServicesWithClaude(ad.description, ad.services_raw);
        claudeCalls++;
        if (claudeCalls % 50 === 0) {
          console.log(`  Claude: ${claudeCalls}/${scraped.length} processed`);
        }
        await sleep(200); // Rate limit for Anthropic
      }
    }
    console.log(`  Claude: ${claudeCalls} service extractions done`);

    // 5. Upsert into bronze.lolla_ads
    console.log('\n  Upserting into bronze.lolla_ads...');
    const bronzeCount = await upsertBronzeAds(scraped);

    // 6. Sync to contacts_leads (dedup on phone)
    console.log('\n  Syncing to contacts_leads...');
    const contactStats = await syncToContactsLeads(scraped);

    // 7. Log results
    const elapsed = Date.now() - startTime;
    await logEnd(logId, {
      fetched: scraped.length,
      new: contactStats.new,
      updated: contactStats.updated,
      errors: fetchErrors,
      durationMs: elapsed,
    });

    const elapsedSec = (elapsed / 1000).toFixed(1);
    console.log(`\n${'='.repeat(60)}`);
    console.log('  SCRAPE COMPLETE');
    console.log(`  Ads scraped:        ${scraped.length}`);
    console.log(`  Bronze upserted:    ${bronzeCount}`);
    console.log(`  Contacts new:       ${contactStats.new}`);
    console.log(`  Contacts updated:   ${contactStats.updated}`);
    console.log(`  Fetch errors:       ${fetchErrors}`);
    console.log(`  Duration:           ${elapsedSec}s`);
    console.log('='.repeat(60));

  } catch (err) {
    const elapsed = Date.now() - startTime;
    console.error('Fatal error:', err);
    await logEnd(logId, { fetched: 0, new: 0, updated: 0, errors: 1, durationMs: elapsed }, String(err));
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
