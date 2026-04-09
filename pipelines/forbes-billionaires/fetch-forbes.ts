/**
 * Forbes Billionaires Parser
 *
 * Fetches the Forbes billionaires list via their public API,
 * then visits each profile page to extract JSON-LD net worth data.
 * Upserts everything into the `forbes_person` table on BillionairesList Supabase.
 *
 * Original: BillionairesList/parsers (Node.js + Knex)
 * Reproduced: camelote-command-center/data-pipelines (TypeScript + Supabase client)
 *
 * Env vars:
 *   SUPABASE_URL              — BillionairesList Supabase URL
 *   SUPABASE_SERVICE_ROLE_KEY — BillionairesList service role key
 *
 * Usage:
 *   npm run fetch
 */

import { createClient } from '@supabase/supabase-js';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const FORBES_API_BASE =
  'https://www.forbes.com/forbesapi/person/billionaires/2025/rank/true.json';

const FORBES_FIELDS = [
  'uri', 'finalWorth', 'age', 'countryOfCitizenship', 'source',
  'qas', 'rank', 'status', 'category', 'person', 'personName',
  'industries', 'organization', 'gender', 'firstName', 'lastName',
  'squareImage', 'bios',
].join(',');

const PAGE_SIZE = 50;
const CONCURRENCY = 5; // parallel profile fetches
const PROFILE_DELAY_MS = 200; // polite delay between profile fetches

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
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Sanitize data: escape backslashes and remove null bytes */
function sanitizeData(data: any): any {
  if (typeof data === 'string') {
    return data.replace(/\\/g, '\\\\').replace(/\u0000/g, '');
  }
  if (Array.isArray(data)) {
    return data.map(sanitizeData);
  }
  if (typeof data === 'object' && data !== null) {
    const result: Record<string, any> = {};
    for (const key of Object.keys(data)) {
      result[key] = sanitizeData(data[key]);
    }
    return result;
  }
  return data;
}

/** Extract JSON-LD block from Forbes profile HTML */
function extractJsonLd(html: string): any | null {
  const regex =
    /<\s*script\s+type="application\/ld\+json"\s*>(.*?)<\/\s*script\s*>/is;
  const match = html.match(regex);
  if (match && match[1]) {
    try {
      return JSON.parse(match[1].trim());
    } catch (e) {
      return null;
    }
  }
  return null;
}

/** Fetch a Forbes profile page and extract net worth from JSON-LD */
async function fetchProfileNetWorth(uri: string): Promise<number | null> {
  try {
    const resp = await fetch(
      `https://www.forbes.com/profile/${uri}/?list=billionaires`,
      {
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; CameloteParser/1.0)',
        },
      },
    );
    if (!resp.ok) return null;

    const html = await resp.text();
    const jsonLd = extractJsonLd(html);
    if (jsonLd?.netWorth?.value) {
      return parseInt(jsonLd.netWorth.value, 10) || null;
    }
    return null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function run() {
  console.log('=== Forbes Billionaires Parser ===');
  console.log(`Target: ${supabaseUrl}`);
  console.log();

  // 1. Get total count
  const countUrl = `${FORBES_API_BASE}?fields=uri&limit=1&start=0`;
  const countResp = await fetch(countUrl);
  if (!countResp.ok) {
    console.error(`Failed to fetch count: ${countResp.status}`);
    process.exit(1);
  }
  const countData = await countResp.json();
  const totalCount: number = countData.personList.count;
  console.log(`Total billionaires: ${totalCount}`);

  let totalProcessed = 0;
  let totalUpserted = 0;

  // 2. Paginate through the list
  for (let offset = 0; offset < totalCount; offset += PAGE_SIZE) {
    const url = `${FORBES_API_BASE}?fields=${FORBES_FIELDS}&limit=${PAGE_SIZE}&start=${offset}`;
    console.log(
      `\nFetching ${offset}–${Math.min(offset + PAGE_SIZE, totalCount)} of ${totalCount}...`,
    );

    const listResp = await fetch(url);
    if (!listResp.ok) {
      console.error(`  API error: ${listResp.status}`);
      continue;
    }

    const listData = await listResp.json();
    const persons: any[] = listData.personList.personsLists || [];

    // 3. Fetch profile pages in parallel batches for JSON-LD net worth
    const enrichedPersons: any[] = [];

    for (let i = 0; i < persons.length; i += CONCURRENCY) {
      const batch = persons.slice(i, i + CONCURRENCY);
      const results = await Promise.all(
        batch.map(async (person: any) => {
          const sanitized = sanitizeData(person);
          const jsonLdWorth = await fetchProfileNetWorth(sanitized.uri);

          return {
            ...sanitized,
            person: JSON.stringify(sanitized.person),
            qas: JSON.stringify(sanitized.qas),
            industries: JSON.stringify(sanitized.industries),
            bios: JSON.stringify(sanitized.bios),
            // Use JSON-LD net worth if available, otherwise API value * 1M
            finalWorth: jsonLdWorth || (sanitized.finalWorth ?? 0) * 1_000_000,
            last_parsed_at: new Date().toISOString(),
          };
        }),
      );
      enrichedPersons.push(...results);
      await sleep(PROFILE_DELAY_MS);
    }

    // 4. Upsert batch to Supabase
    const { error, count } = await supabase
      .from('forbes_person')
      .upsert(enrichedPersons, { onConflict: 'uri', count: 'exact' });

    if (error) {
      console.error(`  Upsert error: ${error.message}`);
      // Try individual inserts on batch failure
      for (const person of enrichedPersons) {
        const { error: singleErr } = await supabase
          .from('forbes_person')
          .upsert(person, { onConflict: 'uri' });
        if (singleErr) {
          console.error(`    Failed ${person.uri}: ${singleErr.message}`);
        } else {
          totalUpserted++;
        }
      }
    } else {
      const upserted = count ?? enrichedPersons.length;
      totalUpserted += upserted;
      console.log(`  Upserted ${upserted} persons`);
    }

    totalProcessed += persons.length;
  }

  console.log(`\n=== Done ===`);
  console.log(`Processed: ${totalProcessed}`);
  console.log(`Upserted:  ${totalUpserted}`);
  process.exit(0);
}

run().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
