const BASE_URL = 'https://files.data.gouv.fr/geo-dvf/latest/csv';

const MAX_RETRIES = 3;
const RETRY_DELAYS = [5_000, 15_000, 30_000];

async function fetchWithRetry(url: string): Promise<Response> {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      return res;
    } catch (err: any) {
      console.error(`[Fetch] Attempt ${attempt + 1}/${MAX_RETRIES} failed: ${err.message}`);
      if (attempt < MAX_RETRIES - 1) {
        const delay = RETRY_DELAYS[attempt];
        console.log(`[Fetch] Retrying in ${delay / 1000}s...`);
        await new Promise((r) => setTimeout(r, delay));
      } else {
        throw err;
      }
    }
  }
  throw new Error('Unreachable');
}

/**
 * Discover available years by fetching the index page and parsing links.
 * Falls back to a range of recent years if parsing fails.
 */
async function discoverYears(): Promise<number[]> {
  try {
    const res = await fetchWithRetry(`${BASE_URL}/`);
    const html = await res.text();
    // Look for links like "2024/" in the directory listing
    const matches = html.matchAll(/href="(\d{4})\/"/g);
    const years = [...matches].map((m) => parseInt(m[1], 10)).sort();
    if (years.length > 0) {
      console.log(`[Fetch] Discovered years: ${years.join(', ')}`);
      return years;
    }
  } catch {
    // fall through
  }
  // Fallback: current and previous year
  const currentYear = new Date().getFullYear();
  return [currentYear - 1, currentYear];
}

/**
 * Download the full national CSV for a given year.
 * The URL pattern is: /latest/csv/{year}/full.csv.gz or /latest/csv/{year}/departements/ etc.
 * We'll try the "full" national file first.
 */
export async function fetchYearlyCSV(year?: number): Promise<{ csv: string; year: number }> {
  const years = await discoverYears();
  const targetYear = year ?? years[years.length - 1]; // latest year

  // geo-dvf serves per-department files, so we download the full national one
  const url = `${BASE_URL}/${targetYear}/full.csv.gz`;
  console.log(`[Fetch] Downloading DVF for ${targetYear} from ${url}`);

  try {
    const res = await fetchWithRetry(url);
    const buffer = await res.arrayBuffer();

    // Decompress gzip
    const ds = new DecompressionStream('gzip');
    const decompressed = new Response(
      new Blob([buffer]).stream().pipeThrough(ds),
    );
    const text = await decompressed.text();
    console.log(`[Fetch] Downloaded and decompressed ${(text.length / 1024 / 1024).toFixed(1)} MB`);
    return { csv: text, year: targetYear };
  } catch (err: any) {
    // Fallback: try without .gz
    console.log(`[Fetch] Gzip failed, trying uncompressed CSV...`);
    const fallbackUrl = `${BASE_URL}/${targetYear}/full.csv`;
    const res = await fetchWithRetry(fallbackUrl);
    const text = await res.text();
    console.log(`[Fetch] Downloaded ${(text.length / 1024 / 1024).toFixed(1)} MB`);
    return { csv: text, year: targetYear };
  }
}
