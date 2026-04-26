const BASE_URL = 'https://files.data.gouv.fr/geo-dvf/latest/csv';

const MAX_RETRIES = 3;
const RETRY_DELAYS = [5_000, 15_000, 30_000];

class HttpError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

async function fetchWithRetry(url: string): Promise<Response> {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new HttpError(res.status, `HTTP ${res.status}: ${res.statusText}`);
      return res;
    } catch (err: any) {
      console.error(`[Fetch] Attempt ${attempt + 1}/${MAX_RETRIES} failed: ${err.message}`);
      // Don't retry 404s — file genuinely missing; let caller try another year.
      if (err instanceof HttpError && err.status === 404) throw err;
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
async function downloadYear(targetYear: number): Promise<string> {
  const url = `${BASE_URL}/${targetYear}/full.csv.gz`;
  console.log(`[Fetch] Downloading DVF for ${targetYear} from ${url}`);
  const res = await fetchWithRetry(url);
  const buffer = await res.arrayBuffer();
  const ds = new DecompressionStream('gzip');
  const decompressed = new Response(new Blob([buffer]).stream().pipeThrough(ds));
  const text = await decompressed.text();
  console.log(`[Fetch] Downloaded and decompressed ${(text.length / 1024 / 1024).toFixed(1)} MB`);
  return text;
}

export async function fetchYearlyCSV(year?: number): Promise<{ csv: string; year: number }> {
  if (year) {
    return { csv: await downloadYear(year), year };
  }

  // Auto mode: walk discovered years from newest to oldest, falling back on
  // 404s. The published directory listing sometimes contains a year whose
  // full.csv.gz hasn't been generated yet (race between directory and file).
  const years = (await discoverYears()).slice().sort((a, b) => b - a); // newest first
  const tried: number[] = [];
  for (const candidate of years) {
    try {
      const csv = await downloadYear(candidate);
      return { csv, year: candidate };
    } catch (err: any) {
      tried.push(candidate);
      if (err instanceof HttpError && err.status === 404) {
        console.log(`[Fetch] ${candidate} not published yet (404) — trying previous year`);
        continue;
      }
      throw err;
    }
  }
  throw new Error(`No DVF year file available. Tried: ${tried.join(', ')}`);
}
