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
 * Open a streaming, gunzipped read of the year's full CSV.
 * Returns a Web ReadableStream of decompressed UTF-8 bytes — never materializes
 * the full file in memory (the 2025 file exceeds V8's 512MB string cap).
 */
async function openYearStream(targetYear: number): Promise<ReadableStream<Uint8Array>> {
  const url = `${BASE_URL}/${targetYear}/full.csv.gz`;
  console.log(`[Fetch] Streaming DVF for ${targetYear} from ${url}`);
  const res = await fetchWithRetry(url);
  if (!res.body) throw new Error('Response has no body');
  return res.body.pipeThrough(new DecompressionStream('gzip'));
}

export async function fetchYearlyCSV(
  year?: number,
): Promise<{ stream: ReadableStream<Uint8Array>; year: number }> {
  if (year) {
    return { stream: await openYearStream(year), year };
  }

  // Auto mode: walk discovered years from newest to oldest, falling back on
  // 404s. The published directory listing sometimes contains a year whose
  // full.csv.gz hasn't been generated yet (race between directory and file).
  const years = (await discoverYears()).slice().sort((a, b) => b - a); // newest first
  const tried: number[] = [];
  for (const candidate of years) {
    try {
      const stream = await openYearStream(candidate);
      return { stream, year: candidate };
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
