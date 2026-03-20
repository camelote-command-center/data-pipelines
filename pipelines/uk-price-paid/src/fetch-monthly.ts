const MONTHLY_URL =
  'http://prod1.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv';

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

export async function fetchMonthlyCSV(): Promise<string> {
  console.log(`[Fetch] Downloading monthly update from ${MONTHLY_URL}`);
  const res = await fetchWithRetry(MONTHLY_URL);
  const text = await res.text();
  console.log(`[Fetch] Downloaded ${(text.length / 1024 / 1024).toFixed(1)} MB`);
  return text;
}
