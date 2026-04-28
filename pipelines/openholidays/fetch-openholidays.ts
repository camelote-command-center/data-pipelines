/**
 * OpenHolidays — fetches public + school holidays for CH across a year window.
 * Default window: current year ± 2 years (so the cron always covers the planning horizon).
 */

import { upsert, verifyAccess, sleep } from '../_shared/re-llm.js';

const SCHEMA = 'bronze_ch';
const TABLE = 'openholidays';
const ON_CONFLICT = 'id';
const BASE = 'https://openholidaysapi.org';

interface ApiHoliday {
  id: string;
  startDate: string;
  endDate: string;
  type: string;
  name: { language: string; text: string }[];
  nationwide: boolean;
  subdivisions?: { code: string; shortName: string }[];
}

function pickName(arr: ApiHoliday['name'], lang: string): string | null {
  return arr.find((n) => n.language.toUpperCase() === lang.toUpperCase())?.text ?? null;
}

async function fetchHolidays(kind: 'PublicHolidays' | 'SchoolHolidays', year: number): Promise<ApiHoliday[]> {
  const url = `${BASE}/${kind}?countryIsoCode=CH&validFrom=${year}-01-01&validTo=${year}-12-31&languageIsoCode=EN`;
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`${kind} ${year}: HTTP ${res.status}`);
  return (await res.json()) as ApiHoliday[];
}

async function fetchHolidaysMultiLang(kind: 'PublicHolidays' | 'SchoolHolidays', year: number): Promise<ApiHoliday[]> {
  // The endpoint returns names in only the requested language. Fetch each
  // language and merge by id so we get FR/DE/EN/IT names per holiday.
  const langs = ['EN', 'FR', 'DE', 'IT'];
  const byId = new Map<string, ApiHoliday>();
  for (const lang of langs) {
    const url = `${BASE}/${kind}?countryIsoCode=CH&validFrom=${year}-01-01&validTo=${year}-12-31&languageIsoCode=${lang}`;
    const res = await fetch(url, { headers: { Accept: 'application/json' } });
    if (!res.ok) {
      console.log(`  ${kind} ${year}/${lang}: HTTP ${res.status} (skipping lang)`);
      continue;
    }
    const arr = (await res.json()) as ApiHoliday[];
    for (const h of arr) {
      const existing = byId.get(h.id);
      if (!existing) {
        byId.set(h.id, h);
      } else {
        // Merge names
        const map = new Map(existing.name.map((n) => [n.language, n.text]));
        for (const n of h.name) map.set(n.language, n.text);
        existing.name = Array.from(map, ([language, text]) => ({ language, text }));
      }
    }
    await sleep(150);
  }
  return Array.from(byId.values());
}

async function main() {
  console.log('='.repeat(64));
  console.log('  OpenHolidays — CH public + school holidays');
  console.log(`  Target: ${SCHEMA}.${TABLE} on re-llm`);
  console.log('='.repeat(64));

  const t0 = Date.now();
  await verifyAccess(SCHEMA, TABLE);

  const thisYear = new Date().getUTCFullYear();
  const years = [thisYear - 2, thisYear - 1, thisYear, thisYear + 1, thisYear + 2];

  let totalUpserted = 0;
  for (const y of years) {
    for (const kind of ['PublicHolidays', 'SchoolHolidays'] as const) {
      const holidays = await fetchHolidaysMultiLang(kind, y);
      console.log(`  ${kind} ${y}: ${holidays.length}`);
      const rows = holidays.map((h) => ({
        id: h.id,
        type: h.type,
        start_date: h.startDate,
        end_date: h.endDate,
        name_en: pickName(h.name, 'EN'),
        name_fr: pickName(h.name, 'FR'),
        name_de: pickName(h.name, 'DE'),
        name_it: pickName(h.name, 'IT'),
        nationwide: h.nationwide,
        subdivisions: h.subdivisions?.map((s) => s.code) ?? null,
        attributes: h,
      }));
      const n = await upsert(SCHEMA, TABLE, rows, ON_CONFLICT, 200);
      totalUpserted += n;
    }
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('\n' + '='.repeat(64));
  console.log('  IMPORT COMPLETE');
  console.log(`  Years covered: ${years[0]}..${years[years.length - 1]}`);
  console.log(`  Rows upserted: ${totalUpserted}`);
  console.log(`  Duration:      ${elapsed}s`);
  console.log('='.repeat(64));
}

main().catch((err) => { console.error('Fatal error:', err); process.exit(1); });
