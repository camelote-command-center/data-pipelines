/**
 * tinjob-personio
 *
 * Phase 1 ATS parser for tinjob.ch.
 *
 * For every row in public.ats_tenants WHERE ats_type='personio' AND status <> 'dead',
 * fetch `https://<tenant>.jobs.personio.de/xml`, parse positions, and upsert every
 * position whose <office> looks Swiss into public.job_listings.
 *
 * Dedup: (source, external_id) where source='ats:personio:<tenant>' and external_id=position id.
 *
 * Tenant health:
 *  - 200 + parseable XML  → status='active', consecutive_fails=0, has_swiss_jobs, country, jobs_seen_last.
 *  - 301 (personio bounce) / 404 / network err → consecutive_fails++. After 5 fails → status='dead'.
 *
 * Concurrency: 20 tenants in flight. Polite 50 ms jitter. ~5-10 min for 5.8k tenants.
 *
 * Env:
 *   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY  — tinjob destination
 *   MAX_TENANTS (optional)                    — cap for smoke tests
 */

import { createClient } from "@supabase/supabase-js";
import { XMLParser } from "fast-xml-parser";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required");
  process.exit(1);
}
const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

const MAX_TENANTS = process.env.MAX_TENANTS ? parseInt(process.env.MAX_TENANTS, 10) : 0;
const CONCURRENCY = 20;
const FEED_TIMEOUT_MS = 10_000;
const DEAD_AFTER_FAILS = 5;

// ---------------------------------------------------------------------------
// Switzerland detector (strict)
// ---------------------------------------------------------------------------
// Personio is a German company so office strings are heavily polluted with DE
// city names that collide with CH (Bern, Freiburg, etc.). We require either an
// explicit country marker, a canton code, or an unambiguous CH-only city name.
// Plain "Freiburg" and "Bern" are excluded — use "Fribourg" / "Berne" only.

const COUNTRY_MARKERS = [
  "switzerland", "schweiz", "suisse", "svizzera", "helvetia",
  "schweizer", "suiza", "swiss",
];

// ISO 3166-2:CH canton abbreviations
const CANTON_CODES = [
  "ZH","BE","LU","UR","SZ","OW","NW","GL","ZG","FR","SO","BS","BL",
  "SH","AR","AI","SG","GR","AG","TG","TI","VD","VS","NE","GE","JU",
];

// Swiss-only (or overwhelmingly Swiss) city names. Excludes Bern, Freiburg.
const UNAMBIGUOUS_CH_CITIES = [
  "zurich", "zürich", "zuerich",
  "geneva", "genève", "geneve", "genf",
  "lausanne",
  "basel", "bâle", "basilea",
  "lugano", "locarno", "bellinzona", "mendrisio", "chiasso",
  "chur", "coire",
  "sion", "sitten", "sierre", "martigny", "monthey",
  "nyon", "morges", "vevey", "montreux", "yverdon", "renens",
  "thun", "thoune", "winterthur", "winterthour",
  "biel/bienne", "bienne",
  "luzern", "lucerne",
  "neuchâtel", "neuchatel", "neuenburg",
  "st. gallen", "st.gallen", "sankt gallen", "saint-gall",
  "schaffhausen", "schaffhouse",
  "fribourg", // French form — disambiguates from DE Freiburg
  "gstaad", "davos", "zermatt", "interlaken", "crans-montana", "villars",
  "aarau", "olten", "solothurn", "soleure",
  "wil", "uster", "rapperswil", "kreuzlingen", "frauenfeld",
  "baar", "zug", "zoug",
  "brugg", "dietikon", "wetzikon", "bülach", "buelach",
  "kloten", "opfikon", "horgen", "wädenswil", "waedenswil",
  "lancy", "meyrin", "vernier", "carouge", "onex", "plan-les-ouates",
  "glarus", "delémont", "delsberg", "liestal", "pratteln", "allschwil",
  "herisau", "appenzell", "altdorf", "stans", "sarnen",
  "romanshorn", "arbon", "rheinfelden",
  "köniz", "koeniz",
];

const COUNTRY_RE = new RegExp("\\b(" + COUNTRY_MARKERS.join("|") + ")\\b", "i");
const CANTON_RE = new RegExp(
  // canton codes appear as "City, ZH" / "8001 Zürich ZH" / "(ZH)" etc.
  "(?:[,\\s\\-(/])(" + CANTON_CODES.join("|") + ")(?=[\\s,)\\/.\\-]|$)",
);
const CH_CITY_RE = new RegExp(
  "\\b(" + UNAMBIGUOUS_CH_CITIES
    .map((w) => w.replace(/\s+/g, "\\s+").replace(/\./g, "\\.?"))
    .join("|") + ")\\b",
  "i",
);

// Swiss postal codes: 4 digits, range 1000-9658. Requires a letter neighbour so we
// don't match "10000 employees". "8001 Zürich" or "CH-8001" both qualify.
const CH_POSTAL_RE = /(?:\bCH[-\s]?|\b)([1-9]\d{3})(?=[\s-][A-Za-zÀ-ÿ])/;

function isSwissOffice(office?: string | null): boolean {
  if (!office) return false;
  const s = office.trim();
  if (!s) return false;

  // Explicit country marker: always wins.
  if (COUNTRY_RE.test(s)) return true;

  // Canton code (strong signal).
  if (CANTON_RE.test(s)) return true;

  // Unambiguous Swiss-only city.
  if (CH_CITY_RE.test(s)) return true;

  // CH postal code with a letter-neighbour.
  const m = s.match(CH_POSTAL_RE);
  if (m) {
    const n = parseInt(m[1], 10);
    if (n >= 1000 && n <= 9658) return true;
  }

  return false;
}

// ---------------------------------------------------------------------------
// XML parser
// ---------------------------------------------------------------------------

const xml = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  isArray: (name) => ["position", "jobDescription"].includes(name),
});

type PersonioPos = {
  id: number | string;
  subcompany?: string;
  office?: string;
  department?: string;
  recruitingCategory?: string;
  name: string;
  jobDescriptions?: {
    jobDescription?: { name: string; value: string }[];
  };
  employmentType?: string;
  seniority?: string;
  schedule?: string;
  yearsOfExperience?: string;
  keywords?: string;
  occupation?: string;
  occupationCategory?: string;
  createdAt?: string;
};

async function fetchWithTimeout(url: string, ms: number): Promise<Response> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    return await fetch(url, {
      redirect: "manual", // 301 bounce = dead tenant
      signal: ctrl.signal,
      headers: { "User-Agent": "tinjob-bot/1.0 (+https://tinjob.ch)" },
    });
  } finally {
    clearTimeout(t);
  }
}

// ---------------------------------------------------------------------------
// Per-tenant processor
// ---------------------------------------------------------------------------

type TenantRow = {
  id: number;
  tenant: string;
  feed_url: string;
  consecutive_fails: number | null;
};

type TenantResult = {
  tenant_id: number;
  status: "active" | "empty" | "dead" | "error";
  jobs_total: number;
  jobs_ch: number;
  error?: string;
  positions?: PersonioPos[];
  country?: string;
};

async function processTenant(row: TenantRow): Promise<TenantResult> {
  let resp: Response;
  try {
    resp = await fetchWithTimeout(row.feed_url, FEED_TIMEOUT_MS);
  } catch (e: any) {
    return { tenant_id: row.id, status: "error", jobs_total: 0, jobs_ch: 0, error: e.message };
  }

  if (resp.status === 301 || resp.status === 302 || resp.status === 404) {
    return { tenant_id: row.id, status: "dead", jobs_total: 0, jobs_ch: 0, error: `http ${resp.status}` };
  }
  if (!resp.ok) {
    return { tenant_id: row.id, status: "error", jobs_total: 0, jobs_ch: 0, error: `http ${resp.status}` };
  }

  const text = await resp.text();
  let parsed: any;
  try {
    parsed = xml.parse(text);
  } catch (e: any) {
    return { tenant_id: row.id, status: "error", jobs_total: 0, jobs_ch: 0, error: `xml parse: ${e.message}` };
  }

  const positions: PersonioPos[] = parsed?.["workzag-jobs"]?.position ?? [];
  const chPositions = positions.filter((p) => isSwissOffice(p.office));

  // infer country for the tenant: if any CH position → ch; else pick majority office country (coarse)
  const country = chPositions.length > 0 ? "CH" : undefined;

  return {
    tenant_id: row.id,
    status: positions.length === 0 ? "empty" : "active",
    jobs_total: positions.length,
    jobs_ch: chPositions.length,
    positions: chPositions,
    country,
  };
}

// ---------------------------------------------------------------------------
// Upsert positions into job_listings
// ---------------------------------------------------------------------------

function htmlToText(html: string | undefined): string {
  if (!html) return "";
  return html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function buildDescription(p: PersonioPos): string {
  const blocks = p.jobDescriptions?.jobDescription ?? [];
  return blocks
    .map((b) => `## ${b.name}\n\n${htmlToText(b.value)}`)
    .join("\n\n");
}

function mapEmploymentGrade(schedule?: string, employmentType?: string): string | null {
  const s = (schedule || "").toLowerCase();
  if (s.includes("full")) return "full_time";
  if (s.includes("part")) return "part_time";
  if (s.includes("intern")) return "internship";
  const e = (employmentType || "").toLowerCase();
  if (e.includes("intern")) return "internship";
  if (e.includes("permanent")) return "full_time";
  if (e.includes("temporary")) return "contract";
  if (e.includes("freelance")) return "freelance";
  return null;
}

async function upsertPositions(
  tenantId: number,
  tenantSlug: string,
  companyName: string | null,
  positions: PersonioPos[],
): Promise<number> {
  if (positions.length === 0) return 0;

  const source = "personio";
  const rows = positions.map((p) => {
    const applyUrl = `https://${tenantSlug}.jobs.personio.de/job/${p.id}`;
    const externalId = `${tenantSlug}:${p.id}`;
    return {
      id: `${source}:${externalId}`,
      source,
      external_id: externalId,
      ats_tenant_id: tenantId,
      title: p.name,
      company: companyName || p.subcompany || tenantSlug,
      location: p.office ?? null,
      description: buildDescription(p),
      job_url: applyUrl,
      job_type: p.recruitingCategory ?? p.occupationCategory ?? null,
      employment_grade: mapEmploymentGrade(p.schedule, p.employmentType),
      seniority_detected: p.seniority ?? null,
      posted_date: p.createdAt ? new Date(p.createdAt).toISOString() : null,
      keywords_extracted: p.keywords
        ? p.keywords.split(/[,;]/).map((s) => s.trim()).filter(Boolean)
        : null,
      raw_data: p as unknown as Record<string, unknown>,
      is_active: true,
      ingested_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      last_seen_at: new Date().toISOString(),
    };
  });

  const { error } = await sb
    .from("job_listings")
    .upsert(rows, { onConflict: "source,external_id", ignoreDuplicates: false });

  if (error) {
    console.error(`  [${tenantSlug}] upsert error: ${error.message}`);
    return 0;
  }
  return rows.length;
}

// ---------------------------------------------------------------------------
// Tenant bookkeeping
// ---------------------------------------------------------------------------

async function recordTenantResult(row: TenantRow, r: TenantResult) {
  const fails = (row.consecutive_fails ?? 0);
  let newFails = fails;
  let status = r.status === "active" || r.status === "empty" ? "active" : "error";

  if (r.status === "error") newFails = fails + 1;
  else if (r.status === "dead") newFails = Math.max(fails + 1, DEAD_AFTER_FAILS);
  else newFails = 0;

  if (newFails >= DEAD_AFTER_FAILS) status = "dead";

  await sb
    .from("ats_tenants")
    .update({
      status,
      last_scraped_at: new Date().toISOString(),
      last_error: r.error ?? null,
      consecutive_fails: newFails,
      jobs_seen_last: r.jobs_total,
      has_swiss_jobs: (r.jobs_ch ?? 0) > 0,
      country: r.country ?? null,
    })
    .eq("id", row.id);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log("=== tinjob-personio ===");
  console.log(`Target: ${SUPABASE_URL}`);

  // Paginate tenants (Supabase caps select at 1000 rows).
  const PAGE = 1000;
  const tenants: any[] = [];
  let from = 0;
  while (true) {
    const to = from + PAGE - 1;
    const { data, error } = await sb
      .from("ats_tenants")
      .select("id, tenant, feed_url, consecutive_fails, company_name")
      .eq("ats_type", "personio")
      .in("status", ["active", "pending"])
      .order("id")
      .range(from, to);
    if (error) {
      console.error("tenant query failed:", error.message);
      process.exit(1);
    }
    if (!data || data.length === 0) break;
    tenants.push(...data);
    if (data.length < PAGE) break;
    if (MAX_TENANTS > 0 && tenants.length >= MAX_TENANTS) break;
    from += PAGE;
  }
  if (MAX_TENANTS > 0) tenants.length = Math.min(tenants.length, MAX_TENANTS);
  if (tenants.length === 0) {
    console.log("no tenants to scrape");
    return;
  }

  console.log(`Processing ${tenants.length} tenants (concurrency=${CONCURRENCY})`);

  let chJobsTotal = 0;
  let tenantsWithCh = 0;
  let tenantsActive = 0;
  let tenantsDead = 0;
  let tenantsError = 0;
  let processed = 0;

  for (let i = 0; i < tenants.length; i += CONCURRENCY) {
    const batch = tenants.slice(i, i + CONCURRENCY);
    const results = await Promise.all(batch.map((t) => processTenant(t as any)));

    for (let k = 0; k < results.length; k++) {
      const row = batch[k] as any;
      const r = results[k];

      if (r.status === "active" || r.status === "empty") tenantsActive++;
      else if (r.status === "dead") tenantsDead++;
      else tenantsError++;

      if (r.jobs_ch > 0) {
        tenantsWithCh++;
        const inserted = await upsertPositions(
          row.id,
          row.tenant,
          row.company_name ?? null,
          r.positions ?? [],
        );
        chJobsTotal += inserted;
      }

      await recordTenantResult(row, r);
    }

    processed += batch.length;
    if (processed % 500 === 0 || processed === tenants.length) {
      console.log(
        `  progress: ${processed}/${tenants.length} · active=${tenantsActive} dead=${tenantsDead} err=${tenantsError} · CH tenants=${tenantsWithCh} · CH jobs=${chJobsTotal}`,
      );
    }
  }

  console.log("\n=== summary ===");
  console.log(`tenants processed : ${processed}`);
  console.log(`  active           : ${tenantsActive}`);
  console.log(`  dead             : ${tenantsDead}`);
  console.log(`  error            : ${tenantsError}`);
  console.log(`tenants w/ CH jobs : ${tenantsWithCh}`);
  console.log(`CH jobs upserted   : ${chJobsTotal}`);
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
