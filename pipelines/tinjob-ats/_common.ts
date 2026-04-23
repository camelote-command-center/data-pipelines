/**
 * tinjob-ats shared helpers
 *
 * CH detection (strict), tenant pagination, upsert, bookkeeping.
 * Used by fetch-greenhouse / fetch-ashby / fetch-smartrecruiters.
 */

import { createClient, SupabaseClient } from "@supabase/supabase-js";

export const CONCURRENCY = 20;
export const FEED_TIMEOUT_MS = 10_000;
export const DEAD_AFTER_FAILS = 5;

export function getClient(): SupabaseClient {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    console.error("ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required");
    process.exit(1);
  }
  return createClient(url, key);
}

// ---------------------------------------------------------------------------
// Switzerland detector (strict)
// ---------------------------------------------------------------------------

const COUNTRY_MARKERS = [
  "switzerland", "schweiz", "suisse", "svizzera", "helvetia",
  "schweizer", "suiza", "swiss",
];

const CANTON_CODES = [
  "ZH","BE","LU","UR","SZ","OW","NW","GL","ZG","FR","SO","BS","BL",
  "SH","AR","AI","SG","GR","AG","TG","TI","VD","VS","NE","GE","JU",
];

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
  "fribourg",
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
  "(?:[,\\s\\-(/])(" + CANTON_CODES.join("|") + ")(?=[\\s,)\\/.\\-]|$)",
);
const CH_CITY_RE = new RegExp(
  "\\b(" + UNAMBIGUOUS_CH_CITIES
    .map((w) => w.replace(/\s+/g, "\\s+").replace(/\./g, "\\.?"))
    .join("|") + ")\\b",
  "i",
);
const CH_POSTAL_RE = /(?:\bCH[-\s]?|\b)([1-9]\d{3})(?=[\s-][A-Za-zÀ-ÿ])/;

/**
 * Returns true if the location string unambiguously refers to Switzerland.
 * Accepts any concatenation of city / region / country.
 */
export function isSwissLocation(loc?: string | null): boolean {
  if (!loc) return false;
  const s = loc.trim();
  if (!s) return false;

  if (COUNTRY_RE.test(s)) return true;
  if (CANTON_RE.test(s)) return true;
  if (CH_CITY_RE.test(s)) return true;

  const m = s.match(CH_POSTAL_RE);
  if (m) {
    const n = parseInt(m[1], 10);
    if (n >= 1000 && n <= 9658) return true;
  }
  return false;
}

/**
 * Looser country-field check — when the ATS exposes an ISO country code.
 */
export function isSwissCountryCode(code?: string | null): boolean {
  if (!code) return false;
  const c = code.trim().toLowerCase();
  return c === "ch" || c === "che" || c === "switzerland";
}

// ---------------------------------------------------------------------------
// Fetch with timeout
// ---------------------------------------------------------------------------

export async function fetchWithTimeout(
  url: string,
  ms: number = FEED_TIMEOUT_MS,
): Promise<Response> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    return await fetch(url, {
      redirect: "manual",
      signal: ctrl.signal,
      headers: { "User-Agent": "tinjob-bot/1.0 (+https://tinjob.ch)" },
    });
  } finally {
    clearTimeout(t);
  }
}

// ---------------------------------------------------------------------------
// Tenant pagination + bookkeeping
// ---------------------------------------------------------------------------

export type TenantRow = {
  id: number;
  tenant: string;
  feed_url: string;
  consecutive_fails: number | null;
  company_name: string | null;
};

export async function loadTenants(
  sb: SupabaseClient,
  atsType: string,
  maxTenants: number = 0,
): Promise<TenantRow[]> {
  const PAGE = 1000;
  const tenants: TenantRow[] = [];
  let from = 0;
  while (true) {
    const to = from + PAGE - 1;
    const { data, error } = await sb
      .from("ats_tenants")
      .select("id, tenant, feed_url, consecutive_fails, company_name")
      .eq("ats_type", atsType)
      .in("status", ["active", "pending"])
      .order("id")
      .range(from, to);
    if (error) {
      console.error("tenant query failed:", error.message);
      process.exit(1);
    }
    if (!data || data.length === 0) break;
    tenants.push(...(data as TenantRow[]));
    if (data.length < PAGE) break;
    if (maxTenants > 0 && tenants.length >= maxTenants) break;
    from += PAGE;
  }
  if (maxTenants > 0) tenants.length = Math.min(tenants.length, maxTenants);
  return tenants;
}

export type TenantResult = {
  status: "active" | "empty" | "dead" | "error";
  jobs_total: number;
  jobs_ch: number;
  error?: string;
  country?: string;
};

export async function recordTenantResult(
  sb: SupabaseClient,
  row: TenantRow,
  r: TenantResult,
) {
  const fails = row.consecutive_fails ?? 0;
  let newFails = fails;
  let status: string = r.status === "active" || r.status === "empty" ? "active" : "error";

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
// HTML → text
// ---------------------------------------------------------------------------

export function htmlToText(html: string | undefined | null): string {
  if (!html) return "";
  return html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n\n")
    .replace(/<li[^>]*>/gi, "• ")
    .replace(/<\/li>/gi, "\n")
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

// ---------------------------------------------------------------------------
// Upsert job_listings
// ---------------------------------------------------------------------------

export type JobRow = {
  source: string;
  external_id: string;
  tenant_id: number;
  title: string;
  company: string;
  location: string | null;
  description: string;
  job_url: string;
  job_type?: string | null;
  employment_grade?: string | null;
  seniority_detected?: string | null;
  posted_date?: string | null;
  keywords?: string[] | null;
  raw: Record<string, unknown>;
};

export async function upsertJobs(
  sb: SupabaseClient,
  rows: JobRow[],
): Promise<number> {
  if (rows.length === 0) return 0;
  const now = new Date().toISOString();
  const payload = rows.map((r) => ({
    id: `${r.source}:${r.external_id}`,
    source: r.source,
    external_id: r.external_id,
    ats_tenant_id: r.tenant_id,
    title: r.title,
    company: r.company,
    location: r.location,
    description: r.description,
    job_url: r.job_url,
    job_type: r.job_type ?? null,
    employment_grade: r.employment_grade ?? null,
    seniority_detected: r.seniority_detected ?? null,
    posted_date: r.posted_date ?? null,
    keywords_extracted: r.keywords ?? null,
    raw_data: r.raw,
    is_active: true,
    ingested_at: now,
    updated_at: now,
    last_seen_at: now,
  }));
  const { error } = await sb
    .from("job_listings")
    .upsert(payload, { onConflict: "source,external_id", ignoreDuplicates: false });
  if (error) {
    console.error(`  upsert error (${rows[0].source}): ${error.message}`);
    return 0;
  }
  return payload.length;
}

export function mapEmploymentGrade(v?: string | null): string | null {
  if (!v) return null;
  const s = v.toLowerCase();
  if (s.includes("full")) return "full_time";
  if (s.includes("part")) return "part_time";
  if (s.includes("intern") || s.includes("praktik") || s.includes("stage")) return "internship";
  if (s.includes("contract") || s.includes("temporary") || s.includes("temp")) return "contract";
  if (s.includes("freelance")) return "freelance";
  if (s.includes("permanent") || s.includes("regular")) return "full_time";
  return null;
}
