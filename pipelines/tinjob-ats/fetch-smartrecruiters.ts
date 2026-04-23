/**
 * tinjob-smartrecruiters
 *
 * Phase 1 SmartRecruiters ATS parser.
 * List endpoint: https://api.smartrecruiters.com/v1/companies/<tenant>/postings?country=ch&limit=100&offset=N
 * Detail endpoint (for descriptions): https://api.smartrecruiters.com/v1/companies/<tenant>/postings/<postingId>
 *
 * We use country=ch so the API only returns CH jobs — zero CPU spent on non-CH
 * postings. Detail call is skipped unless we find CH jobs (rare).
 *
 * Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, optional MAX_TENANTS
 */

import {
  CONCURRENCY,
  FEED_TIMEOUT_MS,
  fetchWithTimeout,
  getClient,
  htmlToText,
  isSwissCountryCode,
  isSwissLocation,
  JobRow,
  loadTenants,
  mapEmploymentGrade,
  recordTenantResult,
  TenantResult,
  TenantRow,
  upsertJobs,
} from "./_common";

const SOURCE = "smartrecruiters";
const MAX_TENANTS = process.env.MAX_TENANTS ? parseInt(process.env.MAX_TENANTS, 10) : 0;

type SRPosting = {
  id: string;
  name: string;
  uuid?: string;
  refNumber?: string;
  releasedDate?: string;
  location?: {
    city?: string;
    region?: string;
    country?: string;
    remote?: boolean;
    fullLocation?: string;
  };
  industry?: { label?: string };
  department?: { label?: string };
  typeOfEmployment?: { id?: string; label?: string };
  experienceLevel?: { id?: string; label?: string };
  company?: { identifier?: string; name?: string };
  ref?: string;
};

type SRDetail = SRPosting & {
  jobAd?: {
    sections?: {
      companyDescription?: { title?: string; text?: string };
      jobDescription?: { title?: string; text?: string };
      qualifications?: { title?: string; text?: string };
      additionalInformation?: { title?: string; text?: string };
    };
  };
};

function locIsSwiss(l?: SRPosting["location"]): boolean {
  if (!l) return false;
  if (isSwissCountryCode(l.country)) return true;
  const composed = [l.city, l.region, l.country, l.fullLocation].filter(Boolean).join(", ");
  return isSwissLocation(composed);
}

async function fetchTenantCH(tenant: string): Promise<SRPosting[]> {
  const out: SRPosting[] = [];
  let offset = 0;
  const limit = 100;
  while (true) {
    const url = `https://api.smartrecruiters.com/v1/companies/${encodeURIComponent(tenant)}/postings?country=ch&limit=${limit}&offset=${offset}`;
    const resp = await fetchWithTimeout(url, FEED_TIMEOUT_MS);
    if (!resp.ok) {
      if (resp.status === 404) throw Object.assign(new Error(`http 404`), { dead: true });
      throw new Error(`http ${resp.status}`);
    }
    const body: any = await resp.json();
    const content: SRPosting[] = body?.content ?? [];
    out.push(...content);
    const total = body?.totalFound ?? 0;
    offset += content.length;
    if (content.length === 0 || offset >= total) break;
    if (out.length > 2000) break; // safety
  }
  return out;
}

async function fetchDetail(tenant: string, postingId: string): Promise<SRDetail | null> {
  try {
    const url = `https://api.smartrecruiters.com/v1/companies/${encodeURIComponent(tenant)}/postings/${postingId}`;
    const resp = await fetchWithTimeout(url, FEED_TIMEOUT_MS);
    if (!resp.ok) return null;
    return (await resp.json()) as SRDetail;
  } catch {
    return null;
  }
}

function buildDescription(d: SRDetail | null): string {
  const s = d?.jobAd?.sections;
  if (!s) return "";
  const parts: string[] = [];
  if (s.companyDescription?.text) parts.push(`## ${s.companyDescription.title ?? "Company"}\n\n${htmlToText(s.companyDescription.text)}`);
  if (s.jobDescription?.text) parts.push(`## ${s.jobDescription.title ?? "Job"}\n\n${htmlToText(s.jobDescription.text)}`);
  if (s.qualifications?.text) parts.push(`## ${s.qualifications.title ?? "Qualifications"}\n\n${htmlToText(s.qualifications.text)}`);
  if (s.additionalInformation?.text) parts.push(`## ${s.additionalInformation.title ?? "More"}\n\n${htmlToText(s.additionalInformation.text)}`);
  return parts.join("\n\n");
}

function publicJobUrl(tenant: string, p: SRPosting): string {
  return `https://careers.smartrecruiters.com/${encodeURIComponent(tenant)}/${p.id}`;
}

function buildRow(tenant: TenantRow, p: SRPosting, detail: SRDetail | null): JobRow {
  const companyName = tenant.company_name || p.company?.name || tenant.tenant;
  return {
    source: SOURCE,
    external_id: `${tenant.tenant}:${p.id}`,
    tenant_id: tenant.id,
    title: p.name,
    company: companyName,
    location: p.location?.fullLocation ?? [p.location?.city, p.location?.region, p.location?.country].filter(Boolean).join(", ") ?? null,
    description: buildDescription(detail),
    job_url: publicJobUrl(tenant.tenant, p),
    job_type: p.department?.label ?? p.industry?.label ?? null,
    employment_grade: mapEmploymentGrade(p.typeOfEmployment?.label || p.typeOfEmployment?.id),
    seniority_detected: p.experienceLevel?.label ?? null,
    posted_date: p.releasedDate ? new Date(p.releasedDate).toISOString() : null,
    keywords: null,
    raw: (detail || p) as unknown as Record<string, unknown>,
  };
}

async function processTenant(row: TenantRow): Promise<{ result: TenantResult; rows: JobRow[] }> {
  let postings: SRPosting[];
  try {
    postings = await fetchTenantCH(row.tenant);
  } catch (e: any) {
    if (e.dead) return { result: { status: "dead", jobs_total: 0, jobs_ch: 0, error: e.message }, rows: [] };
    return { result: { status: "error", jobs_total: 0, jobs_ch: 0, error: e.message }, rows: [] };
  }

  // country=ch already filtered, but re-check to catch false positives
  const ch = postings.filter((p) => locIsSwiss(p.location));
  if (ch.length === 0) {
    return {
      result: {
        status: postings.length === 0 ? "empty" : "active",
        jobs_total: postings.length,
        jobs_ch: 0,
      },
      rows: [],
    };
  }

  // Fetch details for CH jobs only (serialised to avoid hammering a single tenant)
  const rows: JobRow[] = [];
  for (const p of ch) {
    const d = await fetchDetail(row.tenant, p.id);
    rows.push(buildRow(row, p, d));
  }
  return {
    result: {
      status: "active",
      jobs_total: postings.length,
      jobs_ch: ch.length,
      country: "CH",
    },
    rows,
  };
}

async function main() {
  console.log(`=== tinjob-${SOURCE} ===`);
  const sb = getClient();
  console.log(`Target: ${process.env.SUPABASE_URL}`);

  const tenants = await loadTenants(sb, SOURCE, MAX_TENANTS);
  if (tenants.length === 0) {
    console.log("no tenants to scrape");
    return;
  }
  console.log(`Processing ${tenants.length} tenants (concurrency=${CONCURRENCY})`);

  let chJobsTotal = 0,
    tenantsWithCh = 0,
    tenantsActive = 0,
    tenantsDead = 0,
    tenantsError = 0,
    processed = 0;

  for (let i = 0; i < tenants.length; i += CONCURRENCY) {
    const batch = tenants.slice(i, i + CONCURRENCY);
    const results = await Promise.all(batch.map((t) => processTenant(t)));
    const allRows: JobRow[] = [];
    for (let k = 0; k < results.length; k++) {
      const { result, rows } = results[k];
      const row = batch[k];
      if (result.status === "active" || result.status === "empty") tenantsActive++;
      else if (result.status === "dead") tenantsDead++;
      else tenantsError++;
      if (result.jobs_ch > 0) tenantsWithCh++;
      allRows.push(...rows);
      await recordTenantResult(sb, row, result);
    }
    const CHUNK = 500;
    for (let off = 0; off < allRows.length; off += CHUNK) {
      chJobsTotal += await upsertJobs(sb, allRows.slice(off, off + CHUNK));
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
  console.log(`  active          : ${tenantsActive}`);
  console.log(`  dead            : ${tenantsDead}`);
  console.log(`  error           : ${tenantsError}`);
  console.log(`tenants w/ CH jobs: ${tenantsWithCh}`);
  console.log(`CH jobs upserted  : ${chJobsTotal}`);
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
