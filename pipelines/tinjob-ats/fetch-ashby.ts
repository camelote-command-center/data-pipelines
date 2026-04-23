/**
 * tinjob-ashby
 *
 * Phase 1 Ashby ATS parser.
 * Endpoint: https://api.ashbyhq.com/posting-api/job-board/<tenant>?includeCompensation=false
 *
 * Response: { jobs: [{ id, title, department, team, employmentType, location, secondaryLocations:[{location,address}], publishedAt, jobUrl, descriptionHtml, address:{postalAddress:{addressCountry,addressLocality,addressRegion}} }] }
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

const SOURCE = "ashby";
const MAX_TENANTS = process.env.MAX_TENANTS ? parseInt(process.env.MAX_TENANTS, 10) : 0;

type AshbyAddress = {
  postalAddress?: {
    addressCountry?: string;
    addressLocality?: string;
    addressRegion?: string;
    postalCode?: string;
  };
};

type AshbyJob = {
  id: string;
  title: string;
  department?: string;
  team?: string;
  employmentType?: string;
  location?: string;
  secondaryLocations?: { location?: string; address?: AshbyAddress }[];
  publishedAt?: string;
  isListed?: boolean;
  isRemote?: boolean;
  workplaceType?: string;
  address?: AshbyAddress;
  jobUrl?: string;
  applyUrl?: string;
  descriptionHtml?: string;
};

function addressIsSwiss(a?: AshbyAddress): boolean {
  if (!a?.postalAddress) return false;
  if (isSwissCountryCode(a.postalAddress.addressCountry)) return true;
  const composed = [a.postalAddress.addressLocality, a.postalAddress.addressRegion, a.postalAddress.addressCountry, a.postalAddress.postalCode].filter(Boolean).join(", ");
  return isSwissLocation(composed);
}

function jobIsSwiss(j: AshbyJob): boolean {
  if (isSwissLocation(j.location)) return true;
  if (addressIsSwiss(j.address)) return true;
  for (const s of j.secondaryLocations ?? []) {
    if (isSwissLocation(s.location)) return true;
    if (addressIsSwiss(s.address)) return true;
  }
  return false;
}

function buildRow(tenant: TenantRow, j: AshbyJob): JobRow {
  const companyName = tenant.company_name || tenant.tenant;
  return {
    source: SOURCE,
    external_id: `${tenant.tenant}:${j.id}`,
    tenant_id: tenant.id,
    title: j.title.trim(),
    company: companyName,
    location: j.location ?? null,
    description: htmlToText(j.descriptionHtml),
    job_url: j.jobUrl || j.applyUrl || `https://jobs.ashbyhq.com/${tenant.tenant}/${j.id}`,
    job_type: j.department ?? null,
    employment_grade: mapEmploymentGrade(j.employmentType),
    seniority_detected: null,
    posted_date: j.publishedAt ? new Date(j.publishedAt).toISOString() : null,
    keywords: null,
    raw: j as unknown as Record<string, unknown>,
  };
}

async function processTenant(row: TenantRow): Promise<{ result: TenantResult; rows: JobRow[] }> {
  let resp: Response;
  try {
    resp = await fetchWithTimeout(row.feed_url, FEED_TIMEOUT_MS);
  } catch (e: any) {
    return { result: { status: "error", jobs_total: 0, jobs_ch: 0, error: e.message }, rows: [] };
  }
  if (resp.status === 404 || resp.status === 301 || resp.status === 302) {
    return { result: { status: "dead", jobs_total: 0, jobs_ch: 0, error: `http ${resp.status}` }, rows: [] };
  }
  if (!resp.ok) {
    return { result: { status: "error", jobs_total: 0, jobs_ch: 0, error: `http ${resp.status}` }, rows: [] };
  }
  let body: any;
  try {
    body = await resp.json();
  } catch (e: any) {
    return { result: { status: "error", jobs_total: 0, jobs_ch: 0, error: `json parse: ${e.message}` }, rows: [] };
  }

  const jobs: AshbyJob[] = (body?.jobs ?? []).filter((j: AshbyJob) => j.isListed !== false);
  const ch = jobs.filter(jobIsSwiss);
  const rows = ch.map((j) => buildRow(row, j));
  return {
    result: {
      status: jobs.length === 0 ? "empty" : "active",
      jobs_total: jobs.length,
      jobs_ch: ch.length,
      country: ch.length > 0 ? "CH" : undefined,
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
