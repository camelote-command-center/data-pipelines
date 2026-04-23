/**
 * tinjob-greenhouse
 *
 * Phase 1 Greenhouse ATS parser.
 * Endpoint: https://boards-api.greenhouse.io/v1/boards/<tenant>/jobs?content=true
 *
 * Response: { jobs: [{ id, title, location:{name}, absolute_url, content?, updated_at, first_published, company_name, metadata?, departments?, offices? }] }
 *
 * Env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, optional MAX_TENANTS
 */

import {
  CONCURRENCY,
  DEAD_AFTER_FAILS,
  FEED_TIMEOUT_MS,
  fetchWithTimeout,
  getClient,
  htmlToText,
  isSwissLocation,
  JobRow,
  loadTenants,
  mapEmploymentGrade,
  recordTenantResult,
  TenantResult,
  TenantRow,
  upsertJobs,
} from "./_common";

const SOURCE = "greenhouse";
const MAX_TENANTS = process.env.MAX_TENANTS ? parseInt(process.env.MAX_TENANTS, 10) : 0;

type GHJob = {
  id: number;
  title: string;
  location?: { name?: string };
  absolute_url: string;
  content?: string;
  updated_at?: string;
  first_published?: string;
  company_name?: string;
  departments?: { name: string }[];
  offices?: { name?: string; location?: string }[];
  metadata?: { name: string; value: unknown }[] | null;
};

function jobIsSwiss(j: GHJob): boolean {
  if (isSwissLocation(j.location?.name)) return true;
  for (const o of j.offices ?? []) {
    if (isSwissLocation(o.name)) return true;
    if (isSwissLocation(o.location)) return true;
  }
  return false;
}

function buildRow(tenant: TenantRow, j: GHJob): JobRow {
  const companyName = tenant.company_name || j.company_name || tenant.tenant;
  return {
    source: SOURCE,
    external_id: `${tenant.tenant}:${j.id}`,
    tenant_id: tenant.id,
    title: j.title,
    company: companyName,
    location: j.location?.name ?? null,
    description: j.content ? htmlToText(j.content) : "",
    job_url: j.absolute_url,
    job_type: j.departments?.[0]?.name ?? null,
    employment_grade: null,
    seniority_detected: null,
    posted_date: j.first_published ? new Date(j.first_published).toISOString() : null,
    keywords: null,
    raw: j as unknown as Record<string, unknown>,
  };
}

async function processTenant(row: TenantRow): Promise<{ result: TenantResult; rows: JobRow[] }> {
  const url = `${row.feed_url}?content=true`;
  let resp: Response;
  try {
    resp = await fetchWithTimeout(url, FEED_TIMEOUT_MS);
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

  const jobs: GHJob[] = body?.jobs ?? [];
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

    // flatten upsert batch per-concurrency-window for throughput
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

    // chunk upsert to stay under request limits
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
  console.log(`DEAD_AFTER_FAILS  : ${DEAD_AFTER_FAILS}`);
}

main().catch((e) => {
  console.error("fatal:", e);
  process.exit(1);
});
