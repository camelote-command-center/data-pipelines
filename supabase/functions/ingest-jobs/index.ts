import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const SUPABASE_DB_SCHEMA = "bronze_ch";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const TYPE_MAP: Record<string, string> = {
  "1": "CDI", "2": "CDD", "3": "Stage", "5": "Temporaire", "6": "Freelance"
};

// Search terms that cover the Swiss job market broadly
const DEFAULT_SEARCH_TERMS = [
  "analyst", "manager", "director", "consultant", "chef de projet",
  "responsable", "operations", "finance", "data", "real estate",
  "immobilier", "business", "wealth", "product", "project",
];



function normalizeJobType(input: {
  title?: string | null;
  description?: string | null;
  rawType?: string | null;
  raw?: any;
}): string | null {
  const title = (input.title || "").toLowerCase();
  const rawType = (input.rawType || "").toLowerCase();
  const text = `${title} ${rawType}`;

  if (/\b(stage|stagiaire|internship|intern|trainee|apprenti)\b/.test(text)) return "Stage";
  if (/\b(freelance|independent|contractor)\b/.test(text)) return "Freelance";
  if (/\bcdi\b/.test(title)) return "CDI";
  if (/\b(cdd|cdm|fixed[- ]term|fixed term|contrat.{0,20}dur[eé]e|maternity cover|remplacement)\b/.test(title)) return "CDD";
  if (/\b(temporaire|temporary|temp|int[ée]rim|interim|sur appel)\b/.test(title)) return "Temporaire";
  if (/\b(cdi|permanent|full[- ]time|part[- ]time)\b/.test(rawType)) return "CDI";
  if (/\b(cdd|cdm|fixed[- ]term|fixed term|contrat.{0,20}dur[eé]e|maternity cover|remplacement)\b/.test(rawType)) return "CDD";
  if (/\b(temporaire|temporary|temp|int[ée]rim|interim|sur appel)\b/.test(rawType)) return "Temporaire";
  if (/\bcontract\b/.test(rawType)) return "CDD";

  return null;
}

function cleanJobDescription(input: string): string {
  if (!input) return "";

  let cleaned = input
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "")
    .replace(/<iframe[^>]*>[\s\S]*?<\/iframe>/gi, "")
    .replace(/\s*style="[^"]*"/gi, "")
    .replace(/\s*class="[^"]*"/gi, "")
    .replace(/&#x?0*?a0;|&#160;|&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/ /g, " ");

  // SmartRecruiters often sends Markdown headings. Convert them to HTML so
  // previews strip cleanly and full descriptions render as real headings.
  cleaned = cleaned.replace(/^\s*#{1,6}\s+(.+)$/gm, (_match, title) => `<h3>${String(title).trim()}</h3>`);

  return cleaned
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

// Fetch full description from detail page LD+JSON
async function fetchFullDescription(detailUrl: string): Promise<string> {
  try {
    const res = await fetch(detailUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html", "Accept-Language": "fr-CH,fr;q=0.9,en;q=0.8",
      },
    });
    if (!res.ok) return "";
    const html = await res.text();
    const scripts = html.match(/<script[^>]*>([\s\S]*?)<\/script>/g) || [];
    for (const script of scripts) {
      if (script.includes("JobPosting") && script.includes("schema.org")) {
        const json = script.replace(/<script[^>]*>/, "").replace(/<\/script>/, "");
        try {
          let d = JSON.parse(json);
          if (Array.isArray(d)) d = d[0];
          // Keep HTML formatting (h1-h6, p, ul, ol, li, strong, em, a, br)
          // Strip only dangerous tags (script, style, iframe) and inline styles
          const raw = d?.description || "";
          return cleanJobDescription(raw);
        } catch { continue; }
      }
    }
    return "";
  } catch { return ""; }
}

async function searchPortal(baseUrl: string, keyword: string, location: string, rows: number): Promise<any[]> {
  const url = `${baseUrl}?query=${encodeURIComponent(keyword)}&location=${encodeURIComponent(location)}&rows=${rows}`;
  try {
    const res = await fetch(url, { headers: { "Accept": "application/json", "User-Agent": "TinJob/1.0" } });
    if (!res.ok) return [];
    return (await res.json()).documents || [];
  } catch { return []; }
}

// ─── Chunked location pagination (jobup.ch / jobs.ch) ───────────────────
// The old discovery — 15 keyword terms × rows=20 × page-1 — capped coverage
// at ~275 jobs and silently dropped most real Geneva listings (bug 7dda1d82,
// e.g. jobup f1d05593 "Client Life Cycle - KYC"). A naive full scan of the
// whole ~2350-job result set exceeds the Edge Function resource budget
// (WORKER_RESOURCE_LIMIT). So we sweep in cursor-advanced chunks — exactly
// like the ATS sweep: each invocation paginates PORTAL_PAGE_CHUNK pages from
// a persisted offset (ingest_state "portal_cursor"), then advances/wraps.
// A cron runs this often enough that a full sweep (~119 pages) completes
// ~daily. The accented "Genève" token covers the whole Grand-Genève region
// (region /34); the English "Geneva" token maps to a smaller node that drops
// /34 listings entirely.
const PORTAL_LOCATION = "Genève";
const PORTAL_PAGE_CHUNK = 10;   // ~200 jobs/run — within the proven-safe envelope
const PORTAL_MAX_PAGES = 200;   // per-portal safety cap (real depth ~73/~46)
const PAGE_CONCURRENCY = 5;

async function fetchPortalPage(baseUrl: string, location: string, page: number): Promise<any | null> {
  const url = `${baseUrl}?query=&location=${encodeURIComponent(location)}&rows=20&page=${page}`;
  try {
    const res = await fetch(url, { headers: { "Accept": "application/json", "User-Agent": "TinJob/1.0" } });
    if (!res.ok) return null;
    return await res.json();
  } catch { return null; }
}

// Flat page worklist across both portals: [jobup p1..pN, jobs.ch p1..pM].
// One probe fetch per portal establishes num_pages.
async function buildPortalPageList(
  portals: { url: string; source: string }[], location: string,
): Promise<{ url: string; source: string; page: number }[]> {
  const list: { url: string; source: string; page: number }[] = [];
  for (const p of portals) {
    const first = await fetchPortalPage(p.url, location, 1);
    const numPages = Math.min(first?.num_pages || 1, PORTAL_MAX_PAGES);
    for (let pg = 1; pg <= numPages; pg++) list.push({ url: p.url, source: p.source, page: pg });
  }
  return list;
}

// Size-capped JSON fetch — parsing a multi-MB board payload (Speechify:
// ~2900 jobs with full HTML) blows the Edge Function CPU/memory budget.
async function fetchJson(url: string, maxBytes = 5_000_000): Promise<any | null> {
  try {
    const res = await fetch(url, { headers: { "Accept": "application/json", "User-Agent": "TinJob/1.0" } });
    if (!res.ok) return null;
    const text = await res.text();
    if (text.length > maxBytes) {
      console.warn(`payload too large (${text.length} bytes), skipped: ${url}`);
      return null;
    }
    return JSON.parse(text);
  } catch { return null; }
}

// TinJob is a Swiss platform — ATS boards are global, so drop rows whose
// location is clearly foreign. Empty locations and remote roles are kept;
// map_job_cantons() and cleanup_stale_jobs() decide their fate downstream.
const SWISS_LOCATION_RE = /(switzerland|suisse|schweiz|svizzera|genev|geneve|lausanne|zurich|zürich|basel|bâle|bern|nyon|vaud|valais|fribourg|neuchatel|neuchâtel|lugano|ticino|zug|winterthur|lucerne|luzern|st\.? ?gallen|aarau|sion|romand|remote)/i;
function isSwissRelevant(location: string): boolean {
  if (!location || !location.trim()) return true;
  return SWISS_LOCATION_RE.test(location);
}

// Drop location-spam multiposts: the same job posted to dozens of cities
// ("Tech Lead, Android - Geneva, Switzerland" ×358, Speechify incident
// 2026-06-10). If a title-base (title minus trailing " - City, Country")
// appears >= 10 times within one company's batch, it is remote spam with
// fake locations — keep none of them.
function filterMultipostSpam(rows: any[]): any[] {
  const baseCounts = new Map<string, number>();
  // Strips " - City, Country" and " - City, ST, USA" suffixes; the city part
  // may itself contain hyphens (Winston-Salem) so it is comma-delimited.
  const baseOf = (title: string) => title.replace(/\s+-\s+[^,]+(,\s+[^,]+){1,2}$/, "");
  for (const r of rows) {
    const base = baseOf(r.title);
    if (base !== r.title) baseCounts.set(base, (baseCounts.get(base) || 0) + 1);
  }
  const spam = new Set([...baseCounts.entries()].filter(([, n]) => n >= 10).map(([b]) => b));
  if (spam.size > 0) {
    console.warn(`Multipost spam filtered: ${[...spam].join(" | ")}`);
  }
  return rows.filter(r => !spam.has(baseOf(r.title)));
}

async function getKnownSourceKeys(supabase: any, source: string, pattern: RegExp): Promise<string[]> {
  const keys = new Set<string>();
  for (let from = 0; from < 5000; from += 1000) {
    const { data, error } = await supabase
      .from("tinjob_job_listings")
      .select("job_url, raw_data")
      .eq("source", source)
      .order("id")
      .range(from, from + 999);
    if (error || !data || data.length === 0) break;
    for (const row of data) {
      const url = row.job_url || row.raw_data?.jobUrl || row.raw_data?.postingUrl || "";
      const match = String(url).match(pattern);
      if (match?.[1]) keys.add(match[1]);
    }
    if (data.length < 1000) break;
  }
  // Sorted for a stable order — the ATS sweep cursor depends on it.
  return [...keys].sort().slice(0, 120);
}

async function ingestGreenhouseBoard(supabase: any, board: string): Promise<{ found: number; upserted: number }> {
  let found = 0, upserted = 0;
  {
    // Pre-count without content: giant boards (city-multipost spam like
    // Speechify's ~2900 posts) are skipped before the expensive content fetch.
    const meta = await fetchJson(`https://boards-api.greenhouse.io/v1/boards/${board}/jobs`);
    const jobCount = meta?.jobs?.length ?? 0;
    if (jobCount === 0) return { found: 0, upserted: 0 };
    if (jobCount > 300) {
      console.warn(`greenhouse ${board}: ${jobCount} jobs — skipped as multipost/giant board`);
      return { found: jobCount, upserted: 0 };
    }
    const payload = await fetchJson(`https://boards-api.greenhouse.io/v1/boards/${board}/jobs?content=true`);
    const jobs = payload?.jobs || [];
    found += jobs.length;
    const rows = jobs.map((j: any) => ({
      id: `greenhouse:${board}:${j.id}`,
      source: "greenhouse",
      title: (j.title || "").trim(),
      company: board,
      location: j.location?.name || "",
      description: cleanJobDescription(j.content || ""),
      job_url: j.absolute_url || "",
      posted_date: j.updated_at || null,
      job_type: normalizeJobType({ title: j.title, description: j.content, rawType: null, raw: j }),
      employment_grade: null,
      raw_data: { ...j, board_token: board },
      is_active: true,
      updated_at: new Date().toISOString(),
    })).filter((r: any) => r.title && r.job_url);
    const cleanRows = filterMultipostSpam(rows).filter((r: any) => isSwissRelevant(r.location));
    if (cleanRows.length) {
      const { error } = await supabase.from("tinjob_job_listings").upsert(cleanRows, { onConflict: "id" });
      if (!error) upserted += cleanRows.length;
      else console.error(`greenhouse ${board}:`, error.message);
    }
  }
  return { found, upserted };
}

async function ingestAshbyOrg(supabase: any, org: string): Promise<{ found: number; upserted: number }> {
  let found = 0, upserted = 0;
  {
    const payload = await fetchJson(`https://api.ashbyhq.com/posting-api/job-board/${org}`);
    const jobs = payload?.jobs || [];
    found += jobs.length;
    const rows = jobs.map((j: any) => ({
      id: `ashby:${org}:${j.id}`,
      source: "ashby",
      title: (j.title || "").trim(),
      company: org,
      location: j.location || j.address?.postalAddress?.addressLocality || "",
      description: cleanJobDescription(j.descriptionHtml || j.descriptionPlain || ""),
      job_url: j.jobUrl || `https://jobs.ashbyhq.com/${org}/${j.id}`,
      posted_date: j.publishedAt || null,
      job_type: normalizeJobType({ title: j.title, description: j.descriptionHtml || j.descriptionPlain, rawType: j.employmentType, raw: j }),
      employment_grade: null,
      raw_data: { ...j, organization: org },
      is_active: true,
      updated_at: new Date().toISOString(),
    })).filter((r: any) => r.title && r.job_url);
    const cleanRows = filterMultipostSpam(rows).filter((r: any) => isSwissRelevant(r.location));
    if (cleanRows.length) {
      const { error } = await supabase.from("tinjob_job_listings").upsert(cleanRows, { onConflict: "id" });
      if (!error) upserted += cleanRows.length;
      else console.error(`ashby ${org}:`, error.message);
    }
  }
  return { found, upserted };
}

async function ingestPersonioCompany(supabase: any, company: string): Promise<{ found: number; upserted: number }> {
  let found = 0, upserted = 0;
  {
    const payload = await fetchJson(`https://${company}.jobs.personio.de/search.json?language=en`)
      || await fetchJson(`https://${company}.jobs.personio.com/search.json?language=en`);
    const jobs = payload?.jobs || payload?.positions || [];
    found += jobs.length;
    const rows = jobs.map((j: any) => ({
      id: `personio:${company}:${j.id}`,
      source: "personio",
      title: (j.name || j.title || "").trim(),
      company,
      location: j.office || j.location || "",
      description: cleanJobDescription(j.description || j.jobDescriptions?.jobDescription || ""),
      job_url: j.job_url || j.url || `https://${company}.jobs.personio.de/job/${j.id}`,
      posted_date: j.created_at || j.published_at || null,
      job_type: normalizeJobType({ title: j.name || j.title, description: j.description || j.jobDescriptions?.jobDescription, rawType: j.employmentType || j.schedule, raw: j }),
      employment_grade: null,
      raw_data: { ...j, company },
      is_active: true,
      updated_at: new Date().toISOString(),
    })).filter((r: any) => r.title && r.job_url);
    const cleanRows = filterMultipostSpam(rows).filter((r: any) => isSwissRelevant(r.location));
    if (cleanRows.length) {
      const { error } = await supabase.from("tinjob_job_listings").upsert(cleanRows, { onConflict: "id" });
      if (!error) upserted += cleanRows.length;
      else console.error(`personio ${company}:`, error.message);
    }
  }
  return { found, upserted };
}

async function getSmartRecruitersCompanies(supabase: any): Promise<string[]> {
  const { data } = await supabase
    .from("tinjob_job_listings")
    .select("raw_data")
    .eq("source", "smartrecruiters")
    .limit(5000);
  return [...new Set((data || []).map((r: any) => r.raw_data?.company?.identifier).filter(Boolean))]
    .sort()
    .slice(0, 140) as string[];
}

async function ingestSmartRecruitersCompany(supabase: any, company: string): Promise<{ found: number; upserted: number }> {
  let found = 0, upserted = 0;
  {
    const payload = await fetchJson(`https://api.smartrecruiters.com/v1/companies/${company}/postings?limit=100`);
    const jobs = payload?.content || [];
    found += jobs.length;
    const rows = jobs.map((j: any) => {
      const sections = j.jobAd?.sections || {};
      const description = [sections.companyDescription, sections.jobDescription, sections.qualifications, sections.additionalInformation]
        .filter((section: any) => section?.text)
        .map((section: any) => `${section.title ? `<h3>${section.title}</h3>` : ""}${section.text || ""}`)
        .join("\n\n");
      return {
        id: `smartrecruiters:${String(company).toLowerCase()}:${j.id || j.uuid}`,
        source: "smartrecruiters",
        title: (j.name || "").trim(),
        company: j.company?.name || company,
        location: j.location?.fullLocation || [j.location?.city, j.location?.region, j.location?.country].filter(Boolean).join(", "),
        description: cleanJobDescription(description || j.jobAd?.sections?.jobDescription?.text || ""),
        job_url: j.postingUrl || j.applyUrl || "",
        posted_date: j.releasedDate || null,
        job_type: normalizeJobType({ title: j.name, description, rawType: j.typeOfEmployment?.label || j.typeOfEmployment?.id, raw: j }),
        employment_grade: null,
        raw_data: j,
        is_active: true,
        updated_at: new Date().toISOString(),
      };
    }).filter((r: any) => r.title && r.job_url);
    const cleanRows = filterMultipostSpam(rows).filter((r: any) => isSwissRelevant(r.location));
    if (cleanRows.length) {
      const { error } = await supabase.from("tinjob_job_listings").upsert(cleanRows, { onConflict: "id" });
      if (!error) upserted += cleanRows.length;
      else console.error(`smartrecruiters ${company}:`, error.message);
    }
  }
  return { found, upserted };
}

// ─── ATS chunked sweep ──────────────────────────────────────────────────
// One invocation cannot scrape ~500 boards within the Edge Function CPU and
// wall-clock budgets (this is what left every ingestion_log row stuck in
// 'running'). A dedicated cron invokes {"phase":"ats"} every 15 minutes;
// each run processes the next CHUNK of boards and persists a cursor in
// ingest_state. A full sweep completes in ~4-5 hours, continuously.
const ATS_CHUNK_SIZE = 15;

async function buildAtsWorkList(supabase: any): Promise<Array<{ s: string; k: string }>> {
  const [gh, ab, po, sr] = await Promise.all([
    getKnownSourceKeys(supabase, "greenhouse", /greenhouse\.io\/(?:embed\/job_app\?for=|jobs\/)?([^/?#]+)/i),
    getKnownSourceKeys(supabase, "ashby", /jobs\.ashbyhq\.com\/([^/?#]+)/i),
    getKnownSourceKeys(supabase, "personio", /https?:\/\/([^/.]+)\.jobs\.personio\.(?:de|com)/i),
    getSmartRecruitersCompanies(supabase),
  ]);
  return [
    ...gh.map((k: string) => ({ s: "greenhouse", k })),
    ...ab.map((k: string) => ({ s: "ashby", k })),
    ...po.map((k: string) => ({ s: "personio", k })),
    ...sr.map((k: string) => ({ s: "smartrecruiters", k })),
  ];
}

const ATS_PROCESSORS: Record<string, (sb: any, key: string) => Promise<{ found: number; upserted: number }>> = {
  greenhouse: ingestGreenhouseBoard,
  ashby: ingestAshbyOrg,
  personio: ingestPersonioCompany,
  smartrecruiters: ingestSmartRecruitersCompany,
};

async function processAtsChunk(supabase: any, deadline: number): Promise<any> {
  const { data: stateRow } = await supabase
    .from("tinjob_ingest_state").select("value").eq("key", "ats_cursor").maybeSingle();
  const offset: number = stateRow?.value?.offset || 0;

  const work = await buildAtsWorkList(supabase);
  const slice = work.slice(offset, offset + ATS_CHUNK_SIZE);

  let found = 0, upserted = 0, processed = 0;
  for (const item of slice) {
    if (Date.now() > deadline) break;
    try {
      const r = await ATS_PROCESSORS[item.s](supabase, item.k);
      found += r.found;
      upserted += r.upserted;
    } catch (e) {
      console.error(`ats ${item.s}:${item.k}:`, e);
    }
    processed++;
  }

  const consumed = offset + processed;
  const nextOffset = consumed >= work.length ? 0 : consumed;
  await supabase.from("tinjob_ingest_state").upsert({
    key: "ats_cursor",
    value: {
      offset: nextOffset,
      total: work.length,
      last_run: new Date().toISOString(),
      last_processed: processed,
      last_found: found,
      last_upserted: upserted,
      sweep_completed: nextOffset === 0,
    },
    updated_at: new Date().toISOString(),
  }, { onConflict: "key" });

  return { phase: "ats", processed, found, upserted, next_offset: nextOffset, total: work.length };
}

// Normalize title+company for deduplication
function dedupeKey(title: string, company: string): string {
  return `${title.toLowerCase().trim()}|||${company.toLowerCase().trim()}`;
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  // Wall-clock budget: the Edge runtime kills the worker around 400s. Leave
  // room for the close-out so ingestion_log never stays stuck in 'running'.
  const startedAt = Date.now();
  const DEADLINE = startedAt + 280_000;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY).schema(SUPABASE_DB_SCHEMA);
  let logId: string | null = null;

  try {
    const body = await req.json().catch(() => ({}));

    // ATS sweep chunk — invoked by its own cron every 15 minutes.
    if (body.phase === "ats") {
      const result = await processAtsChunk(supabase, DEADLINE);
      return new Response(JSON.stringify(result),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const location = body.location || PORTAL_LOCATION;
    const chunkPages = body.chunk_pages || PORTAL_PAGE_CHUNK;

    // Create ingestion log entry
    const { data: logEntry, error: logInsertError } = await supabase.from("tinjob_ingestion_log").insert({
      search_terms_used: [`paginated:location=${location}`],
      status: "running",
    }).select("id").single();
    if (logInsertError) throw new Error(`tinjob_ingestion_log insert failed: ${logInsertError.message}`);
    logId = logEntry?.id;

    const portals = [
      { url: "https://www.jobup.ch/api/v1/public/search", source: "jobup.ch" },
      { url: "https://www.jobs.ch/api/v1/public/search", source: "jobs.ch" },
    ];

    // Discovery: one cursor-advanced chunk of the full location page worklist
    // (resource-safe). Was: 15 keyword terms × rows=20 × page-1 — bug 7dda1d82.
    const pageList = await buildPortalPageList(portals, location);
    const totalPages = pageList.length;
    const { data: cursorRow, error: cursorError } = await supabase
      .from("tinjob_ingest_state").select("value").eq("key", "portal_cursor").maybeSingle();
    if (cursorError) throw new Error(`tinjob_ingest_state read failed: ${cursorError.message}`);
    let offset: number = cursorRow?.value?.offset || 0;
    if (offset >= totalPages) offset = 0;
    const chunk = pageList.slice(offset, offset + chunkPages);

    // Fetch the chunk's pages (bounded concurrency), grouped by source. Leave
    // the bulk of the wall-clock budget for description enrichment below.
    const bySource = new Map<string, any[]>();
    for (let i = 0; i < chunk.length; i += PAGE_CONCURRENCY) {
      if (Date.now() > DEADLINE - 150_000) {
        console.warn(`portal fetch budget hit at ${i}/${chunk.length} pages`);
        break;
      }
      const batch = chunk.slice(i, i + PAGE_CONCURRENCY);
      const pages = await Promise.all(batch.map(c => fetchPortalPage(c.url, location, c.page)));
      pages.forEach((pg, k) => {
        const src = batch[k].source;
        if (!bySource.has(src)) bySource.set(src, []);
        bySource.get(src)!.push(...(pg?.documents || []));
      });
    }
    const results = [...bySource.entries()].map(([source, docs]) => ({ docs, source }));

    // Deduplicate by API job ID (within this ingestion run)
    const jobMapById = new Map<string, { raw: any; source: string }>();
    for (const { docs, source } of results) {
      for (const j of docs) {
        const id = j.job_id || j.datapool_id;
        if (id && !jobMapById.has(id)) {
          jobMapById.set(id, { raw: j, source });
        }
      }
    }

    // Cross-platform dedup: same title+company from different sources → keep first seen
    const jobMapByTitleCompany = new Map<string, { id: string; raw: any; source: string }>();
    for (const [id, { raw, source }] of jobMapById) {
      const title = (raw.title || "").trim();
      const company = (raw.company_name || "").trim();
      const key = dedupeKey(title, company);
      if (!jobMapByTitleCompany.has(key)) {
        jobMapByTitleCompany.set(key, { id, raw, source });
      }
    }

    const uniqueJobs = Array.from(jobMapByTitleCompany.values()).map(({ id, raw, source }) => [id, { raw, source }] as [string, { raw: any; source: string }]);
    let jobsNew = 0;
    let jobsUpdated = 0;
    let jobsSkippedDupe = 0;

    // Check which jobs already exist by ID
    const existingById = new Set<string>();
    if (uniqueJobs.length > 0) {
      const ids = uniqueJobs.map(([id]) => id);
      for (let i = 0; i < ids.length; i += 100) {
        const batch = ids.slice(i, i + 100);
        const { data } = await supabase.from("tinjob_job_listings").select("id").in("id", batch);
        (data || []).forEach((r: any) => existingById.add(r.id));
      }
    }

    // For jobs not found by ID, check if they already exist by title+company
    // (same job, different API-assigned UUID)
    const newByIdJobs = uniqueJobs.filter(([id]) => !existingById.has(id));
    const existingByTitleCompany = new Set<string>(); // dedupeKeys that already exist in DB

    if (newByIdJobs.length > 0) {
      // Batch-check: query existing active jobs with matching titles
      const titles = newByIdJobs.map(([, { raw }]) => (raw.title || "").trim()).filter(Boolean);
      const uniqueTitles = [...new Set(titles)];

      for (let i = 0; i < uniqueTitles.length; i += 50) {
        const batch = uniqueTitles.slice(i, i + 50);
        const { data } = await supabase
          .from("tinjob_job_listings")
          .select("id,title,company")
          .in("title", batch)
          .eq("is_active", true);

        if (data) {
          for (const row of data) {
            existingByTitleCompany.add(dedupeKey(row.title, row.company));
          }
        }
      }
    }

    // Filter out jobs that already exist by title+company
    const trulyNewJobs = newByIdJobs.filter(([, { raw }]) => {
      const title = (raw.title || "").trim();
      const company = (raw.company_name || "").trim();
      const key = dedupeKey(title, company);
      if (existingByTitleCompany.has(key)) {
        jobsSkippedDupe++;
        return false;
      }
      return true;
    });

    // Fetch descriptions and insert — only for truly new jobs
    const BATCH = 6;
    for (let i = 0; i < trulyNewJobs.length; i += BATCH) {
      // Reserve the tail of the budget for the ATS phase + log close-out;
      // jobs skipped here are still "new" next run and get picked up then.
      if (Date.now() > DEADLINE - 120_000) {
        console.warn(`Description-fetch budget hit at ${i}/${trulyNewJobs.length} new jobs`);
        break;
      }
      const batch = trulyNewJobs.slice(i, i + BATCH);

      // Fetch full descriptions in parallel
      const descPromises = batch.map(([, { raw: j, source }]) => {
        const link = source === "jobup.ch"
          ? j._links?.detail_fr?.href
          : (j._links?.detail_en?.href || j._links?.detail_de?.href);
        return link ? fetchFullDescription(link) : Promise.resolve("");
      });
      const descriptions = await Promise.all(descPromises);

      // Prepare upsert data
      const rows = batch.map(([id, { raw: j, source }], k) => {
        const et = j.tags?.find((t: any) => t.type === "employment_type");
        const eg = j.tags?.find((t: any) => t.type === "employment_grade");
        const link = source === "jobup.ch"
          ? j._links?.detail_fr?.href
          : (j._links?.detail_en?.href || j._links?.detail_de?.href);

        return {
          id,
          source,
          title: (j.title || "").trim(),
          company: (j.company_name || "").trim(),
          location: j.place || "",
          description: cleanJobDescription(descriptions[k] || j.preview || ""),
          job_url: link || "",
          posted_date: j.publication_date || null,
          job_type: normalizeJobType({ title: j.title, description: descriptions[k] || j.preview || "", rawType: et ? (TYPE_MAP[et.value] || et.value) : null, raw: j }),
          employment_grade: eg ? `${eg.value_min}-${eg.value_max}%` : null,
          company_logo: j.company_logo_file || null,
          raw_data: j,
          is_active: true,
        };
      });

      const { error } = await supabase.from("tinjob_job_listings").upsert(rows, { onConflict: "id" });
      if (error) {
        // If unique index violation, insert one by one and skip dupes
        if (error.code === "23505") {
          for (const row of rows) {
            const { error: singleErr } = await supabase.from("tinjob_job_listings").upsert([row], { onConflict: "id" });
            if (singleErr) {
              console.log(`Skipped duplicate: "${row.title}" @ ${row.company}`);
              jobsSkippedDupe++;
            } else {
              jobsNew++;
            }
          }
        } else {
          console.error("Upsert error:", error.message);
        }
      } else {
        jobsNew += rows.length;
      }
    }

    // Mark existing jobs as still active (touch updated_at)
    // Include both ID-matched and title+company-matched existing jobs
    const existingJobIds = uniqueJobs.filter(([id]) => existingById.has(id)).map(([id]) => id);
    if (existingJobIds.length > 0) {
      for (let i = 0; i < existingJobIds.length; i += 100) {
        const batch = existingJobIds.slice(i, i + 100);
        await supabase.from("tinjob_job_listings").update({ updated_at: new Date().toISOString(), is_active: true }).in("id", batch);
      }
      jobsUpdated = existingJobIds.length;
    }

    // Also touch existing jobs matched by title+company (different ID but same job)
    // We need to keep them active since they're still on the portals
    if (jobsSkippedDupe > 0) {
      const titleCompanyPairs = newByIdJobs
        .filter(([, { raw }]) => {
          const key = dedupeKey((raw.title || "").trim(), (raw.company_name || "").trim());
          return existingByTitleCompany.has(key);
        })
        .map(([, { raw }]) => (raw.title || "").trim());

      const uniqueMatchedTitles = [...new Set(titleCompanyPairs)];
      for (let i = 0; i < uniqueMatchedTitles.length; i += 50) {
        const batch = uniqueMatchedTitles.slice(i, i + 50);
        await supabase
          .from("tinjob_job_listings")
          .update({ updated_at: new Date().toISOString(), is_active: true })
          .in("title", batch)
          .eq("is_active", true);
      }
      jobsUpdated += jobsSkippedDupe;
    }

    // Mark jobs not seen in this ingestion as inactive (if older than 7 days)
    await supabase.from("tinjob_job_listings")
      .update({ is_active: false })
      .lt("updated_at", new Date(Date.now() - 7 * 86400000).toISOString())
      .eq("is_active", true);

    // NOTE: ATS sources (greenhouse/ashby/personio/smartrecruiters) are NOT
    // scraped here anymore. They run as chunked sweeps via the ingest-ats
    // cron ({"phase":"ats"}) — one invocation could not fit ~500 board
    // scrapes within the Edge Function compute budget, which killed the run
    // before the log close-out (ingestion_log rows stuck in 'running').

    // Advance the portal sweep cursor; wrap at the end for a continuous re-sweep.
    const nextOffset = (offset + chunk.length) >= totalPages ? 0 : offset + chunk.length;
    await supabase.from("tinjob_ingest_state").upsert({
      key: "portal_cursor",
      value: {
        offset: nextOffset,
        total: totalPages,
        location,
        last_run: new Date().toISOString(),
        last_pages: chunk.length,
        last_new: jobsNew,
        sweep_completed: nextOffset === 0,
      },
      updated_at: new Date().toISOString(),
    }, { onConflict: "key" });

    // Enrich cantons for the jobs just ingested. map_job_cantons() maps
    // location -> canton (e.g. "Genève" -> "GE"). The public /offres-emploi
    // listing (JobListings.tsx) filters on canton — its default "Toute la
    // Suisse" view is `canton IS NOT NULL` — so any job left with a NULL
    // canton is invisible on the site. The function is on no cron, so run it
    // every ingestion (idempotent; only fills NULL cantons on active jobs).
    await supabase.rpc("map_tinjob_job_cantons").then(() => {}, (e: unknown) => console.error("map_tinjob_job_cantons failed:", e));

    // Update log
    if (logId) {
      await supabase.from("tinjob_ingestion_log").update({
        completed_at: new Date().toISOString(),
        jobs_found: uniqueJobs.length,
        jobs_new: jobsNew,
        jobs_updated: jobsUpdated,
        status: "completed",
      }).eq("id", logId);
    }

    return new Response(JSON.stringify({
      success: true,
      jobs_found_from_apis: jobMapById.size,
      jobs_after_cross_platform_dedup: uniqueJobs.length,
      jobs_new: jobsNew,
      jobs_updated: jobsUpdated,
      jobs_skipped_duplicate: jobsSkippedDupe,
      jobs_already_existed_by_id: existingById.size,
      portal_sweep: { from_offset: offset, next_offset: nextOffset, total_pages: totalPages, pages_this_run: chunk.length, location },
      elapsed_ms: Date.now() - startedAt,
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (err) {
    console.error("ingest-jobs error:", err);
    // Close out the log on failure too — a 'running' row left behind gets
    // reaped by the next cron and hides the real error.
    if (logId) {
      await supabase.from("tinjob_ingestion_log").update({
        completed_at: new Date().toISOString(),
        status: "failed",
        error: String(err),
      }).eq("id", logId).then(() => {}, (e: unknown) => console.error("log close-out failed:", e));
    }
    return new Response(JSON.stringify({ error: "Ingestion failed", detail: String(err) }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
