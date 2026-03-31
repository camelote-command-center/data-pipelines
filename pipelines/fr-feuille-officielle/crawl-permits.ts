/**
 * FR Permit Crawler — Outputs JSON to stdout (no Supabase dependency)
 * Crawls fo.fr.ch category 21 pages and parses 3-column permit tables.
 */
import * as cheerio from 'cheerio';
import proj4 from 'proj4';

const BASE_URL = 'https://fo.fr.ch';
const RATE_LIMIT_MS = 500;

proj4.defs('EPSG:2056', '+proj=somerc +lat_0=46.9524055555556 +lon_0=7.43958333333333 +k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel +towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs +type=crs');

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchPage(url: string): Promise<string | null> {
  try {
    const res = await fetch(url);
    if (!res.ok) { if (res.status !== 404) console.error(`  HTTP ${res.status} ${url}`); return null; }
    return await res.text();
  } catch (err) { console.error(`  Fetch error: ${url} ${err}`); return null; }
}

function extractIssueNumbers(html: string, year: number): number[] {
  const $ = cheerio.load(html);
  const issues: number[] = [];
  const seen = new Set<number>();
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (!href) return;
    const match = href.match(new RegExp(`/archive/${year}/(\\d+)(?:\\D|$)`));
    if (match) { const n = parseInt(match[1], 10); if (!seen.has(n)) { seen.add(n); issues.push(n); } }
  });
  return issues.sort((a, b) => a - b);
}

function extractNodeUrls(html: string): string[] {
  const $ = cheerio.load(html);
  const urls: string[] = [];
  const seen = new Set<string>();
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (!href) return;
    const match = href.match(/\/node\/(\d+)/);
    if (match) { const url = `${BASE_URL}/node/${match[1]}`; if (!seen.has(url)) { seen.add(url); urls.push(url); } }
  });
  return urls;
}

function convertLV95(east: number, north: number): { lon: number; lat: number } | null {
  try {
    const [lon, lat] = proj4('EPSG:2056', 'EPSG:4326', [east, north]);
    if (lon >= 5.5 && lon <= 11 && lat >= 45.5 && lat <= 48) {
      return { lon: Math.round(lon * 1e7) / 1e7, lat: Math.round(lat * 1e7) / 1e7 };
    }
    return null;
  } catch { return null; }
}

function parsePermitPage(html: string, nodeUrl: string, year: number, issue: number): any[] {
  const $ = cheerio.load(html);
  const permits: any[] = [];
  let table = $('div.body table').first();
  if (!table.length) table = $('article table').first();
  if (!table.length) table = $('table').first();
  if (!table.length) return permits;

  table.find('tr').each((_, tr) => {
    const tds = $(tr).find('> td');
    if (tds.length < 3) return;
    const col2Text = $(tds[1]).text().trim();
    const col3Text = $(tds[2]).text().trim();
    if (!col2Text && !col3Text) return;

    // Column 1
    const col1Lines: string[] = [];
    $(tds[0]).find('p').each((_, p) => { const t = $(p).text().trim(); if (t) col1Lines.push(t); });
    if (col1Lines.length === 0) {
      $(tds[0]).text().trim().split(/\n/).forEach(l => { const t = l.trim(); if (t) col1Lines.push(t); });
    }

    const coordPattern = /^(\d{7}\.?\d*)\/(\d{7}\.?\d*)$/;
    let geometry: string | null = null;
    let coordLineIdx = -1;
    for (let i = 0; i < col1Lines.length; i++) {
      const m = col1Lines[i].match(coordPattern);
      if (m) {
        coordLineIdx = i;
        const wgs = convertLV95(parseFloat(m[1]), parseFloat(m[2]));
        if (wgs) geometry = `SRID=4326;POINT(${wgs.lon} ${wgs.lat})`;
        break;
      }
    }

    const nonCoordLines = col1Lines.filter((_, i) => i !== coordLineIdx);
    const addrPat = /\d|rue |route |chemin |impasse |avenue |boulevard |place |passage |allée |sur-les-|im |weg|strasse|gasse/i;
    let commune = '';
    const addressParts: string[] = [];
    for (const line of nonCoordLines) {
      if (!commune) { commune = line; }
      else if (!addrPat.test(line) && addressParts.length === 0) { commune += ' – ' + line; }
      else { addressParts.push(line); }
    }

    // Column 2
    const col2Lines: string[] = [];
    $(tds[1]).find('p').each((_, p) => { const t = $(p).text().trim(); if (t) col2Lines.push(t); });
    const architect = col2Lines.length > 0 ? col2Lines.join(', ') : null;

    // Column 3
    const col3P: string[] = [];
    $(tds[2]).find('p').each((_, p) => { const t = $(p).text().trim(); if (t) col3P.push(t); });

    let applicant: string | null = col3P.length >= 1 ? col3P[0] : null;
    const lastP = col3P[col3P.length - 1] || '';
    const parcelMatch = lastP.match(/Art\.\s*([\d\s,]+)\s*RF/);
    const parcelNumber = parcelMatch ? parcelMatch[1].replace(/\s+/g, ' ').trim() : null;
    const friacMatch = lastP.match(/Ref\.\s*FRIAC:\s*(\d+-\d+-\d+-[A-Z])/);
    const friacRef = friacMatch ? friacMatch[1] : null;

    let description: string | null = null;
    if (col3P.length >= 3) {
      description = col3P.slice(1, -1).join(' ').trim() || null;
    } else if (col3P.length === 2) {
      const artIdx = col3P[1].search(/Art\.\s*[\d]/);
      description = artIdx > 0 ? col3P[1].substring(0, artIdx).trim() : col3P[1];
    }
    if (description) {
      description = description.replace(/\s*Sans appel d[''']offres\s*$/i, '').replace(/\.\s*$/, '').trim() || null;
    }

    const sourceId = friacRef
      ? `FR-FRIAC-${friacRef}`
      : `FR-${year}-${issue}-${nodeUrl.match(/\/node\/(\d+)/)?.[1]}-${permits.length}`;

    permits.push({
      source_id: sourceId,
      canton: 'FR',
      permit_type: null,
      status: null,
      description,
      applicant,
      owner: null,
      commune,
      address: addressParts.length > 0 ? addressParts.join(', ') : null,
      parcel_number: parcelNumber,
      zone: null,
      submission_date: null,
      publication_date: null,
      decision_date: null,
      display_start: null,
      display_end: null,
      geometry,
      source_url: nodeUrl,
      source_system: 'fo_fr_ch',
      raw_data: { year, issue, architect, col1_lines: col1Lines, col2_lines: col2Lines, col3_paragraphs: col3P },
    });
  });

  return permits;
}

async function main() {
  const args = process.argv.slice(2);
  let years: number[];
  if (args.length > 0) {
    years = args.map(a => parseInt(a, 10)).filter(n => !isNaN(n));
  } else {
    years = [new Date().getFullYear()];
  }

  console.error(`Crawling years: ${years.join(', ')}`);
  const allPermits: any[] = [];

  for (const year of years) {
    console.error(`\n--- Year ${year} ---`);
    const archiveHtml = await fetchPage(`${BASE_URL}/archive/${year}`);
    await sleep(RATE_LIMIT_MS);
    if (!archiveHtml) continue;

    let issues = extractIssueNumbers(archiveHtml, year);
    if (issues.length === 0) {
      console.error(`  No issue links — trying 1-52`);
      issues = Array.from({ length: 52 }, (_, i) => i + 1);
    }
    console.error(`  ${issues.length} issues to check`);

    for (const issue of issues) {
      const catHtml = await fetchPage(`${BASE_URL}/archive/${year}/${issue}/21`);
      await sleep(RATE_LIMIT_MS);
      if (!catHtml) continue;

      const nodeUrls = extractNodeUrls(catHtml);
      if (nodeUrls.length === 0) continue;
      console.error(`  Issue ${issue}: ${nodeUrls.length} articles`);

      for (const nodeUrl of nodeUrls) {
        const articleHtml = await fetchPage(nodeUrl);
        await sleep(RATE_LIMIT_MS);
        if (!articleHtml) continue;

        const permits = parsePermitPage(articleHtml, nodeUrl, year, issue);
        allPermits.push(...permits);
        console.error(`    ${nodeUrl.match(/\/node\/(\d+)/)?.[1]}: ${permits.length} permits`);
      }
    }
  }

  // Deduplicate
  const seen = new Map<string, any>();
  for (const p of allPermits) seen.set(p.source_id, p);
  const deduped = Array.from(seen.values());
  
  console.error(`\nTotal: ${allPermits.length} parsed, ${deduped.length} unique`);
  console.log(JSON.stringify(deduped));
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
