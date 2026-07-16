/**
 * Deterministic parser for amtsblattportal.ch succession content XML.
 *
 * Pure functions only — no network, no DB — so the fixture tests exercise the
 * exact logic the pipeline runs. Two content shapes are handled:
 *   - TE-<CC>*  cantonal succession notices → <content><testator>…
 *   - KK        federal Konkurs (repudiated estates) → <content><debtor><person>…
 * Both share the person sub-structure, so one extractor covers them.
 */

import { XMLParser } from 'fast-xml-parser';

const parser = new XMLParser({
  ignoreAttributes: true,
  trimValues: true,
  parseTagValue: false, // keep everything as strings; we coerce dates ourselves
});

export type EventType =
  | 'erbenruf'
  | 'ausschlagung'
  | 'liquidation'
  | 'dereliktion'
  | 'escheat'
  | 'testament'
  | 'inventar'
  | 'other';

export interface Notice {
  publicationId: string;
  tenant: string;
  canton: string;
  rubric: string | null;
  subRubric: string | null;
  eventType: EventType;
  publicationDate: string | null; // YYYY-MM-DD
  language: string | null;
  title: string | null;
  authority: string | null;
  authorityMunicipalityId: string | null;
  addition: string | null; // e.g. 'refusedLegacy'
  deceasedPrename: string | null;
  deceasedSurname: string | null;
  deceasedName: string | null;
  deceasedDob: string | null; // YYYY-MM-DD
  deceasedDod: string | null; // date of death OR detection
  deceasedHeimatort: string | null; // placeOfOrigin
  deceasedLastDomicile: string | null;
  deadlineDate: string | null; // entryDeadline
  bodyText: string | null;
  contentJson: Record<string, unknown>;
}

// --- helpers ----------------------------------------------------------------

function s(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  if (typeof v === 'object') return null;
  const t = String(v).trim();
  return t.length ? t : null;
}

/** ISO date passthrough; the API already emits YYYY-MM-DD. */
function isoDate(v: unknown): string | null {
  const t = s(v);
  if (!t) return null;
  const m = t.match(/^(\d{4})-(\d{2})-(\d{2})/);
  return m ? `${m[1]}-${m[2]}-${m[3]}` : null;
}

/** Strip HTML tags / decode the handful of entities the gazette uses. */
export function stripHtml(v: unknown): string | null {
  let t = s(v);
  if (!t) return null;
  t = t
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&nbsp;/g, ' ')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return t.length ? t : null;
}

/** Map (subRubric, addition) → normalized event type. Canton-agnostic on TE-<CC>. */
export function mapEventType(subRubric: string | null, addition: string | null): EventType {
  const sr = (subRubric ?? '').toUpperCase();
  const te = sr.match(/^TE-[A-Z]{2}(\d{2})$/);
  if (te) {
    switch (te[1]) {
      case '20': return 'erbenruf';
      case '10': return 'testament';
      case '60':
      case '70': return 'inventar';
      case '90': return 'escheat';
      default: return 'other';
    }
  }
  if (sr.startsWith('KK')) {
    if ((addition ?? '').toLowerCase() === 'refusedlegacy') return 'ausschlagung';
    return 'liquidation';
  }
  return 'other';
}

/** Assemble a one-line domicile from a Swiss/foreign address block. */
function assembleAddress(addr: Record<string, unknown> | undefined): string | null {
  if (!addr || typeof addr !== 'object') return null;
  const parts: string[] = [];
  const line1 = s(addr.addressLine1);
  const street = s(addr.street);
  const houseNumber = s(addr.houseNumber);
  const zip = s(addr.swissZipCode) ?? s(addr.postalCode);
  const town = s(addr.town) ?? s(addr.city);
  const country = s((addr.country as Record<string, unknown>)?.name) ?? null;
  if (line1) parts.push(line1);
  if (street) parts.push(houseNumber ? `${street} ${houseNumber}` : street);
  const zipTown = [zip, town].filter(Boolean).join(' ').trim();
  if (zipTown) parts.push(zipTown);
  if (country) parts.push(country);
  return parts.length ? parts.join(', ') : null;
}

/** First non-empty line of a free-text registrationOffice block. */
function firstLine(v: unknown): string | null {
  const t = s(v);
  if (!t) return null;
  const line = t.split('\n').map((x) => x.trim()).find((x) => x.length);
  return line ?? null;
}

// --- main -------------------------------------------------------------------

export interface ParseInput {
  publicationId: string;
  tenant: string;
  canton: string;
  xml: string;
}

export function parseNoticeXml(input: ParseInput): Notice {
  const doc = parser.parse(input.xml) as Record<string, unknown>;
  // The root element is namespaced by rubric (e.g. "TE-BE20:publication"); take
  // the single top-level object regardless of its tag name.
  const rootKey = Object.keys(doc).find((k) => k !== '?xml');
  const root = (rootKey ? doc[rootKey] : {}) as Record<string, unknown>;
  const meta = (root.meta ?? {}) as Record<string, unknown>;
  const content = (root.content ?? {}) as Record<string, unknown>;

  const subRubric = s(meta.subRubric);
  const rubric = s(meta.rubric);
  const language = s(meta.language);

  // person block: testator (TE) or debtor.person (KK)
  const debtor = (content.debtor ?? {}) as Record<string, unknown>;
  const person = (content.testator ?? debtor.person ?? {}) as Record<string, unknown>;

  const addition = s(content.addition);
  const eventType = mapEventType(subRubric, addition);

  const prename = s(person.prename);
  const surname = s(person.name);
  const fullName = [prename, surname].filter(Boolean).join(' ').trim() || null;

  // authority: prefer the clean meta displayName, fall back to content free-text
  const regOffice = (meta.registrationOffice ?? {}) as Record<string, unknown>;
  const authority = s(regOffice.displayName) ?? firstLine(content.registrationOffice);
  const authorityMunicipalityId =
    s(regOffice.municipalityId) ??
    s(((content.localization as Record<string, unknown>)?.municipalityId as Record<string, unknown>)?.key);

  // body text field name varies by rubric
  const bodyText = stripHtml(content.callInheritance ?? content.probate ?? content.arrangement);

  return {
    publicationId: input.publicationId,
    tenant: input.tenant,
    canton: input.canton,
    rubric,
    subRubric,
    eventType,
    publicationDate: isoDate(meta.publicationDate),
    language,
    title: s((meta.title as Record<string, unknown>)?.de) ?? s((meta.title as Record<string, unknown>)?.fr),
    authority,
    authorityMunicipalityId,
    addition,
    deceasedPrename: prename,
    deceasedSurname: surname,
    deceasedName: fullName,
    deceasedDob: isoDate(person.dateOfBirth),
    deceasedDod: isoDate(person.dateOfDeath) ?? isoDate(person.dateOfDetection),
    deceasedHeimatort: s(person.placeOfOrigin),
    deceasedLastDomicile: assembleAddress(person.addressSwitzerland as Record<string, unknown>) ??
      assembleAddress(person.addressForeign as Record<string, unknown>),
    deadlineDate: isoDate(content.entryDeadline),
    bodyText,
    contentJson: content as Record<string, unknown>,
  };
}

/** Normalize a name/authority token for the dedupe key. */
export function norm(v: string | null): string {
  return (v ?? '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '') // strip combining diacritics
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

/**
 * Estate-identity dedupe key.
 *
 * Keys on the DECEASED, not the notice, so an estate's many publications collapse
 * to one event: identity = name + dob + last_domicile. These three are ~100%
 * present on every KK subrubric (verified), so the KK01→06 lifecycle collapses
 * cleanly. `event_type` is included so a person's *distinct* legal proceedings
 * (e.g. an Erbenaufruf vs a later liquidation) stay distinct — it never splits the
 * within-category collapse because a category is constant across an estate's notices.
 *
 * heimatort is deliberately NOT a key component: it is present on only ~85% of
 * notices and inconsistently across an estate's lifecycle, so including it would
 * fragment estates. It is retained as a stored attribute.
 */
export function buildDedupeKey(n: Pick<Notice, 'canton' | 'eventType' | 'deceasedName' | 'deceasedDob' | 'deceasedLastDomicile'>): string {
  return [
    n.canton.toUpperCase(),
    n.eventType,
    norm(n.deceasedName),
    n.deceasedDob ?? '',
    norm(n.deceasedLastDomicile),
  ].join('|');
}

/** Repudiation scope for the actionable list (brief §2). */
export function repudiationScope(eventType: EventType, refusedLegacy: boolean): 'all_heirs_liquidation' | 'not_applicable' | 'unknown' {
  if (eventType === 'ausschlagung') return refusedLegacy ? 'all_heirs_liquidation' : 'unknown';
  if (eventType === 'liquidation') return 'unknown';
  return 'not_applicable';
}
