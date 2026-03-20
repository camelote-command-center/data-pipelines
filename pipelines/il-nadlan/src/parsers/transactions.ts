import type { NadlanDeal } from '../api/nadlan-client.js';

export interface TransactionRecord {
  source_id: string;
  city: string | null;
  city_he: string | null;
  neighborhood: string | null;
  neighborhood_he: string | null;
  street: string | null;
  street_he: string | null;
  house_number: string | null;
  block_gush: number | null;
  plot_chelka: number | null;
  sub_plot: number | null;
  sale_date: string | null;
  sale_price_nis: number | null;
  transaction_type: string | null;
  property_type: string | null;
  property_type_he: string | null;
  rooms: number | null;
  floor: number | null;
  total_floors: number | null;
  area_sqm: number | null;
  built_area_sqm: number | null;
  garden_area_sqm: number | null;
  year_built: number | null;
  is_new_construction: boolean | null;
  raw_data: Record<string, any>;
  country: string;
  admin_level_1: string;
}

function toNumber(val: any): number | null {
  if (val === null || val === undefined || val === '' || val === 'לא ידוע') return null;
  const num = Number(val);
  return isNaN(num) ? null : num;
}

function toInt(val: any): number | null {
  const num = toNumber(val);
  return num !== null ? Math.round(num) : null;
}

function toStr(val: any): string | null {
  if (val === null || val === undefined || val === '' || val === 'לא ידוע') return null;
  return String(val).trim() || null;
}

/**
 * Parse a Hebrew date string from the API.
 * Formats seen: "15/01/2024", "2024-01-15T00:00:00", etc.
 */
function parseDate(val: any): string | null {
  if (!val) return null;
  const s = String(val).trim();

  // ISO format
  if (s.match(/^\d{4}-\d{2}-\d{2}/)) {
    return s.substring(0, 10);
  }

  // DD/MM/YYYY
  const parts = s.split('/');
  if (parts.length === 3) {
    return `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
  }

  return null;
}

/**
 * Parse gush/chelka from parcelNum like "12345-678" or from separate fields.
 */
function parseParcel(deal: NadlanDeal): { gush: number | null; chelka: number | null; subPlot: number | null } {
  // Try direct fields first
  let gush = toInt(deal.gush || deal.block || deal.GUSH);
  let chelka = toInt(deal.helka || deal.chelka || deal.plot || deal.HELKA);
  let subPlot = toInt(deal.tatHelka || deal.subPlot || deal.TAT_HELKA);

  // Parse from parcelNum format "gush-chelka" or "gush-chelka-tatHelka"
  if ((!gush || !chelka) && deal.parcelNum) {
    const parts = String(deal.parcelNum).split('-');
    if (parts.length >= 2) {
      gush = gush || toInt(parts[0]);
      chelka = chelka || toInt(parts[1]);
      if (parts.length >= 3) subPlot = subPlot || toInt(parts[2]);
    }
  }

  return { gush, chelka, subPlot };
}

/**
 * Determine the admin_level_1 region from settlement data.
 * We use the district/region if available, otherwise default to city name.
 */
function getAdminLevel1(deal: NadlanDeal): string {
  return toStr(deal.district || deal.region || deal.mapiArea || deal.settlementName || deal.city) || 'unknown';
}

/**
 * Map nadlan.gov.il deal nature codes to English transaction types.
 */
function mapDealNature(nature: string | null): string | null {
  if (!nature) return null;
  const map: Record<string, string> = {
    דירה: 'apartment',
    'פנטהאוז': 'penthouse',
    'בית פרטי': 'private_house',
    'דו משפחתי': 'duplex',
    'מגרש': 'land',
    'מסחר': 'commercial',
    'תעשיה': 'industrial',
    'משרד': 'office',
    'חנות': 'shop',
    'מחסן': 'storage',
    'חניה': 'parking',
  };
  return map[nature] || nature;
}

/**
 * Transform a raw deal from the nadlan.gov.il API into our DB schema.
 */
export function transformDeal(deal: NadlanDeal): TransactionRecord {
  const { gush, chelka, subPlot } = parseParcel(deal);

  // Build a unique source_id from available identifiers
  const sourceId =
    deal.dealId ||
    deal.OBJECTID ||
    deal.objectId ||
    `${gush || ''}-${chelka || ''}-${deal.dealDatetime || deal.dealDate || ''}-${deal.dealAmount || ''}`;

  const isNew =
    deal.newProjectText === 'חדש' ||
    deal.newProjectText === 'יד ראשונה' ||
    deal.hokHamecher === 'first_hand' ||
    deal.isNewProject === true;

  return {
    source_id: String(sourceId),
    city: null, // English city name — not available from API
    city_he: toStr(deal.settlementName || deal.city || deal.setlName),
    neighborhood: null,
    neighborhood_he: toStr(deal.neighborhoodName || deal.neighborhood),
    street: null,
    street_he: toStr(deal.streetName || deal.street),
    house_number: toStr(deal.houseNum || deal.houseNumber || deal.buildingNum),
    block_gush: gush,
    plot_chelka: chelka,
    sub_plot: subPlot,
    sale_date: parseDate(deal.dealDatetime || deal.dealDate),
    sale_price_nis: toNumber(deal.dealAmount || deal.dealPrice || deal.price),
    transaction_type: toStr(deal.hokHamecher || deal.dealType),
    property_type: mapDealNature(toStr(deal.dealNature || deal.assetType)),
    property_type_he: toStr(deal.dealNature || deal.assetType),
    rooms: toNumber(deal.roomNum || deal.rooms),
    floor: toInt(deal.floor),
    total_floors: toInt(deal.buildingFloors || deal.totalFloors),
    area_sqm: toNumber(deal.area || deal.areaSqm),
    built_area_sqm: toNumber(deal.builtArea || deal.areaBruto),
    garden_area_sqm: toNumber(deal.gardenArea),
    year_built: toInt(deal.yearBuilt),
    is_new_construction: isNew || null,
    raw_data: deal,
    country: 'il',
    admin_level_1: getAdminLevel1(deal),
  };
}
