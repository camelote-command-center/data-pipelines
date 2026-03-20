import { parse } from 'csv-parse/sync';

export interface DVFRecord {
  id_mutation: string;
  date_mutation: string; // ISO date
  nature_mutation: string | null;
  valeur_fonciere: number | null;
  adresse_numero: string | null;
  adresse_suffixe: string | null;
  adresse_nom_voie: string | null;
  adresse_code_voie: string | null;
  code_postal: string | null;
  code_commune: string | null;
  nom_commune: string | null;
  code_departement: string | null;
  ancien_code_commune: string | null;
  ancien_nom_commune: string | null;
  id_parcelle: string | null;
  ancien_id_parcelle: string | null;
  numero_volume: string | null;
  lot1_numero: string | null;
  lot1_surface_carrez: number | null;
  lot2_numero: string | null;
  lot2_surface_carrez: number | null;
  lot3_numero: string | null;
  lot3_surface_carrez: number | null;
  lot4_numero: string | null;
  lot4_surface_carrez: number | null;
  lot5_numero: string | null;
  lot5_surface_carrez: number | null;
  nombre_lots: number | null;
  code_type_local: string | null;
  type_local: string | null;
  surface_reelle_bati: number | null;
  nombre_pieces_principales: number | null;
  code_nature_culture: string | null;
  nature_culture: string | null;
  code_nature_culture_speciale: string | null;
  nature_culture_speciale: string | null;
  surface_terrain: number | null;
  longitude: number | null;
  latitude: number | null;
  country: string;
  admin_level_1: string;
}

function emptyToNull(val: string | undefined): string | null {
  if (val === undefined || val === null) return null;
  const trimmed = val.trim();
  return trimmed === '' ? null : trimmed;
}

/** Parse French decimal (comma → dot) and return number or null */
function parseFrenchDecimal(val: string | undefined): number | null {
  if (!val) return null;
  const trimmed = val.trim();
  if (trimmed === '') return null;
  const normalized = trimmed.replace(',', '.');
  const num = parseFloat(normalized);
  return isNaN(num) ? null : num;
}

function parseInteger(val: string | undefined): number | null {
  if (!val) return null;
  const trimmed = val.trim();
  if (trimmed === '') return null;
  const num = parseInt(trimmed, 10);
  return isNaN(num) ? null : num;
}

/** Parse DD/MM/YYYY → YYYY-MM-DD */
function parseFrenchDate(val: string): string {
  const trimmed = val.trim();
  // Try DD/MM/YYYY
  const parts = trimmed.split('/');
  if (parts.length === 3) {
    return `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
  }
  // Already ISO?
  return trimmed;
}

export function parseCSV(csvText: string): DVFRecord[] {
  const rows: Record<string, string>[] = parse(csvText, {
    delimiter: '|',
    columns: true, // use header row
    skip_empty_lines: true,
    relax_column_count: true,
  });

  const records: DVFRecord[] = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    try {
      const idMutation = emptyToNull(row['id_mutation']);
      if (!idMutation) continue; // skip rows without mutation ID

      records.push({
        id_mutation: idMutation,
        date_mutation: parseFrenchDate(row['date_mutation']),
        nature_mutation: emptyToNull(row['nature_mutation']),
        valeur_fonciere: parseFrenchDecimal(row['valeur_fonciere']),
        adresse_numero: emptyToNull(row['adresse_numero']),
        adresse_suffixe: emptyToNull(row['adresse_suffixe']),
        adresse_nom_voie: emptyToNull(row['adresse_nom_voie']),
        adresse_code_voie: emptyToNull(row['adresse_code_voie']),
        code_postal: emptyToNull(row['code_postal']),
        code_commune: emptyToNull(row['code_commune']),
        nom_commune: emptyToNull(row['nom_commune']),
        code_departement: emptyToNull(row['code_departement']),
        ancien_code_commune: emptyToNull(row['ancien_code_commune']),
        ancien_nom_commune: emptyToNull(row['ancien_nom_commune']),
        id_parcelle: emptyToNull(row['id_parcelle']),
        ancien_id_parcelle: emptyToNull(row['ancien_id_parcelle']),
        numero_volume: emptyToNull(row['numero_volume']),
        lot1_numero: emptyToNull(row['lot1_numero']),
        lot1_surface_carrez: parseFrenchDecimal(row['lot1_surface_carrez']),
        lot2_numero: emptyToNull(row['lot2_numero']),
        lot2_surface_carrez: parseFrenchDecimal(row['lot2_surface_carrez']),
        lot3_numero: emptyToNull(row['lot3_numero']),
        lot3_surface_carrez: parseFrenchDecimal(row['lot3_surface_carrez']),
        lot4_numero: emptyToNull(row['lot4_numero']),
        lot4_surface_carrez: parseFrenchDecimal(row['lot4_surface_carrez']),
        lot5_numero: emptyToNull(row['lot5_numero']),
        lot5_surface_carrez: parseFrenchDecimal(row['lot5_surface_carrez']),
        nombre_lots: parseInteger(row['nombre_lots']),
        code_type_local: emptyToNull(row['code_type_local']),
        type_local: emptyToNull(row['type_local']),
        surface_reelle_bati: parseFrenchDecimal(row['surface_reelle_bati']),
        nombre_pieces_principales: parseInteger(row['nombre_pieces_principales']),
        code_nature_culture: emptyToNull(row['code_nature_culture']),
        nature_culture: emptyToNull(row['nature_culture']),
        code_nature_culture_speciale: emptyToNull(row['code_nature_culture_speciale']),
        nature_culture_speciale: emptyToNull(row['nature_culture_speciale']),
        surface_terrain: parseFrenchDecimal(row['surface_terrain']),
        longitude: parseFrenchDecimal(row['longitude']),
        latitude: parseFrenchDecimal(row['latitude']),
        country: 'fr',
        admin_level_1: emptyToNull(row['code_departement']) ?? '',
      });
    } catch (err: any) {
      console.warn(`[Parse] Row ${i + 1} failed: ${err.message}`);
    }
  }

  console.log(`[Parse] Parsed ${records.length} records from CSV`);
  return records;
}
