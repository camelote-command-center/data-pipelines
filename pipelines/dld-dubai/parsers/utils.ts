/**
 * Parse date strings in DD-MM-YYYY, DD/MM/YYYY, or ISO 8601 formats.
 * Returns ISO date string or null.
 */
export function parseDate(value: any): string | null {
  if (!value || (typeof value === 'string' && value.trim() === '')) return null;

  const str = String(value).trim();

  // DD-MM-YYYY or DD/MM/YYYY
  const ddmmyyyy = str.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
  if (ddmmyyyy) {
    const [, day, month, year] = ddmmyyyy;
    const d = new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T00:00:00Z`);
    if (!isNaN(d.getTime())) return d.toISOString().split('T')[0];
  }

  // ISO 8601 or YYYY-MM-DD
  const d = new Date(str);
  if (!isNaN(d.getTime())) return d.toISOString().split('T')[0];

  return null;
}

/**
 * Parse boolean from Yes/No strings.
 */
export function parseBool(value: any): boolean | null {
  if (value === null || value === undefined || value === '') return null;
  const str = String(value).trim().toLowerCase();
  if (str === 'yes' || str === 'true' || str === '1') return true;
  if (str === 'no' || str === 'false' || str === '0') return false;
  return null;
}

/**
 * Parse numeric value, stripping commas.
 */
export function parseNumeric(value: any): number | null {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return value;
  const cleaned = String(value).replace(/,/g, '').trim();
  if (cleaned === '') return null;
  const num = Number(cleaned);
  return isNaN(num) ? null : num;
}

/**
 * Parse integer value.
 */
export function parseInt_(value: any): number | null {
  const num = parseNumeric(value);
  if (num === null) return null;
  return Math.round(num);
}

/**
 * Clean text: trim whitespace, empty strings → null.
 */
export function cleanText(value: any): string | null {
  if (value === null || value === undefined) return null;
  const str = String(value).trim();
  return str === '' ? null : str;
}

/**
 * Get a value from an object by trying multiple possible key names.
 * API field names may vary slightly.
 */
export function getField(row: Record<string, any>, ...keys: string[]): any {
  for (const key of keys) {
    if (key in row) return row[key];
  }
  // Try case-insensitive match
  const lowerKeys = keys.map((k) => k.toLowerCase());
  for (const [k, v] of Object.entries(row)) {
    if (lowerKeys.includes(k.toLowerCase())) return v;
  }
  return undefined;
}
