/**
 * Shared phone-number normalisation.
 *
 * Every parser must run normalizePhone() before upserting to contacts_leads
 * so the UNIQUE constraint on phone_number is respected consistently.
 *
 * Output format: digits only, Swiss numbers start with "41" (no leading +).
 * Returns null for junk data (< 10 digits after cleaning).
 */

export function normalizePhone(raw: string | null | undefined): string | null {
  if (!raw) return null;

  let cleaned = String(raw);

  // 1. Strip trailing ".0" (float-to-string CSV artifact)
  cleaned = cleaned.replace(/\.0+$/, '');

  // 2. Remove every non-digit character except a leading +
  //    Keep the + temporarily so we can detect international prefix
  cleaned = cleaned.replace(/(?!^\+)[^\d]/g, '');

  // 3. Strip + prefix (our canonical form is digits-only)
  cleaned = cleaned.replace(/^\+/, '');

  // 4. Swiss normalisation
  //    0041... → 41...
  if (cleaned.startsWith('0041')) {
    cleaned = '41' + cleaned.slice(4);
  }
  //    041... with enough digits → 41...
  else if (cleaned.startsWith('041') && cleaned.length > 10) {
    cleaned = '41' + cleaned.slice(3);
  }
  //    07x... (10-digit local Swiss) → 417x...
  else if (cleaned.startsWith('0') && cleaned.length === 10) {
    cleaned = '41' + cleaned.slice(1);
  }

  // 5. Reject junk (< 10 digits)
  if (cleaned.length < 10) return null;

  return cleaned;
}
