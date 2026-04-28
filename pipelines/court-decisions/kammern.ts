// Curated list of RE-relevant court chambers in Switzerland (entscheidsuche keys).
//
// Selected from /docs/Facetten.json with focus on:
//   - Civil chambers handling property/rental disputes
//   - Specific rental tribunals (Tribunal des baux et loyers GE, Mietgericht ZH)
//   - Construction / planning / cadastre administrative chambers
//
// To extend: probe https://entscheidsuche.ch/docs/Facetten.json and append.

export interface Kammer {
  /** Stable hierarchy key like "GE_CJ_004". */
  key: string;
  canton: string;       // 'GE', 'VD', 'NE', 'JU', 'FR', 'VS', 'CH' (federal)
  court: string;        // e.g. 'GE_CJ' (Cour de justice)
  name_fr?: string;
  name_de?: string;
  /** Coarse tags for the documents this chamber produces. */
  tags: string[];
}

export const KAMMERN: Kammer[] = [
  // ── Federal civil divisions (real-estate-relevant) ─────────────────────────
  { key: 'CH_BGer_004', canton: 'CH', court: 'CH_BGer', name_fr: 'Ire Cour de droit civil', name_de: 'I. Zivilrechtliche Abteilung', tags: ['court', 'civil', 'federal'] },
  { key: 'CH_BGer_005', canton: 'CH', court: 'CH_BGer', name_fr: 'IIe Cour de droit civil', name_de: 'II. Zivilrechtliche Abteilung', tags: ['court', 'civil', 'federal', 'property'] },

  // ── Geneva ─────────────────────────────────────────────────────────────────
  { key: 'GE_CJ_001', canton: 'GE', court: 'GE_CJ', name_fr: 'Chambre civile', tags: ['court', 'civil', 'geneva'] },
  { key: 'GE_CJ_002', canton: 'GE', court: 'GE_CJ', name_fr: 'Chambre civile (Sommaires)', tags: ['court', 'civil', 'geneva', 'summary'] },
  { key: 'GE_CJ_004', canton: 'GE', court: 'GE_CJ', name_fr: 'Chambre des baux et loyers', tags: ['court', 'civil', 'geneva', 'rental', 'tenancy', 'real_estate'] },

  // ── Vaud ───────────────────────────────────────────────────────────────────
  { key: 'VD_TC_002', canton: 'VD', court: 'VD_TC', name_fr: "Cour d'appel civile", tags: ['court', 'civil', 'vaud'] },
  { key: 'VD_TC_007', canton: 'VD', court: 'VD_TC', name_fr: 'Cour civile', tags: ['court', 'civil', 'vaud'] },
  { key: 'VD_TC_010', canton: 'VD', court: 'VD_TC', name_fr: 'Chambre des recours civile', tags: ['court', 'civil', 'vaud'] },
  { key: 'VD_TC_032', canton: 'VD', court: 'VD_TC', name_fr: 'Chambre des recours civile', tags: ['court', 'civil', 'vaud'] },

  // ── Fribourg ───────────────────────────────────────────────────────────────
  { key: 'FR_TC_001', canton: 'FR', court: 'FR_TC', name_fr: "Cours d'appel civil", name_de: 'Zivilappellationshöfe', tags: ['court', 'civil', 'fribourg'] },

  // ── Neuchâtel ──────────────────────────────────────────────────────────────
  { key: 'NE_TC_001', canton: 'NE', court: 'NE_TC', name_fr: 'Cour Civile, Cour civile au sens strict', tags: ['court', 'civil', 'neuchatel'] },
  { key: 'NE_TC_002', canton: 'NE', court: 'NE_TC', name_fr: "Cour Civile, Cour d'appel", tags: ['court', 'civil', 'neuchatel'] },
  { key: 'NE_TC_004', canton: 'NE', court: 'NE_TC', name_fr: 'Cour Civile, Autorité de recours en matière civile', tags: ['court', 'civil', 'neuchatel'] },
  { key: 'NE_TC_005', canton: 'NE', court: 'NE_TC', name_fr: 'Cour de cassation civile', tags: ['court', 'civil', 'neuchatel'] },

  // ── Jura ───────────────────────────────────────────────────────────────────
  { key: 'JU_TC_002', canton: 'JU', court: 'JU_TC', name_fr: 'Cour civile', tags: ['court', 'civil', 'jura'] },
  { key: 'JU_TC_011', canton: 'JU', court: 'JU_TC', name_fr: 'Affaires civiles', tags: ['court', 'civil', 'jura'] },

  // ── Zurich (DE Switzerland — rental tribunal worth keeping for cross-cantonal rental case law) ──
  { key: 'ZH_BK_004', canton: 'ZH', court: 'ZH_BK', name_de: 'Mietgericht', tags: ['court', 'civil', 'zurich', 'rental', 'tenancy', 'real_estate'] },

  // ── Bern (Direction des travaux publics — building/zoning admin decisions) ──
  { key: 'BE_VB_001', canton: 'BE', court: 'BE_VB', name_fr: 'Direction des travaux publics et des transports', name_de: 'Bau- und Verkehrsdirektion', tags: ['court', 'admin', 'bern', 'planning', 'construction'] },
];

export const ENTSCHEIDSUCHE_ENDPOINT = 'https://entscheidsuche.ch/_searchV2.php';
