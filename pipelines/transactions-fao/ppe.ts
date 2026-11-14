/**
 * PPE (propriété par étages) flag for an FAO transaction, decided from the notice text — never from
 * the model's answer alone.
 *
 * Bug (found 2026-11-14): the extraction prompt showed `"PPE indicator": "Yes"` as a literal example
 * and no rule said when it applies, so the model answered « Yes » for plain houses, whole buildings,
 * land and ordinary co-ownership. 598 of 3,087 « Yes » rows had no PPE in their text; re-LLM turned
 * them into `ppe_indicator = true` and `property_type = 'apartment'` all the way down to LaMap
 * (e.g. 2026/10757/0, a single-family house in Cologny, shown as « en PPE »).
 *
 * A Geneva PPE lot is always written with the word PPE (« PPE Meyrin, 33/15242-118 sur 49/1000 ») or
 * with its share in thousandths (« B-F Chêne-Bourg, 13/4717-118 sur 37.1/1000 »). A lot suffix alone is
 * NOT evidence: « COP Hermance, 28/1881-2 sur 1/2 du B-F » is ordinary co-ownership of a plot. The model's
 * per-mille fields are not evidence either: it copies COP shares (« 1/2 ») into them.
 */

const PPE_WORD = /\bPPE\b/i;
const THOUSANDTHS = /\/\s*1['’.]?000(?![\d])/;
const MILLIEME = /milli[eè]me/i;

/** True when the FAO notice itself shows a PPE lot. */
export function hasPpeEvidence(text: string | null | undefined): boolean {
  const t = text ?? '';
  return PPE_WORD.test(t) || THOUSANDTHS.test(t) || MILLIEME.test(t);
}

/**
 * Final PPE fields for a record: « Yes » only with evidence in the text, « No » otherwise; the
 * cumulative per-mille is kept only when the text shows a PPE share.
 */
export function decidePpe(
  text: string | null | undefined,
  modelIndicator: unknown,
  modelPourmille: unknown,
): { ppe_indicator: 'Yes' | 'No'; cumulative_pourmille_ppe: string | null; overridden: boolean } {
  const evidence = hasPpeEvidence(text);
  const saidYes = String(modelIndicator ?? '').trim().toLowerCase() === 'yes';
  const pourmille = String(modelPourmille ?? '').trim();
  return {
    ppe_indicator: evidence ? 'Yes' : 'No',
    cumulative_pourmille_ppe: evidence && pourmille ? pourmille : null,
    overridden: saidYes !== evidence,
  };
}
