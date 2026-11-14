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
 * A Geneva PPE lot is written with the word PPE (« PPE Meyrin, 33/15242-118 sur 49/1000 ») or with a
 * PPE unit number (« 4.06 appartement », « 1.43 box », older notices « UE 2.05 appartement »). Not evidence:
 * a lot suffix or a share in thousandths alone — ordinary co-ownership uses both (« COP Meyrin,
 * 33/14346-91 sur 3/1000 du B-F », « cop. 283/1000 ») — and the model's per-mille fields, into which it
 * copies COP shares (« 1/2 »). COP is never PPE. Same rule as gold_ch.core_transactions on re-LLM
 * (2026-11-14), which also uses the PPE plot register when the text does not sell the whole plot.
 */

const PPE_WORD = /\bPPE\b/i;
const UNITS = 'appartement|studio|attique|duplex|triplex|loft|box|parking|place|garage|cave|grenier|bureaux?|local|locaux|arcade|dépôt|depot|chambre|hall|magasin|atelier|cabinet|salle|restaurant|réduit';
// « UE 2.05 » (older notices) or a unit number before its use: « 4.06 appartement », « 1.20 partie appartement ».
const PPE_UNIT = new RegExp(`\\bUE\\s*\\d|(?<![\\d.])\\d{1,2}\\.\\d{2,3}\\s+(?:partie\\s+)?(?:${UNITS})(?![\\wàâäéèêëîïôöùûüç])`, 'i');

/** True when the FAO notice itself shows a PPE lot. */
export function hasPpeEvidence(text: string | null | undefined): boolean {
  const t = text ?? '';
  return PPE_WORD.test(t) || PPE_UNIT.test(t);
}

/**
 * Final PPE fields for a record: « Yes » only with evidence in the text, « No » otherwise; the
 * cumulative per-mille is kept only when the text shows a PPE lot.
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
