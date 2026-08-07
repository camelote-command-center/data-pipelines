\set ON_ERROR_STOP on
INSERT INTO platform.standards (category, rule_key, rule_text, correct_call, incorrect_call, applies_to, severity)
VALUES (
 'data_exposure',
 'tree_cadastre_legal_flags_ge',
 'Never present a Geneva tree-protection status the data does not support, and never let an unmeasured tree read as an unprotected one.

WHAT THE LAW ACTUALLY SAYS. The governing text is the RCVA -- Règlement sur la conservation de la végétation arborée, rsGE L 4 05.04 (NOT the RPMNS; the RCVA is made under art. 36 LPMNS of 4 June 1976). Its only size threshold is art. 3 al. 2: felling by the owner is exempt from authorisation for trees UNDER 45 cm circumference measured at 1 m. There is no 200 cm threshold anywhere in it, and no legal category called "arbre majeur". A 200 cm figure circulates informally; it has no basis in the regulation and must not be shipped as one.

1. THE FLAG IS AN EXEMPTION, NOT A PROTECTION. At or over 45 cm circumference authorisation is required; under it the owner may fell. Name the column for what it is (requires_felling_authorisation), never for a legal category that does not exist.

2. THE FLAG IS TRI-STATE AND MUST STAY SO. 103873 of 239167 inventoried trees carry no circumference at all (NULL, or a literal 0). NULL means unmeasured, never small. Collapsing it to false tells a promoteur he may fell a tree nobody has measured. Report authorisation_unknown separately from below_threshold in every summary.

3. FALSE IS NOT PERMISSION. Art. 3 al. 2 keeps authorisation mandatory regardless of size for trees designated by departmental directive, vegetation marked "à sauvegarder" in a PLQ (art. 8), compensation vegetation (art. 17), and plantings financed by the art. 18A fund. None of those appear in the SITG layers, so the flag cannot see them.

4. REMARKABILITY IS CARRIED, NEVER DERIVED. It is a scored designation -- OCAN with the CJB and HEPIA weight six criteria and require at least 12 of 20 points -- so it cannot be computed from size. And presence in FFP_ARBRES_REMARQUABLES does NOT mean remarkable: that layer is the assessment register and holds 221 pending and 165 rejected candidates alongside 208 approved. Filter on etat = ''Approuvé remarquable''.

5. CARRY POSITIONAL PRECISION THROUGH. STATUT has four live values; SITG documents an accuracy for only two (Historique about 25 m, Relevé about 1 m). Positionné and Nommé get NULL, not an invented figure. A 25 m point near a parcel boundary can be attributed to the wrong parcel, and the UI must be able to say so.

6. THE FELLING-AUTHORISATION LAYER IS LICENCE-BLOCKED. SIPV_ICA_ABATTAGE_SEV_PTS is "Accès libre, usage privé / A-star non commercial" AND "Uniquement pour l affichage dans la carte interactive dédiée". Both clauses independently exclude a commercial product re-displaying it, including internal or LBI use. Do not ingest or expose it without a written licence variation from the Ville de Genève.',
 'requires_felling_authorisation = CASE WHEN nullif(circonference_1m,0) IS NULL THEN NULL ELSE circonference_1m >= 45 END, with authorisation_unknown surfaced separately in the summary; is_remarquable = (etat = ''Approuvé remarquable'').',
 'is_arbre_majeur = (circonference_1m >= 200) OR present_in_remarquables -- invents a legal category, uses a threshold with no basis in the RCVA, counts 386 pending or rejected candidates as remarkable, and silently turns 103873 unmeasured trees into "not protected".',
 '{lamap_db,re-llm}',
 'critical')
RETURNING id, rule_key;
