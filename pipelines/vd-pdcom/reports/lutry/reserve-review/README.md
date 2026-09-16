# Lutry: approval exceptions, missing annexes and full raw OCR

**The dossier's general approval date does not apply unconditionally to all mapped sectors.** Part 2 PDF page 9 (printed 50) records a Council of State decision dated 28 February 2000 suspending approval for:

- A: Grandchamp;
- B: Bossière;
- C: the sector between Le Châtelard and Le Daley.

The stated condition relates to the result of revision of the Lavaux protection-plan law (LPPL). The page was visually inspected at readable resolution; the decision text and three pink-highlighted sector labels are preserved in `approval-reservations-page-9.png` and structured in `approval-exceptions.json`. Their later resolution has **not** been established. The municipal page's general in-force statement cannot resolve that sector-specific question.

These A/B/C identifiers are different from the land-use map's broad-area A–O letters. Pink labels are location markers, not precise exclusion polygons. No exception geometry or reserve-sector boundary has been digitized. Future Lutry extraction must preserve these exceptions and require later sector-specific evidence before claiming approved development geography there.

## Corpus completeness

The table of contents (part 1 PDF7) explicitly lists action sheets (FA) and base data (DB) as annexes. Part 1 PDF4 describes access through the action sheets; part 3 PDF16–19 contains an index referencing them. **An index reference is not the referenced action sheet.** The acquired three-part dossier ends with that index and glossary and does not contain these annexes.

Part 1 PDF16/18 references a separate base-data booklet and historically says it could be consulted at the municipal offices. Current availability is unverified. The currently published planning page links the three dossier parts but no FA/DB booklet. A bounded official-source search did not locate them or a later resolution of the three suspended sectors. An older indexed official `/Documents_services/ATB/…partie_3.pdf` route returns HTTP404. `source-gaps.json` preserves this evidence; it does not assert that the annexes or later decisions do not exist. No request was sent to the municipality.

## Raw OCR completion

All **86 acquired pages** now have a raw French OCR result (106,949 characters including headings/noise). The 12 previous OCR pages are reused by reference; 74 new outputs are in `ocr/`. `ocr-manifest.json` records every source part, PDF page, document hash, text hash and character count. Blank/noisy pages remain in the inventory rather than being silently dropped.

The method is PyMuPDF 2× page rendering followed by Tesseract French, page segmentation mode 3. Rendering is serial; separate Tesseract processes run with bounded concurrency. This is **uncorrected research text**, not a fully verified transcription, normalized rule set or delivered receiver text corpus. Native PDF text remains empty and is not overwritten. The approval reservation and annex references were checked visually; most other OCR content has not been line-by-line reviewed.

## Verification and next work

`verify.py` verifies complete 44/21/21 page coverage without duplicates, source/text hashes, manifest approval qualifications and no-release flags. Existing Lutry source verification and all 47 targeted parser tests remain valid.

Next: obtain the FA/DB annexes and establish later treatment of the three reserved sectors; proceed with explicitly scoped historical map research elsewhere while keeping these gaps visible. Current cadastral alignment, boundary extraction and parcel validation remain pending. No new runtime sectors, parcels, receiver releases or completed communes result from this work.
