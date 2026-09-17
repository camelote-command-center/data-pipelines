# Grandson annex OCR and map semantics

Processed all **44 scanned pages** in annexes 6 and 7 with French Tesseract at 180 dpi (61,728 raw OCR characters). Source hashes match the acquired dossier. `ocr.json` retains page references and uncorrected text; reading-order and recognition errors remain. `map-legends.json` inventories native legend text from all 11 map sheets, preserving per-sheet semantics rather than flattening the symbols into development capacity.

Annex 6 is a 36-page natural-habitat inventory, with several observations dated May/June 1997. It is not 36 development sectors or a 2021 survey. PDF15 records the Froideville ditches as already disappeared; PDF17 says the Perraudettaz wet depression disappeared after land improvements. These are historical assertions, not new field checks.

Two suspect eastings are present in the actual scans: PDF13 prints 359’200/185’650 and PDF21 prints 583’078/183’925. Both were visually checked, kept verbatim and excluded from geographic use. No inferred digit swaps. The 7,677 m² on PDF21 is a source attribute, not an extracted polygon measurement.

Annex 7 contains eight nature/landscape guidance sheets: hedges, orchards, banks/slopes, lakeshores, watercourses, walls, outdoor landscaping and nature/buildings. Its objectives and inventory links must remain separate from statutory current rules and opportunity geometry.

All 11 map legends explicitly say boundaries are indicative. In particular, new zoning, alternative allocation, zones to reconsider, probable extension, green areas and densification remain distinct. Full symbol-to-vector mapping, independent alignment, currentness and exact-version approval are still pending. No runtime geometry or parcel delivery changes.
