# Broye-Vully regional batch

The previously broken consultation download page hid five public PDFs still available in the official WordPress media index. Registered 212 pages, 25 raw tables and two native map sheets for the 31 current Broye-Vully communes. This is regional PDR/SRGZA source coverage, not 31 completed municipal PDComs. No receiver increment.

The May 2026 consultation concerns Payerne modifications. The strategy is updated July 2025 and the synthesis map is from February 2025; later changes and approval remain unresolved. The separate Fribourg map bundle was excluded from Vaud registration.

The annex tables separate diagnostic/strategy vintages, occupied land, potentially mobilisable reserves, proposals, reconversion and dezoning. Their 518 raw rows include headers and do not represent distinct parcels. Native path inventories preserve 38,198 classification-map paths and 110,309 synthesis-map paths without accepting geographic alignment.

Annex PDF39 exposed a false positive: an RL measurement viewport was marked georeferenced. Acquisition now requires GEO subtype metadata and resolves bounded indirect references. This is only a metadata presence flag, not proof of geographic accuracy. Verified against the actual RL table and an independent genuine GEO page, with regression tests.

Full evidence and reproducible batch scripts: RE-LLM workspace `artifacts/vd-pdcom/broye-regional`. Stable operation/checkpoint IDs are in review.json and Pixxels. The existing annual acquisition handles the added sources; no duplicate crawl is needed.
