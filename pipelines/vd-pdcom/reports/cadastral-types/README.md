# Cadastral object kinds in private PDCom delivery

The official Vaud API/APIGeo layer21 identifies Aigle233/CH314581837868 and1817/CH598378414564 as parcelle privée (bien-fonds);3825/CH400745598377 and1818/CH807945578340 are DDP superficie. Overlap is therefore expected between distinct cadastral objects, not evidence of a duplicate to delete. The authoritative response snapshot includes selected cadastral attributes and geometry, no owners. Coordinates are EPSG2056.

cadastral_types.py refreshes all candidate EGRIDs through layer21 during full/annual acquisition. Adds cadastral_object_kind and cadastral_type_evidence to source/private receiver rows. Values: bien_fonds, ddp_superficie, ddp_source, unknown. Missing/unrecognized objects remain explicit unknown; unexpected/duplicate identities, commune mismatches and truncated/count-mismatched responses fail before writes. HTTP errors leave prior metadata intact. All batches complete before one write transaction.

Preserve both land parcels and rights. Downstream land-area calculations must scope to land objects or union geometry; object-kind alone does not establish planning rights, validity, or final release. Current classification does not refresh older federal geometry or area. Do not substitute the official service area values without reviewing the different source vintage.

Install source migration20260914183409; run updated Lamap receiver DDL and RE-LLM foreign DDL on their registered databases. Existing RLS/private grants remain. The full workflow refreshes types after acquisition and delivers via the existing private FDW route; discovery-only runs retain prior evidence timestamps. Unknown types do not silently become land parcels.
