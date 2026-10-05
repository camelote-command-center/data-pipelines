# Épalinges: preserve an exact native junction before transformation

This is a held extraction research fixture, with no production adapter or database mutation. It does not change the existing seven thematic collections or their 676 parcel links.

The original page 102 depicts three filled paths 46/47/48 for **Friches**, with effective group opacity 0.699997 and Normal blend. They are indicative environmental source supports, not development capacity or precise legal boundaries. Original fill operators, winding rules and full rendered ancestry are retained in review.json. The original PDF digest, page transfer and diagnostic alignment are unchanged.

Path 47's existing vertex (423.2460021972656, 513.2969970703125) lies exactly inside path 48's third archived edge, at rational parameter 158367/316801. The original three-polygon collection is valid. Independent affine rounding loses this vertex-on-edge relation; the old LV95 union acquires a hole and its later geographic transform becomes invalid. Inserting that already-existing exact source vertex into the edge before transformation preserves source support and exact rational area. The unchanged transforms then preserve three valid polygons without holes. No coordinate moves, approximate collinearity, snapping, buffering, simplification, make_valid or refit are used.

The helper only accepts valid, nonoverlapping collections of simple straight-edged source polygons. It records every exact insertion and is idempotent. It does not resolve arbitrary invalid rings, compound fills or curved contours.

Independent source/operator and fresh read-only PostGIS verification confirmed the noded LV95/4326 coordinates and validity. The source-space symmetric difference and area change are zero. The LV95 area difference from separately rounded old paths is approximately -1.55e-9 m²; this numerical comparison is not a source-accuracy estimate.

**Remaining hold:** geographic alignment remains diagnostic; the whole-support gate fails: all 15 vertices of path 46 lie outside the existing 12 accepted building-centroid control hull (maximum distance 401.047 m); paths 47/48 are inside. The whole collection remains held, without selecting a smaller subset. Source thematic precision remains NULL, currentness and completeness are unqualified, and later source reservations still apply. Valid topology does not authorize delivery, qualification, parcel rights or commune completion. A future delivery requires its own reviewed frame, reference checks and owned transaction approval. Existing adapter gates continue rejecting the fallow key.


Reproduce source-space and LV95 extraction from the exact original PDF (PyMuPDF1.26.7 and Shapely):

```sh
/Users/a/.codex/task-artifacts/vaud-pdcom/runner-test-venv/bin/python /Users/a/.codex/task-artifacts/vaud-pdcom/runner-fix-repo/pipelines/vd-pdcom/epalinges_fallow_research.py --source-pdf /Users/a/.codex/task-artifacts/vaud-pdcom/epalinges-independent/source.pdf --output /Users/a/LLM_Work/re-llm/vaud-pdcom/oct5-epalinges-gates/regenerated-held-extraction.json
```

The entrypoint checks the frozen review digest, original PDF digest, all three native path operators and complete rendered ancestry. It invokes `node_existing_vertices`, applies the unchanged page-transfer/alignment and requires exact agreement with frozen noded coordinates. Output remains explicitly held. Geographic4326 coordinates are separately recorded independent PostGIS evidence; this offline command does not regenerate them or connect to a database.
