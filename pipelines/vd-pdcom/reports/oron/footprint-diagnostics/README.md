# Oron footprint mismatch diagnostics

Six deterministic IoU-quantile cases visually reviewed. Blue is the selected PDF177 native source outline; magenta is the nearest current official footprint under the unchanged boundary transform. Case1 is visibly a different object; case2 shows a compound source outline versus a partial current footprint. Cases3–5 show shape/offset or overprint limitations; case6 agrees closely.

Across1563candidates,median source/reference area ratio1.371;1336source outlines larger;1332centroids within5m;1040IoU>=0.5. These are diagnostics, not certified building identities. Generalisation, split/merge representations and temporal changes require separate resolution; lowIoU alone does not establish an alignment error.

No transform adjustment, transfer to urbanisation77, parcel extraction or runtime delivery. All PR137 diagnostics preserved. diagnose.py reproduces statistics/panels with the exact sourcePDF at ../recovered-source/report.pdf, writing replayed-statistics.json without overwriting human visual review.
