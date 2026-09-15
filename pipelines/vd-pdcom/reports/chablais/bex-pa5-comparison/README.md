# PA5 Bex map comparison — research only

SHA-gated extraction selects the 834×707 raster on PDF70. This is a different image from the PA4 809×979 inset. The PA5 legend explicitly calls its perimeter indicative.

`prepare.py` isolates grayscale building components (RGB distance <14 from128/126/129, 30–5000 pixels, edge components excluded):150 candidates. `fit.py` uses the existing frozen1581 RCB references, fixed every-fifth holdout, coarse similarity search and robust nearest-point refinement. An approximate courtyard seed was also explored; it is not a surveyed control. Best training solution has22/120 within5m, while only5/30 holdouts are within5m, median12.887m and RMSE19.755m. **Rejected: no geographic use.** Nearest-point coincidence is insufficient. All attempted solutions retained.

`outline.py` extracts the pink outer boundary plus red interior into image-pixel polygons using1/2/3pixel closing and hole filling. These operations infer continuity across overprints. The output is explicitly NOT geographic and no area in square metres or parcel comparison is produced. The review image was visually inspected.

No sector geometry, parcel intersections, receiver rows or completion statuses changed. Existing Bex candidate remains PA4 review-only. Next identify explicit building correspondences or obtain georeferenced PA5 cartography and validate separate controls. Pixel resemblance, plan approval and stated area do not establish map equivalence.
