# Grandson trial alignment — withheld from delivery

Split 393 historical candidates **before fitting** using SHA256 of drawing index: 273 training and 120 reserved. A training-only search tested 756 poses from footprint shape/orientation comparisons. Printed 1:5000 scale was held fixed at 1.7638889 metres per PDF point; training centroid matches refined rotation and translation. Reserved candidates did not select or refine the transform.

Training: 150 accepted automated correspondences, 123 rejected/unmatched; accepted centroid RMSE 1.959 m and median outline IoU 0.658. Reserved: **66 accepted, 54 rejected/unmatched**, accepted centroid RMSE 1.938 m and median IoU 0.695. Only **2 of the 120 reserved candidates** satisfy both IoU >=0.85 and Hausdorff <=5 m among accepted correspondences. Accepted reference identities are unique within each set and disjoint across sets.

These results do **not validate parcel extraction**. The centroid fit alone masks weak footprint-shape agreement. Correspondences are automated, with fixed 8 m/area-ratio gates; accepted-subset RMSE does not describe all 120 reserved candidates. No holdout-driven tuning was performed. Source geometry is indicative historical cadastre; geometry changes, map generalisation or incorrect correspondences remain possible.

Six reserved examples (four spatial extremes, lowest/highest IoU) are rendered in `checks.pdf`. Source paths 2377, 1662 and 2308 were visually inspected: offsets/shape differences remain, including on the stronger example. Full results retain all rejected cases and distances. Exact replay and disjoint accepted identities were checked. Source/reference hashes and the frozen split are recorded.

The affine coefficients are research only. No sectors, parcel intersections, receiver changes or commune completion. Next investigate source scale/generalisation and explicit landmark correspondences independently, retaining this reserved audit without relabelling failed points as successes. Any new fitting experiment needs a new untouched validation set.
