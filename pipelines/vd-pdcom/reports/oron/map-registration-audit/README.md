# Oron environment-map registration audit

Page177 contains a native candidate municipal boundary, path10342,1727line items/2280unique vertices. North-up bbox registration against current official municipal bounds yields nearly equal x/y scales8.6760/8.6758m per PDF point. Same-boundary vertex distance RMSE0.376m,max2.029m. These are boundary consistency diagnostics, not independent accuracy.

Frozen transform then checked against1563 selected gray native polygons and current aboveground reference footprints using nearest centroids:median distance1.948m,medianIoU0.588;only28satisfyIoU>=0.85 anddistance<=5m. All mismatches and selection exclusions retained. Gray-path building identity and source generalisation remain unresolved. No refitting after these observations.

Page177 environment and page77 urbanisation are distinct maps. No transform transferred to the three densification candidates, and no geographic runtime sector/parcel created. Next verify individual correspondences/generalisation and inter-map placement; do not tune on these checks then claim held-out accuracy.

Reproduce boundary_fit.py then building_check.py with exact hashed report.pdf placed in ../recovered-source. Native path IDs and reference snapshots are fixed. Consultation status persists.
