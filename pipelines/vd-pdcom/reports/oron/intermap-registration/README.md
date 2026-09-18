# Oron urbanisation-to-environment registration

SIFT matches page77 to177 at2pixels/PDFpoint, with deterministic coordinate-hash fit/check split before fitting.240ratio-filtered matches:153fit,142RANSACinliers;87reserved checks,76within3pixels. All-check median0.361pixels; accepted-check RMSE0.502pixels. All11outside-threshold checks retained, no post-check tuning. These are image-correspondence diagnostics, not ground accuracy.

The fitted affine includes translation/rotation/scale differences; treating map pages as identically positioned would be wrong. Three sector-center source/target crop pairs visually reviewed: common roads/buildings/river/rail context support placement under different thematic overlays. Composed geographic accuracy still inherits unresolved environment-map error and manual boundary uncertainty. No geographic sector, parcel intersection or receiver delivery.

register.py reproduces algorithm-result.json/matches/crops without overwriting semantic visual review. Requires exact hashed report.pdf at ../recovered-source, PyMuPDF1.28.2,OpenCV5.0.0 andNumPy2.5.3. Source map registration is distinct from approval/currentness.
