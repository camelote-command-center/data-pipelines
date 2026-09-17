# Communet east and northwest diagnostic controls

Adds 12 corner observations on three existing hatched buildings (1176a/994a/1430) to the frozen affine audit. New sample RMSE0.665m,max0.983m. Original crop pixels and exact cadastral vertices retained; no fit update. Adjacent historic building labelled non-cadastral is excluded. Labels1705/1706 are land labels, not these building identities.

Combined24corners/sixbuildings remain a selected, correlated diagnostic, not blind ground validation. Review.json reports combined error and per-envelope fraction outside the expanded diagnostic convex hull. Northern envelopes remain extrapolated; a hull is only a coverage indicator. All PR129 observations and failed-index audit preserved unchanged.

Extract/evaluate scripts reproduce crops and this sample. Source PDF, frozen transform and official reference snapshot are the previously hashed inputs. No parcel intersections, runtime geometry, LV95 conversion or remaining-capacity inference.
