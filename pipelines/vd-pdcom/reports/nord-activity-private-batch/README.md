# Nord-vaudois activity outlines — private review

Four native category footprints from the April 2026 consultation synthesis (source SHA pinned in `nord_activity_pilot.py`). ZAL47 Bullet and ZAL85 Chêne-Pâquier explicitly indicate reconversion **out of activity into another building zone**. ZAL103 Molondin and ZAL12 Suchy have no intention symbol shown on their outlines; this does not establish current availability or absence of change. All four have the poor-public-transport asterisk based on 2023 DGMR data. Category color does not establish productive/mixed vocation.

Source geometry is unchanged. A common regional-boundary transform was held fixed and tested against complete independent current **land** parcel shapes: all four have IoU above0.98 and symmetric Hausdorff below5m. DDP shapes are excluded as alignment identities but retained as separately typed overlapping receiver candidates. This verifies indicative placement, not survey accuracy, legal identity, plan approval or development rights. The regional fit alone has31.714m maximum mismatch and is not accepted across the other102 outlines.

Annual acquisition calls the exact-document/SHA hook. Changed source bytes, current cadastral identity, type, geometry or candidate membership require review. Exact EWKB freezes current geometry; GeoJSON display rounding is not used for equality checks. Positive edge overlaps remain marked for boundary review; these are not available parcels. Delivery uses the existing Pixxels monitor and RE-LLM/private Lamap route.

Full106-outline reference acquisition and24 source-code reviews remain in domain inspection evidence and the session artifacts; this delivery selects only the four complete-shape holdouts.


## Multi-parcel batch — 21 September

Thirteen more native outlines pass the same IoU>0.98 and symmetric-Hausdorff<5m thresholds against complete unions of current land parcels. References were acquired before this fit validation and do not refit the regional transform. No clipping, snapping or geometry repair is applied. Source/whole-parcel overlays and source-code leaders were visually reviewed. Seven other candidates remain held, including close misses below0.98 and residual partially intersected land.

Intentions remain separate: zone16 shows extension/dezoning and potential SDA impact;71 dezoning;74 reconversion out of activity;82 reconversion into activity;99 site-level reconversion out/dezoning. For99 only the eastern native fragment10549 is delivered; western10551 remains held. No intention arrow on the other selected outlines is not proof of absence of change.

Hausdorff distances use GEOS discrete symmetric comparison of the complete boundary shapes. An additional check subdivides each segment at5% intervals for all17 outlines; maximum3.0962m, all below5m. This is a reproducible geometric check, not surveyed accuracy.
