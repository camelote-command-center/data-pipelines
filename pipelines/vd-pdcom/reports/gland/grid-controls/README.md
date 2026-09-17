# Communet–Borgeaud provisional grid registration

Visually read six labelled crossings from the rotated cadastral grid of the signed PPA map. Coordinates areLV03 (EPSG:21781), notLV95. Three points were assigned to affine fitting and three reserved as grid checks before solving. Pixel picks and crop-to-PDF conversions are explicit in controls.json. fit.py reproduces the result.

The three reserved grid residuals are0.843m,1.970m and0.542m (RMSE1.276m). Three training points solve the affine exactly, so a zero training error has no accuracy significance. Reserved checks share the printed grid; they are not independent ground validation. Manual picks and local scan distortion remain possible. No retuning performed after checking residuals.

The transform remains withheld from parcel delivery. Independent cadastral correspondences, spatial coverage (particularly west/south), explicitLV03→LV95conversion and semantic boundaries are still required. This is a later PPA sheet, not automatic georeferencing of the1997PDCom synthesis. No geometry, parcel join, capacity allocation or receiver changes.
