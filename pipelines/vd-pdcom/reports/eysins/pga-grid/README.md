# Eysins historical PGA grid calibration

The recovered one-page PGA contains a labelled Swiss LV03 coordinate grid. This is an adjacent historical zoning reference, not the SDAN development map. Its native7016×4961 image is rotated90° clockwise and resized to a documented1000×1414 review coordinate system. The PDF prints2November1997; the municipal publication labels1999. Those dates are not interchangeable approval evidence.

For each coordinate axis, two labelled grid lines (four manually selected endpoint observations) fit an affine coordinate function. A third labelled line is withheld. Four held-out **axis-coordinate** residuals have RMSE1.9477m and maximum3.0752m. These are internal printed-grid consistency checks; they do not measure independent geographical accuracy or a two-dimensional surveyed-point RMSE. Manual picks and scan distortion remain limitations.

Current official footprint outlines from the previously frozen690-feature snapshot are transformed from EPSG:2056 to EPSG:21781 with PostGIS and inverse-projected into the scan. The magenta overlay broadly follows many historical buildings, with changes/mismatches and new buildings visible. No specific footprint identity or corner correspondence is yet accepted, and no claim is made that every1997building matches today. Blue grid lines trained the fit; orange lines were withheld.

`controls.json` preserves labelled observations and source identity. `calibrate.py` reproduces the affine coefficients, residuals and visual overlay. `verify.py` checks source/artifact hashes and exact replay. `footprint-lines-lv03.json` is a visual-reference MultiLineString in explicitly declared LV03, not candidate sector geometry.

Next use clearly identified matching buildings/corners to bridge the PGA and SDAN image, reserve independent unused controls and check distortion. Do not apply this PGA transform directly to the SDAN image. No sector/parcel/receiver mutation; no completed commune.

Separately, workflow35029823718 ran the PR84 head successfully:30of243queued unresolved communes were retried,0new candidates,213remain due. All30attempt rows were verified and no discovery lock remained held. Empty results remain unresolved. Full annual acquisition was not verified by this discovery-only check; private counts remain40/2267.
