# Villa Louise explicit-landmark alignment trial

Three manually identified main corners of Villa Louise (EGID841451, official OBJECTID46315544) train a four-parameter similarity with reflected pixel-y axis. Two stream/road junctions over100m away and a fourth villa corner were reserved as checks before fitting. `controls.json` freezes the source-image hash, coordinates, reference vertex identities and train/check roles. No nearest-neighbour search or post-fit adjustment was used.

Train RMSE0.6653m is not acceptance evidence. Reserved checks are5.8483m and8.1890m at the two stream/road junctions and1.2624m at the same-building corner: combined RMSE5.8554m, only1/3within5m. The external checks both exceed5m. **Geographic release rejected.** Three train corners on one building do not constrain wider scan distortion; historical stream geometry, symbolic building outlines and manual matching remain uncertain. Do not reuse the tested check points as fresh independent evidence in a later fit.

`fit.py` replays the frozen controls and renders current parcels (cyan) and footprints (magenta) over the historical page. The first local implementation had an equation/sign error, corrected before saving this final result; controls were unchanged. The corrected equations are E=a*x+b*y+tx, N=b*x-a*y+ty. Replay verification checks both the algebra and reference identities.

`pixel-outline.json` is one source-only manually traced historical intermediate-zone perimeter, in rotated displayed-image pixels. It preserves the source’s spatial proposal, not modern parcel boundaries or developable capacity. No geographic polygon, parcel intersections, receiver delivery or commune completion is generated. The earlier cadastral identity/currentness and area discrepancies remain open.

Next obtain more widely separated explicit historical-stable controls or a better georeferenced source. Preserve this rejected trial and its held-out errors rather than tune points to pass.
