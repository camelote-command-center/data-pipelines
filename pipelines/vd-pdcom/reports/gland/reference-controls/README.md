# Communet–Borgeaud independent reference acquisition

756 current official cadastral footprints from Vaud APIGeo layer 22, Gland BFS 5721, bounded LV03 envelope. Count before/after and unique OBJECTIDs agree. The snapshot excludes owner attributes. Source parameters, timestamp and digest are in source.json.

The frozen PR127 affine is inverted to draw current outlines in magenta over the 2010 map. West/south diagnostic crops visually support rough placement, with offsets still visible. This is not quantified validation. Interior modern buildings are not historical control correspondences. Elevation diagrams below the map are excluded from crops.

Run overlay.py with NumPy, Pillow and PyMuPDF after obtaining the exact source PDF named in review.json at ../sector-currentness/communet-plan.pdf. It does not refit any control. Snapshot coordinates were requested directly as EPSG:21781 from the official service; no LV95 geometry was created.

Next: declare unchanged historical corner correspondences with spatial coverage and measure withheld errors. No new runtime sector, parcel allocation, residual-capacity claim or receiver delivery is authorized by this research result.
