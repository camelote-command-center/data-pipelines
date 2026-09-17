# Communet raster-to-current-cadastre diagnostic

Twelve manually picked historical corners across three identifiable buildings (numbers 2188, 894 and 928) checked against the frozen PR127 affine and PR128 official EPSG:21781 snapshot. RMSE 1.505 m, maximum 1.915 m; mean signed offset +1.387 m east/+0.472 m north. This is a selected diagnostic, not blind validation or release approval. Four corners per building are correlated. No east/north independent building coverage yet.

Original raster crops, crop coordinates, exact vertex correspondences and full residuals are retained. No affine adjustment was made. Current footprint identity and recognizable form support provisional correspondence, not a surveyed guarantee of unchanged buildings. Contemporary interior structures are excluded.

An initial reference-index error assigned the 2188 lower-right pick to a neighboring step vertex (14). It is corrected to vertex13, preserving the initial result in initial-index-error.json. Raster picks and transformation were not altered. This is a correspondence bookkeeping correction, not tuning a transform to validation errors.

With the previously hashed source PDF available, extract.py regenerates crops/references; evaluate.py reproduces final results using NumPy/PyMuPDF. No runtime geographic sector, parcel allocation, LV95 conversion or receiver delivery. Next: wider independent controls and explicit frame/semantic outline validation.
