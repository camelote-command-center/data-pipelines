"""Provisional raster-grid fit. Reserved grid checks are not independent ground controls."""
import json
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent
rows = json.loads((ROOT / 'controls.json').read_text())
train = [x for x in rows if x['role'] == 'fit']
checks = [x for x in rows if x['role'] == 'reserved_grid_check']
a = np.array([[*x['pdf_point'], 1] for x in train])
b = np.array([x['lv03'] for x in train])
coef = np.linalg.solve(a, b)
results = []
for x in checks:
    pred = np.array([*x['pdf_point'], 1]) @ coef
    err = float(np.linalg.norm(pred - x['lv03']))
    results.append(dict(id=x['id'], predicted_lv03=pred.tolist(), residual_m=err))
result = dict(source_crs='EPSG:21781', source_coordinates='unrotated PDF points',
    coefficients_pdf_x_y_1_to_lv03=coef.tolist(), fit_count=len(train),
    reserved_grid_checks=results,
    reserved_grid_rmse_m=float(np.sqrt(np.mean([r['residual_m']**2 for r in results]))),
    status='provisional_grid_only_not_ground_validated',
    limits=['Three fitting points solve affine exactly: training residual is not accuracy.',
            'Checks use the same printed grid, not independent ground controls.',
            'Manual picks approximate; no refinement using reserved points.',
            'No geographic sector or parcel delivery; no LV95 conversion performed.'])
(ROOT / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
