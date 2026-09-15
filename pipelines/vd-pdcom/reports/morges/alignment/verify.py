"""Offline replay of frozen Morges alignment metrics and geographic geometry."""
import pathlib,json,numpy as np
from scipy.spatial import cKDTree
from shapely.geometry import shape
p=pathlib.Path(__file__).resolve().parent;a=json.loads((p/'alignment.json').read_text());P=np.array(json.loads((p/'source-points.json').read_text()));Q=np.array([r[1:] for r in json.loads((p/'reference-points.json').read_text())]);angle,scale,tx,ty=a['parameters'];R=np.array([[np.cos(angle),-np.sin(angle)],[np.sin(angle),np.cos(angle)]]);xy=(P-a['source_origin_y_flipped'])@R.T*scale+[tx,ty]+a['reference_origin_lv95'];dist,idx=cKDTree(Q).query(xy[::5]);assert (dist<5).sum()==a['holdout_under5m']==111
for k,v in [('holdout_median_m',np.median(dist)),('holdout_p90_m',np.quantile(dist,.9)),('holdout_rmse_m',np.sqrt(np.mean(dist**2)))]:assert abs(a[k]-v)<1e-7
foot=json.loads((p/'official-footprints.geojson').read_text())['features'];review=json.loads((p/'footprint-review.json').read_text());assert len(foot)==len({f['properties']['OBJECTID'] for f in foot})==review['count']==2540
assert all(shape(f['geometry']).is_valid for f in foot)
feats=json.loads((p/'full-hatch-lv95.geojson').read_text())['features'];assert len(feats)==6
assert {f['properties']['drawing_index'] for f in feats}=={5558,5761,7935,8039,8192,8238}
commune=shape(json.loads((p/'commune.geojson').read_text()))
for f in feats:
 g=shape(f['geometry']);assert g.is_valid
 assert abs(g.area-f['properties']['area_m2'])<1e-6
 assert abs(g.difference(commune).area-f['properties']['outside_morges_m2'])<1e-6
print('Frozen holdout, official count/identity, geometry/commune metrics verified')
