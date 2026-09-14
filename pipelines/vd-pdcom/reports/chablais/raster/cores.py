"""Separate interior-pigment evidence from thin strokes, without inventing sectors."""
import argparse,json,hashlib
from pathlib import Path
import numpy as np
from scipy import ndimage

def analyze(root,output):
    report=json.loads((root/'candidates.json').read_text())
    if report['coordinate_system']!='source_image_pixels_top_left_NOT_geographic':raise ValueError('wrong_coordinate_system')
    masks=np.load(root/'masks.npz');result={'source_sha256':report['source_sha256'],'source_pdf_page':29,
      'coordinate_system':report['coordinate_system'],'mask_file_sha256':hashlib.sha256((root/'masks.npz').read_bytes()).hexdigest(),
      'method':'Euclidean interior distance in source pixels; retain original connected component when its maximum distance is at least the threshold. No boundary filling.',
      'interpretation':'Interior depth distinguishes substantial colour interiors from thin strokes. It does not identify a complete planning sector or prove a semantic category.',
      'tolerances':{}}
    saved={}
    for key in masks.files:
        mask=masks[key];labels,count=ndimage.label(mask,np.ones((3,3),dtype=np.uint8));sizes=np.bincount(labels.ravel());distance=ndimage.distance_transform_edt(mask)
        peaks=ndimage.maximum(distance,labels,np.arange(1,count+1));components=[]
        for idx,sl in enumerate(ndimage.find_objects(labels),1):
            ys,xs=sl
            components.append({'id':idx,'matched_pixels':int(sizes[idx]),'max_interior_depth_pixels':float(peaks[idx-1]),'bbox':[xs.start,ys.start,xs.stop,ys.stop]})
        sensitivity={}
        for depth in [2,3,4]:
            ids=np.flatnonzero(peaks>=depth)+1;keep=np.isin(labels,ids);saved[key+'_depth'+str(depth)]=keep
            sensitivity[str(depth)]={'retained_components':len(ids),'retained_pixels':int(keep.sum()),'thin_or_small_pixels':int(mask.sum()-keep.sum()),'component_ids':ids.tolist()}
        result['tolerances'][key]={'components':components,'depth_sensitivity':sensitivity}
    output.mkdir(parents=True,exist_ok=True);np.savez_compressed(output/'core-supported-masks.npz',**saved)
    (output/'core-analysis.json').write_text(json.dumps(result,indent=2)+'\n')
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();r=analyze(a.input,a.output)
    print(json.dumps({k:v['depth_sensitivity'] for k,v in r['tolerances'].items()}))
