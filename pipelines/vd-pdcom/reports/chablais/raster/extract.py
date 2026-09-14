"""Reproduce source-pixel candidates. No geographic/parcel/release output."""
import argparse,hashlib,json
from pathlib import Path
import numpy as np
from scipy import ndimage
import pymupdf as fitz

SHA='d2be996343614077c860bc47587ebabd927cf6603edfb4f592c4cee3469dc7ff'

def extract(pdf,output):
    data=pdf.read_bytes()
    if hashlib.sha256(data).hexdigest()!=SHA:raise ValueError('source_sha_mismatch')
    output.mkdir(parents=True,exist_ok=True)
    with fitz.open(stream=data,filetype='pdf') as doc:
        page=doc[28];images=page.get_images(full=True)
        if len(images)!=1 or page.get_drawings():raise ValueError('source_structure_changed')
        raw=doc.extract_image(images[0][0]);pix=fitz.Pixmap(doc,images[0][0])
        if pix.n!=3 or (pix.width,pix.height)!=(3627,2573):raise ValueError('unexpected_raster')
        rgb=np.frombuffer(pix.samples,dtype=np.uint8).reshape(pix.height,pix.width,3)
        transform=list(page.get_image_rects(images[0][0],transform=True)[0][1])
    # Half-open pixel boxes, origin top left. Selected inside the printed swatch.
    swatch=[2766,730,2815,747]
    x0,y0,x1,y1=swatch;target=np.median(rgb[y0:y1,x0:x1].reshape(-1,3),axis=0)
    # Map rectangle excludes every legend/title/text block; no canton assignment.
    extent=[73,52,2037,2513];x0,y0,x1,y1=extent
    delta=np.linalg.norm(rgb.astype(np.float32)-target,axis=2)
    allowed=np.zeros(rgb.shape[:2],dtype=bool);allowed[y0:y1,x0:x1]=True
    report={'source_sha256':SHA,'source_pdf_page':29,'image_sha256':hashlib.sha256(raw['image']).hexdigest(),
      'coordinate_system':'source_image_pixels_top_left_NOT_geographic','pymupdf_version':fitz.VersionBind,'numpy_version':np.__version__,'image_size':[pix.width,pix.height],
      'image_unit_square_to_pdf_matrix':transform,'legend_label':'secteur mixte de densification stratégique',
      'legend_sample_box':swatch,'legend_median_rgb':target.tolist(),'map_box':extent,
      'method':'Euclidean RGB distance, 8-connected components; no smoothing, closing or inferred boundary completion',
      'limitations':['Red dashed future-development borders may also match.','Labels, roads and base-map pixels can split or obscure a sector.','Components are pigment fragments, not a count of planning sectors.','Vaud/Valais assignment and geographic alignment are pending.','Printed boundaries are approximate; source version approval unverified.'],
      'thresholds':{}}
    masks={}
    for tolerance in [20,35,50]:
        mask=allowed&(delta<=tolerance);labels,count=ndimage.label(mask,np.ones((3,3),dtype=np.uint8));sizes=np.bincount(labels.ravel());components=[]
        for idx,sl in enumerate(ndimage.find_objects(labels),1):
            if sizes[idx]<20:continue
            ys,xs=sl;cy,cx=ndimage.center_of_mass(mask,labels,idx)
            components.append({'id':idx,'matched_pixels':int(sizes[idx]),'bbox':[xs.start,ys.start,xs.stop,ys.stop],'centroid':[cx,cy]})
        report['thresholds'][str(tolerance)]={'matched_pixels':int(mask.sum()),'all_components':count,'components_at_least_20_pixels':components,'retained_pixels':sum(c['matched_pixels'] for c in components)}
        masks['t'+str(tolerance)]=mask
    np.savez_compressed(output/'masks.npz',**masks)
    (output/'candidates.json').write_text(json.dumps(report,indent=2)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,ax=plt.subplots(figsize=(10,13));ax.imshow(rgb);overlay=np.zeros((*rgb.shape[:2],4));overlay[masks['t35']]=[0,1,1,.9];ax.imshow(overlay)
    for c in report['thresholds']['35']['components_at_least_20_pixels']:
        x,y=c['centroid'];ax.annotate(str(c['id']),(x,y),fontsize=5,color='blue')
    ax.set_xlim(extent[0],extent[2]);ax.set_ylim(extent[3],extent[1]);ax.set_title('PA4 PDF29 — cyan: matched red pigment, not validated sectors');ax.set_xlabel('Source image pixels');fig.tight_layout();fig.savefig(output/'review.png',dpi=150);plt.close(fig)
    return report

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--pdf',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();r=extract(a.pdf,a.output)
    print(json.dumps({k:{'matched_pixels':v['matched_pixels'],'retained_components':len(v['components_at_least_20_pixels'])} for k,v in r['thresholds'].items()}))
