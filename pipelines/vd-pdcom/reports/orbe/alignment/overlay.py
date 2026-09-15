import pathlib,json,numpy as np,matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from shapely.geometry import shape,Polygon,box
root=pathlib.Path(__file__).resolve().parent;a=json.loads((root/'alignment.json').read_text());angle,scale,tx,ty=a['parameters'];R=np.array([[np.cos(angle),-np.sin(angle)],[np.sin(angle),np.cos(angle)]]);P0=np.array(a['source_origin_y_flipped']);Q0=np.array(a['reference_origin_lv95']);meta=json.loads((root/'source-components.json').read_text());foot=[shape(f['geometry']) for f in json.loads((root/'official-footprints.geojson').read_text())['features']];review=json.loads((root/'footprint-review.json').read_text())
def trans(p):return (p-P0)@R.T*scale+[tx,ty]+Q0
fig,axes=plt.subplots(4,4,figsize=(16,16))
for ax,ev in zip(axes.flat,review['outliers']):
 i=ev['source_index'];s=np.array(meta[i]['source_geometry']);s[:,1]*=-1;g=Polygon(trans(s));center=g.centroid;roi=box(center.x-60,center.y-60,center.x+60,center.y+60)
 for f in foot:
  if not f.intersects(roi):continue
  for sub in [f] if f.geom_type=='Polygon' else f.geoms:
   x,y=sub.exterior.xy;ax.fill(x,y,color='lightgray',ec='gray',lw=.5)
 x,y=g.exterior.xy;ax.plot(x,y,color='magenta',lw=1.5);ax.scatter(*ev['lv95'],color='blue',s=10);ax.set_xlim(center.x-60,center.x+60);ax.set_ylim(center.y-60,center.y+60);ax.set_aspect('equal');ax.ticklabel_format(useOffset=False,style='plain');ax.tick_params(labelsize=5);ax.set_title(f"Source{i}: RCB{ev['rcb_distance_m']:.1f}m / IoU{ev['intersection_over_union']:.2f}",fontsize=9)
for ax in axes.flat[len(review['outliers']):]:ax.axis('off')
fig.suptitle('Orbe holdout outliers: magenta source outlines; gray current footprints; blue source centroid');fig.tight_layout();fig.savefig(root/'outlier-review.png',dpi=120);plt.close(fig)
