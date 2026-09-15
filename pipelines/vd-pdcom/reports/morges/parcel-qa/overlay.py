import pathlib,json,matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from shapely.geometry import shape,box
p=pathlib.Path(__file__).resolve().parent;parcels=json.loads((p/'official-parcels.geojson').read_text())['features'];outlines=json.loads((p.parent/'alignment/full-hatch-lv95.geojson').read_text())['features'];r=json.loads((p/'parcel-review.json').read_text());fig,axes=plt.subplots(2,3,figsize=(15,10))
for ax,f in zip(axes.flat,outlines):
 g=shape(f['geometry']);idx=str(f['properties']['drawing_index']);bounds=g.buffer(20).bounds;roi=box(*bounds);included={x['egrid'] for x in r['sectors'][idx]['0']['pairs']}
 for parcel in parcels:
  pg=shape(parcel['geometry'])
  if not pg.intersects(roi):continue
  prop=parcel['properties'];color='purple' if prop['GENRE_TXT']=='DDP superficie' else 'gray'
  for part in [pg] if pg.geom_type=='Polygon' else pg.geoms:
   x,y=part.exterior.xy;ax.fill(x,y,facecolor='lightblue' if prop['EGRID'] in included else 'white',edgecolor=color,alpha=.5,lw=.5)
 ax.plot(*g.exterior.xy,c='magenta',lw=2)
 for d in [-10,10]:
  bg=g.buffer(d)
  for part in [bg] if bg.geom_type=='Polygon' else bg.geoms:ax.plot(*part.exterior.xy,c='orange',ls='--',lw=.8)
 ax.set_xlim(bounds[0],bounds[2]);ax.set_ylim(bounds[1],bounds[3]);ax.set_aspect('equal');ax.ticklabel_format(useOffset=False,style='plain');ax.tick_params(labelsize=6);ax.set_title(idx+': '+str(len(included))+' base intersections')
fig.suptitle('Morges research: magenta base outlines; orange ±10m sensitivity; blue intersecting cadastral objects')
fig.tight_layout();fig.savefig(p/'parcel-review.png',dpi=120);plt.close(fig)
