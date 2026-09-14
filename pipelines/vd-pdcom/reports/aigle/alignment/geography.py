import json,pathlib,numpy as np,pymupdf as f,argparse,hashlib
ap=argparse.ArgumentParser();ap.add_argument("--pdf",required=True);args=ap.parse_args()
assert hashlib.sha256(pathlib.Path(args.pdf).read_bytes()).hexdigest()=="bbd1d3d0474d5f491ac4cc17b05d7bf1c7912861d9f93f9347c719bf7522fedc"
from shapely.geometry import shape,mapping
from shapely.affinity import affine_transform
root=pathlib.Path(__file__).resolve().parent;a=json.load(open(root/'alignment.json'));angle,s,tx,ty=a['parameters'];R=np.array([[np.cos(angle),-np.sin(angle)],[np.sin(angle),np.cos(angle)]]);P0=np.array(a['pdf_origin_y_flipped']);Q0=np.array(a['reference_origin_lv95']);d=f.open(args.pdf);p=d[18];h=p.rect.height;m=[s*R[0,0],-s*R[0,1],s*R[1,0],-s*R[1,1],Q0[0]+tx+s*(R[0,1]*h-(R@P0)[0]),Q0[1]+ty+s*(R[1,1]*h-(R@P0)[1])]
features=[]
for x in json.load(open(root.parent/'source-paths.json'))['selected']:
 g=affine_transform(shape(x['geometry_pdf_coordinates']),m);features.append({'type':'Feature','properties':{k:v for k,v in x.items() if k!='geometry_pdf_coordinates'},'geometry':mapping(g)})
(root/'sectors-lv95.geojson').write_text(json.dumps({'type':'FeatureCollection','crs':{'type':'name','properties':{'name':'EPSG:2056'}},'features':features},ensure_ascii=False,indent=2))
Q=np.array([r[1:] for r in json.load(open(root/'reference-points.json'))]);pdf=(Q-Q0-[tx,ty])@R/s+P0;pdf[:,1]=h-pdf[:,1]
overlay=p.new_shape()
for pt in pdf:
 if p.rect.contains(f.Point(*pt)):overlay.draw_circle(f.Point(*pt),1.2)
overlay.finish(color=(1,0,0),width=.5);overlay.commit()
for name,b in [('north',(120,160,410,370)),('centre',(240,330,480,520)),('south',(310,490,520,730))]:p.get_pixmap(matrix=f.Matrix(2,2),clip=f.Rect(*b)).save(root/(name+'.png'))
print('areas',len(features),sum(shape(x['geometry']).area for x in features))
