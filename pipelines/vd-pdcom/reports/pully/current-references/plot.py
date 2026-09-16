from pathlib import Path
import json
from PIL import Image,ImageDraw,ImageFont
from shapely.geometry import shape
from shapely.ops import unary_union
out=Path(__file__).resolve().parent;par=json.loads((out/'current-named-parcels.geojson').read_text());build=json.loads((out/'current-footprints.geojson').read_text());box=unary_union([shape(f['geometry']) for f in par['features']]).bounds
x0,y0,x1,y1=box;pad=35;x0-=pad;y0-=pad;x1+=pad;y1+=pad;scale=min(1120/(x1-x0),1070/(y1-y0));im=Image.new('RGB',(1400,1320),'white');draw=ImageDraw.Draw(im);font=ImageFont.truetype('/System/Library/Fonts/Supplemental/Arial.ttf',22);small=ImageFont.truetype('/System/Library/Fonts/Supplemental/Arial.ttf',18)
def pix(x,y):return (100+(x-x0)*scale,140+(y1-y)*scale)
def poly(g,fill,outline,width):
 for p in ([g] if g.geom_type=='Polygon' else g.geoms):
  pts=[pix(x,y) for x,y in p.exterior.coords];draw.polygon(pts,fill=fill);draw.line(pts,fill=outline,width=width)
  for ring in p.interiors:draw.polygon([pix(x,y) for x,y in ring.coords],fill='white')
for f in par['features']:poly(shape(f['geometry']),'#eef5fa','#38678a',3)
for f in build['features']:poly(shape(f['geometry']),'#aeb9c1','#6a747a',1)
for f in par['features']:
 p=shape(f['geometry']).representative_point();x,y=pix(p.x,p.y);label=f['properties']['NUMERO'];draw.rectangle((x-27,y-14,x+35,y+15),fill='white');draw.text((x-23,y-12),label,font=font,fill='#142e45')
draw.text((60,25),'Boverattes: current number-matched parcels and official building footprints',font=font,fill='black')
draw.text((60,65),'EPSG:2056 | 8 parcels | 57 footprints in query envelope | 21 overlaps > 1 m²',font=small,fill='black')
draw.text((60,95),'Research reference only. No historical map fit, lineage proof or residual-capacity estimate.',font=small,fill='#8b2929')
draw.line((1250,260,1250,170),fill='black',width=3);draw.polygon([(1250,155),(1243,175),(1257,175)],fill='black');draw.text((1243,125),'N',font=font,fill='black')
draw.line((100,1240,100+50*scale,1240),fill='black',width=4);draw.text((100,1250),'50 m',font=small,fill='black')
im.save(out/'current-reference.png')
