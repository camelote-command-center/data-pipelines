"""Reproduce original pictogram rectangle centers, not physical parking extents."""
import base64
from shapely.geometry import shape,box
from shapely.affinity import affine_transform

def validate_symbol(p):
    s=p['source_symbol'];g=shape(p['symbol_geometry_pdf'])
    if 'xref' in s:
        w,h=s['image_dimensions'];raw=base64.b64decode(p['symbol_rgb_base64'],validate=True)
        if len(raw)!=w*h*3:raise ValueError('parking_image_size')
        points=[(i%w,i//w) for i in range(w*h) if max(raw[3*i:3*i+3])-min(raw[3*i:3*i+3])>40]
        bounds=[min(x for x,y in points),min(y for x,y in points),max(x for x,y in points)+1,max(y for x,y in points)+1]
        if bounds!=s['colored_pixel_bounds']:raise ValueError('parking_source_pixel_bounds')
        m=s['image_transform'];expected=affine_transform(box(bounds[0]/w,bounds[1]/h,bounds[2]/w,bounds[3]/h),[m[0],m[2],m[1],m[3],m[4],m[5]])
        if not expected.equals_exact(g,1e-7):raise ValueError('parking_image_transform')
    elif s.get('source_operator')!='re' or not g.equals(g.envelope):raise ValueError('parking_original_rectangle')
    if not shape(p['geometry_pdf']).equals_exact(g.centroid,1e-7):raise ValueError('parking_pictogram_center')
