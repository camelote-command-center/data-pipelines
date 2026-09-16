from pathlib import Path
import json,hashlib,pymupdf
p=Path(__file__).resolve().parent;m=json.loads((p/'manifest.json').read_text());source=p/m['source_file'];assert hashlib.sha256(source.read_bytes()).hexdigest()==m['source_sha256'];d=pymupdf.open(source)
for a in m['assets']:
 assert hashlib.sha256((p/a['file']).read_bytes()).hexdigest()==a['sha256']
 assert d.extract_image(a['xref'])['image']==(p/a['file']).read_bytes()
 box=pymupdf.Rect(0,0,a['width'],a['height'])*pymupdf.Matrix(a['pixel_to_display_pdf_matrix']);page=d[a['pdf_page']-1]
 assert max(abs(x-y) for x,y in zip(box,a['image_display_bbox']))<.001
 clipped=box & pymupdf.Rect(a['display_clip_rect']);assert max(abs(x-y) for x,y in zip(clipped,page.rect))<.001
 overflow=box.width-page.rect.width;assert abs(overflow-(0 if a['pdf_page']==11 else .960022))<.001
assert [a['pdf_page'] for a in m['assets'] if a['rotation']==180]==[16,18,28]
print('8 original image bytes/hashes, transforms, clipping extents and rotation flags verified')
