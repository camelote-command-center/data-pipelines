"""Render full layered scans; the colour background alone omits map marks."""
from pathlib import Path
import hashlib,json,pymupdf
out=Path(__file__).resolve().parent
review=json.loads((out/'source-review.json').read_text())
assert hashlib.sha256((out/'source.pdf').read_bytes()).hexdigest()==review['sha256']
rows=[]
with pymupdf.open(out/'source.pdf') as doc:
 for mapped in review['map_pages']:
  n=mapped['page_number'];page=doc[n-1];target=f'map-{n}.png'
  pix=page.get_pixmap(matrix=pymupdf.Matrix(3,3),alpha=False);pix.save(out/target)
  layers=[]
  for im in page.get_images(full=True):
   xref=im[0]
   layers.append({'xref':xref,'smask':im[1],'width':im[2],'height':im[3],'bits_per_component':im[4],'colourspace':im[5],'filter':im[8],'decoded_stream_sha256':hashlib.sha256(doc.xref_stream(xref)).hexdigest(),'placements':[{'bbox_unrotated':list(b),'matrix_unit_image_to_unrotated':list(m)} for b,m in page.get_image_rects(xref,transform=True)]})
  rows.append({'pdf_page':n,'theme':mapped['theme'],'file':target,'sha256':hashlib.sha256((out/target).read_bytes()).hexdigest(),'width_px':pix.width,'height_px':pix.height,'scale_px_per_display_point':3,'page_rotation':page.rotation,'display_rect':list(page.rect),'page_rotation_matrix':list(page.rotation_matrix),'layers':layers,'georeference_VP':doc.xref_get_key(page.xref,'VP'),'geographic_fit':'not_attempted','approval_scope':mapped['approval_scope']})
manifest={'document_id':review['document_id'],'source_sha256':review['sha256'],'pymupdf_version':pymupdf.VersionBind,'method':'Full PDF page composite at 3x display resolution; applies PDF rotation, all colour/stencil layers and existing text layer. Rendering resolution is not ground accuracy.','maps':rows}
(out/'map-manifest.json').write_text(json.dumps(manifest,indent=2,ensure_ascii=False)+'\n')
print('maps',len(rows),'layers',sum(len(x['layers']) for x in rows))
