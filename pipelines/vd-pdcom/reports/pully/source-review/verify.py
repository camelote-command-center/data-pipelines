from pathlib import Path
import hashlib,json,pymupdf
p=Path(__file__).resolve().parent
review=json.loads((p/'source-review.json').read_text());assert hashlib.sha256((p/'source.pdf').read_bytes()).hexdigest()==review['sha256']
ocr=json.loads((p/'ocr-manifest.json').read_text());assert ocr['source_sha256']==review['sha256'];assert [x['pdf_page'] for x in ocr['pages']]==list(range(1,74))
for row in ocr['pages']:
 data=(p/row['file']).read_bytes();assert hashlib.sha256(data).hexdigest()==row['sha256'];assert len(data.decode())==row['chars']
maps=json.loads((p/'map-manifest.json').read_text());assert maps['source_sha256']==review['sha256'];assert {x['pdf_page'] for x in maps['maps']}=={x['page_number'] for x in review['map_pages']}
with pymupdf.open(p/'source.pdf') as d:
 assert len(d)==73
 for row in maps['maps']:
  assert hashlib.sha256((p/row['file']).read_bytes()).hexdigest()==row['sha256']
  page=d[row['pdf_page']-1];assert page.rotation==row['page_rotation'];assert len(page.get_images(full=True))==len(row['layers'])
  for layer in row['layers']:assert hashlib.sha256(d.xref_stream(layer['xref'])).hexdigest()==layer['decoded_stream_sha256']
 for row in json.loads((p/'inventory.json').read_text()):assert len(d[row['pdf_page']-1].get_text())==row['native_chars']
for row in json.loads((p/'web-evidence.json').read_text()):assert hashlib.sha256((p/row['file']).read_bytes()).hexdigest()==row['sha256']
print('Verified source, 73 OCR pages, 12 map composites/524 layers and municipal evidence hashes. No geographic accuracy claim.')
