from pathlib import Path
import json,subprocess,pymupdf,tempfile,concurrent.futures,hashlib
out=Path(__file__).resolve().parent;p=out.parent;(out/'ocr').mkdir(exist_ok=True)
with tempfile.TemporaryDirectory(prefix='lutry-ocr-') as tmp:
 tmp=Path(tmp).resolve();jobs=[]
 for part in [1,2,3]:
  with pymupdf.open(p/f'source-review/part-{part}.pdf') as doc:
   for k,page in enumerate(doc,1):
    existing=p/f'source-review/ocr/part-{part}-page-{k}.txt'
    if existing.exists():continue
    file=tmp/f'{part}-{k}.png';page.get_pixmap(matrix=pymupdf.Matrix(2,2)).save(file);jobs.append((part,k,file))
 def run(job):
  part,k,file=job;r=subprocess.run(['tesseract',str(file),'stdout','-l','fra','--psm','3'],capture_output=True,text=True,errors='replace',timeout=90)
  if r.returncode:raise RuntimeError(f'OCR failed part{part}page{k}: '+r.stderr[:200])
  (out/'ocr'/f'part-{part}-page-{k}.txt').write_text(r.stdout);return {'part':part,'pdf_page':k,'text_file':f'ocr/part-{part}-page-{k}.txt','chars':len(r.stdout),'sha256':hashlib.sha256(r.stdout.encode()).hexdigest()}
 with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:rows=list(pool.map(run,jobs))
 for f in (p/'source-review/ocr').glob('*.txt'):
  _,part,_,page=f.stem.split('-');text=f.read_text();rows.append({'part':int(part),'pdf_page':int(page),'text_file':'../source-review/ocr/'+f.name,'chars':len(text),'sha256':hashlib.sha256(text.encode()).hexdigest()})
 rows.sort(key=lambda r:(r['part'],r['pdf_page']));assert len(rows)==86
 (out/'ocr-manifest.json').write_text(json.dumps({'method':'Tesseract French psm3,PyMuPDF2x; raw uncorrected research OCR. Existing12pages reused,74new pages.','tesseract_version':subprocess.check_output(['tesseract','--version'],text=True).splitlines()[0],'source_pdfs':[{'part':r['part'],'document_id':r['document_id'],'sha256':r['sha256'],'pages':r['page_count']} for r in json.loads((p/'source-review/sources.json').read_text())],'pages':rows},indent=2)+'\n');print('86page OCR inventory complete',sum(r['chars'] for r in rows))
