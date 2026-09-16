from pathlib import Path
import pymupdf,hashlib,json,subprocess,tempfile,concurrent.futures
out=Path(__file__).resolve().parent;source=out/'source.pdf';sha=hashlib.sha256(source.read_bytes()).hexdigest();assert sha=='02a96fd6036e51c77e45ab188168e1c451d40cef8b71204e1271151431173d6d'
(out/'ocr').mkdir(exist_ok=True)
with tempfile.TemporaryDirectory(prefix='pully-ocr-') as tmp:
 tmp=Path(tmp).resolve();jobs=[]
 with pymupdf.open(source) as doc:
  assert len(doc)==73
  for i,page in enumerate(doc,1):
   f=tmp/f'{i}.png';page.get_pixmap(matrix=pymupdf.Matrix(2,2)).save(f);jobs.append((i,f))
 def run(job):
  i,f=job;r=subprocess.run(['tesseract',str(f),'stdout','-l','fra','--psm','3'],capture_output=True,text=True,timeout=90);assert r.returncode==0
  target=f'ocr/page-{i}.txt';(out/target).write_text(r.stdout);return {'pdf_page':i,'file':target,'chars':len(r.stdout),'sha256':hashlib.sha256(r.stdout.encode()).hexdigest()}
 with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:rows=list(pool.map(run,jobs))
 (out/'ocr-manifest.json').write_text(json.dumps({'document_id':'98ffb71a-e9e2-5643-9baf-93c42967cb30','source_sha256':sha,'source_url':'https://www.pully.ch/media/mzbds3fv/plan-directeur-communal.pdf','method':'raw French Tesseract psm3 PyMuPDF2x; unverified transcription','tesseract_version':subprocess.check_output(['tesseract','--version'],text=True).splitlines()[0],'pymupdf_version':pymupdf.VersionBind,'pages':rows},indent=2)+'\n');print('OCR done',len(rows),sum(r['chars'] for r in rows))
