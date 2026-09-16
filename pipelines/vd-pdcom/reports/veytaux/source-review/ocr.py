from pathlib import Path
import pymupdf,hashlib,json,subprocess,tempfile,concurrent.futures
out=Path(__file__).resolve().parent;source=out/'source.pdf';sha=hashlib.sha256(source.read_bytes()).hexdigest();assert sha=='15978db34e86ca523f27f518b126db915bf52052dc15e087da6e172439c2e348'
(out/'ocr').mkdir(exist_ok=True)
with tempfile.TemporaryDirectory(prefix='veytaux-ocr-') as tmp:
 tmp=Path(tmp).resolve();jobs=[]
 with pymupdf.open(source) as doc:
  assert len(doc)==32
  for i,page in enumerate(doc,1):
   f=tmp/f'{i}.png';page.get_pixmap(matrix=pymupdf.Matrix(2,2)).save(f);jobs.append((i,f))
 def run(job):
  i,f=job;r=subprocess.run(['tesseract',str(f),'stdout','-l','fra','--psm','3'],capture_output=True,text=True,timeout=90);assert r.returncode==0
  target=f'ocr/page-{i}.txt';(out/target).write_text(r.stdout);return {'pdf_page':i,'file':target,'chars':len(r.stdout),'sha256':hashlib.sha256(r.stdout.encode()).hexdigest()}
 with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:rows=list(pool.map(run,jobs))
 (out/'ocr-manifest.json').write_text(json.dumps({'document_id':'43add308-b945-5a8f-bd9e-fc2217fe9bc5','source_sha256':sha,'source_url':'https://veytaux.ch/uploads/26c3b5aecb7be4adf973b0b883013a99/documents/PDCom%20du%2027_1600259156.pdf','method':'raw French Tesseract psm3 PyMuPDF2x; unverified transcription','tesseract_version':subprocess.check_output(['tesseract','--version'],text=True).splitlines()[0],'pymupdf_version':pymupdf.VersionBind,'pages':rows},indent=2)+'\n');print('OCR done',len(rows),sum(r['chars'] for r in rows))
