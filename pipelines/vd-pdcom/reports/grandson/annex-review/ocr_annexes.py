import pathlib,json,hashlib,subprocess,concurrent.futures,pymupdf,os
p=pathlib.Path(__file__).resolve().parent.parent;src=p/'source-review';out=p/'annex-review';out.mkdir(exist_ok=True);manifest=json.loads((src/'sources.json').read_text());jobs=[]
for i in [12,13]:
 b=(src/f'{i:02}.pdf').read_bytes();assert hashlib.sha256(b).hexdigest()==next(x['sha256'] for x in manifest if x['local_index']==i)
 d=pymupdf.open(stream=b,filetype='pdf')
 for n,q in enumerate(d,1):
  img=out/f'{i:02}-{n:02}.png';q.get_pixmap(dpi=180).save(img);jobs.append((i,n,img))
def work(job):
 i,n,img=job;r=subprocess.run(['/opt/homebrew/bin/tesseract',str(img),'stdout','-l','fra'],capture_output=True,text=True,timeout=50,env={**os.environ,'OMP_THREAD_LIMIT':'1'});assert r.returncode==0,r.stderr;return {'source_index':i,'page':n,'text':r.stdout,'method':'tesseract fra at 180dpi; uncorrected OCR'}
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:rows=list(ex.map(work,jobs))
(out/'ocr.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2));print('OCR pages',len(rows),'chars',sum(len(x['text']) for x in rows))
for x in rows:print(x['source_index'],x['page'],x['text'][:180].replace('\n',' '))
