"""Batch unreviewed candidate extraction, distinct from accepted corpus/geometry.

Evidence lives in discovery_attempts and workflow artifacts. Never promotes
candidates or communes. Reuses source network guards; bounds PDF subprocess time.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from acquire_sources import download


def extract(path):
    import pymupdf
    pages=[]
    with pymupdf.open(path) as doc:
        if len(doc)>500: raise ValueError('page_limit')
        preview_count=0
        for i,page in enumerate(doc):
            text=page.get_text()[:30000]
            item={'page':i+1,'width':page.rect.width,'height':page.rect.height,
                  'text':text,'needs_ocr':len(text.strip())<50,
                  'embedded_georef':doc.xref_get_key(page.xref,'VP')[0]!='null'}
            # Do not enumerate arbitrary PDF drawing paths: some reports exhaust memory.
            if preview_count<8 and (i==0 or page.rect.width>page.rect.height or
                any(t in text.lower() for t in ('plan de synthèse','concept territorial','légende'))):
                name=f'{path.stem}-page-{i+1}.png'
                scale=min(1.5,1600/max(page.rect.width,page.rect.height))
                page.get_pixmap(matrix=pymupdf.Matrix(scale,scale)).save(path.parent/name)
                item['preview']=name;preview_count+=1
            pages.append(item)
    path.with_suffix('.json').write_text(json.dumps(pages,ensure_ascii=False))


def process(row,output):
    result={k:row[k] for k in ('commune_bfs','source_url','evidence_url','link_text')}
    result['review_status']='pending'
    try:
        data=download(row['source_url'],max_bytes=100_000_000,max_seconds=90)
        sha=hashlib.sha256(data).hexdigest();path=output/(str(row['commune_bfs'])+'-'+sha+'.pdf');path.write_bytes(data)
        subprocess.run([sys.executable,__file__,'--extract',str(path)],check=True,timeout=120,capture_output=True)
        result.update(sha256=sha,pages=json.loads(path.with_suffix('.json').read_text()),status='extracted_unreviewed')
    except Exception as exc:
        result.update(status='extraction_failed',error_type=type(exc).__name__)
        response=getattr(exc,'response',None)
        if response is not None:result['http_status']=response.status_code
    return result


def run(args):
    args.output.mkdir(parents=True,exist_ok=True);operation=str(uuid.uuid4())
    with psycopg2.connect(os.environ['RE_LLM_DB_URL'],connect_timeout=15) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as c:
            c.execute('''SELECT s.* FROM bronze_ch.vd_pdcom_source_candidates s
              JOIN bronze_ch.vd_pdcom_communes c USING(commune_bfs)
              WHERE c.is_current AND s.kind='pdf' AND s.review_status='pending'
              AND NOT EXISTS(SELECT 1 FROM bronze_ch.vd_pdcom_documents d WHERE d.source_url=s.source_url)
              AND NOT EXISTS(SELECT 1 FROM bronze_ch.vd_pdcom_discovery_attempts a,
                jsonb_array_elements(COALESCE(a.evidence->'documents','[]'::jsonb)) x
                WHERE a.outcome='candidate_batch' AND a.commune_bfs=s.commune_bfs
                AND x->>'source_url'=s.source_url AND (x->>'status'='extracted_unreviewed' OR a.attempted_at>now()-interval '7 days'))
              ORDER BY s.commune_bfs,s.source_url LIMIT %s''',(args.limit,));rows=c.fetchall()
        groups={};report={'operation_id':operation,'selected':len(rows),'extracted':0,'failed':0,'coverage_complete':False}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures=[pool.submit(process,r,args.output) for r in rows]
            for future in as_completed(futures):
                r=future.result();bfs=r['commune_bfs'];groups.setdefault(bfs,[]).append(r)
                report['extracted' if r['status']=='extracted_unreviewed' else 'failed']+=1
                with conn.cursor() as c:
                    evidence={'stage':'candidate_batch','documents':groups[bfs],'review_status':'pending','coverage_complete':False}
                    c.execute('''INSERT INTO bronze_ch.vd_pdcom_discovery_attempts(operation_id,commune_bfs,outcome,evidence)
                      VALUES(%s,%s,'candidate_batch',%s) ON CONFLICT(operation_id,commune_bfs) DO UPDATE SET evidence=excluded.evidence''',(operation,bfs,Json(evidence)))
                conn.commit()
                (args.output/'candidate-batch-report.json').write_text(json.dumps(report,indent=2))
                print(json.dumps({k:v for k,v in r.items() if k!='pages'}),flush=True)
        report['communes']=len(groups)
        (args.output/'candidate-batch-report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
        return report


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--extract',type=Path);p.add_argument('--limit',type=int,default=100);p.add_argument('--workers',type=int,default=4);p.add_argument('--output',type=Path,default=Path('vd-pdcom-output/candidates'));a=p.parse_args()
    if a.extract:extract(a.extract)
    else:run(a)
