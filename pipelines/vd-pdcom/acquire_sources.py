"""Acquire reviewed official PDCom links; preserve unverified version status.

This inventories document pages, not geographic features. No reviewed link or
successful PDF download advances a commune to validated or delivery verified.
"""
import argparse
import hashlib
import io
import zipfile
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urljoin, urlsplit
import uuid

import pymupdf as fitz
import psycopg2
from psycopg2.extras import Json
import requests
from discovery import public_url

ROOT=Path(__file__).resolve().parent


def download(url,max_bytes=100_000_000,max_seconds=180, *, archive=False):
    """Retry bounded transient GET failures, preserving redirect and size guards."""
    deadline=time.monotonic()+max_seconds
    for attempt in range(3):
        if time.monotonic()>=deadline:raise requests.Timeout("pdf_download_deadline")
        try:
            return _download_once(url,max_bytes,deadline,archive=True) if archive else _download_once(url,max_bytes,deadline)
        except (requests.Timeout,requests.ConnectionError,requests.HTTPError,requests.exceptions.ChunkedEncodingError) as exc:
            response=getattr(exc,'response',None)
            status=response.status_code if response is not None else None
            retryable=isinstance(exc,(requests.Timeout,requests.ConnectionError,requests.exceptions.ChunkedEncodingError)) or status in (429,500,502,503,504)
            if not retryable or attempt==2:raise
            delay=2**(attempt+1)
            if time.monotonic()+delay>=deadline:raise requests.Timeout("pdf_download_deadline") from exc
            time.sleep(delay)


def _download_once(url,max_bytes=100_000_000,deadline=None, *, archive=False):
    if deadline is None:deadline=time.monotonic()+180
    host=urlsplit(url).hostname
    for _ in range(5):
        public_url(url)
        if urlsplit(url).hostname not in (host,host.removeprefix('www.'),'www.'+host.removeprefix('www.')):
            raise ValueError('unreviewed_redirect_host')
        remaining=deadline-time.monotonic()
        if remaining<=0:raise requests.Timeout("pdf_download_deadline")
        with requests.get(url,stream=True,timeout=(min(15,remaining),min(30,remaining)),allow_redirects=False) as r:
            if r.is_redirect:
                url=urljoin(url,r.headers['Location']);continue
            r.raise_for_status();data=bytearray()
            for chunk in r.iter_content(131072):
                if time.monotonic()>=deadline:raise requests.Timeout("pdf_download_deadline")
                data.extend(chunk)
                if len(data)>max_bytes:raise ValueError('pdf_size_limit')
            if not data.startswith(b'PK\x03\x04' if archive else b'%PDF'):
                raise ValueError('response_not_zip' if archive else 'response_not_pdf')
            return bytes(data)
    raise ValueError('redirect_limit')


def extract_archive_member(data, member, max_bytes=100_000_000):
    """Read exactly one reviewed PDF member in memory; never extract paths to disk."""
    if not isinstance(member,str) or not member or member.startswith('/') or '..' in member.split('/'):
        raise ValueError('invalid_archive_member')
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        matches=[info for info in archive.infolist() if info.filename==member]
        if len(matches)!=1:raise ValueError('archive_member_missing_or_ambiguous')
        info=matches[0]
        if info.is_dir() or info.flag_bits & 1:raise ValueError('unsupported_archive_member')
        if info.file_size>max_bytes:raise ValueError('archive_member_size_limit')
        with archive.open(info) as stream:pdf=stream.read(max_bytes+1)
        if len(pdf)>max_bytes:raise ValueError('archive_member_size_limit')
        if not pdf.startswith(b'%PDF'):raise ValueError('archive_member_not_pdf')
        return pdf


def acquire(source):
    member=source.get('archive_member')
    if member is None:
        return download(source['pdf_url'],source.get('max_bytes',100_000_000)),None
    data=download(source['pdf_url'],source.get('max_archive_bytes',100_000_000),archive=True)
    pdf=extract_archive_member(data,member,source.get('max_bytes',100_000_000))
    return pdf,{'archive_url':source['pdf_url'],'archive_sha256':hashlib.sha256(data).hexdigest(),
                'archive_member':member,'member_sha256':hashlib.sha256(pdf).hexdigest()}


def has_geographic_viewport(doc, page):
    """Detect GEO viewport metadata, not relative-length measurement viewports.

    Presence is an inspection hint only; CRS/control-point validation is separate.
    Resolve indirect dictionaries with a bounded traversal and cycle protection.
    """
    kind, value = doc.xref_get_key(page.xref, 'VP')
    if kind == 'null':
        return False
    queue, seen, parts = [value], set(), []
    while queue:
        value = queue.pop()
        parts.append(value)
        for ref in re.findall(r'(?<!\d)(\d+)\s+0\s+R\b', value):
            ref = int(ref)
            if ref in seen:
                continue
            if len(seen) >= 32:
                return False
            seen.add(ref)
            queue.append(doc.xref_object(ref))
    metadata = '\n'.join(parts)
    return bool(re.search(r'/Subtype\s*/GEO\b', metadata))


def inspect(data, *, enumerate_vectors=True, max_pages=500):
    if type(max_pages) is not int or not 1 <= max_pages <= 1000:
        raise ValueError('invalid_page_limit')
    pages=[]
    with fitz.open(stream=data,filetype='pdf') as doc:
        if len(doc)>max_pages:raise ValueError('page_limit')
        for number,page in enumerate(doc,1):
            raw_text=page.get_text()
            text=raw_text.replace('\x00','\ufffd')
            pages.append({'page_number':number,'width':page.rect.width,'height':page.rect.height,
                          'text':text,'nul_replacements':raw_text.count('\x00'),'vector_paths':len(page.get_drawings()) if enumerate_vectors else None,
                          'vector_inventory_status':'counted' if enumerate_vectors else 'not_evaluated',
                          'embedded_georef':has_geographic_viewport(doc,page)})
    return pages


def coverage_members(source):
    members=source.get('commune_bfs_list',[source['commune_bfs']])
    if not members or len(members)!=len(set(members)) or source['commune_bfs'] not in members:
        raise ValueError('invalid_commune_coverage')
    if len(members)>1 and source['scope']!='intercommunal':
        raise ValueError('multi_commune_source_requires_intercommunal_scope')
    return members


def persist(conn,source,sha,pages):
    doc_id=str(uuid.uuid5(uuid.NAMESPACE_URL,source['pdf_url']+'#'+sha))
    with conn,conn.cursor() as c:
        members=coverage_members(source)
        c.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_communes WHERE commune_bfs=ANY(%s) AND is_current',(members,))
        if c.fetchone()[0]!=len(members):raise ValueError('source_commune_not_current')
        c.execute('''INSERT INTO bronze_ch.vd_pdcom_documents
        (id,source_url,landing_url,sha256,title,plan_status,scope,status_evidence_url,page_count,inspection)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(source_url,sha256) DO NOTHING''',
        (doc_id,source['pdf_url'],source['landing_url'],sha,source['title'],source['plan_status'],source['scope'],source['landing_url'],len(pages),Json(pages)))
        for bfs in members:
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_document_communes(document_id,commune_bfs,evidence_url)
            VALUES(%s,%s,%s) ON CONFLICT DO NOTHING''',(doc_id,bfs,source['landing_url']))
            c.execute('''UPDATE bronze_ch.vd_pdcom_communes SET discovery_status='sources_found',
            extraction_status=CASE WHEN extraction_status='pending' THEN %s ELSE extraction_status END,
            last_checked_at=now(),evidence=CASE WHEN evidence @> %s THEN evidence ELSE evidence || %s END
            WHERE commune_bfs=%s''',('needs_ocr' if sum(len(p['text'].strip()) for p in pages)<50 else 'downloaded',Json([{'document_id':doc_id}]),Json([{'document_id':doc_id,'url':source['landing_url'],'version_review':'pending'}]),bfs))
        from reconcile_candidates import reconcile
        reconcile(conn,document_ids=[doc_id])
    from reviews import apply_review
    apply_review(conn,doc_id,sha)
    from reviewed_batches import apply_batch
    apply_batch(conn,doc_id,sha)
    from epalinges_pilot import persist as persist_epalinges
    persist_epalinges(conn,doc_id,sha)
    from epalinges_thematic import persist as persist_epalinges_thematic
    persist_epalinges_thematic(conn,doc_id,sha)
    from aigle_pilot import persist as persist_aigle
    persist_aigle(conn,doc_id,sha)
    from bex_pilot import persist as persist_bex
    persist_bex(conn,doc_id,sha)
    from orbe_pilot import persist as persist_orbe
    persist_orbe(conn,doc_id,sha)
    from rivelac_pilot import persist as persist_rivelac
    persist_rivelac(conn,doc_id,sha)
    from nord_activity_pilot import persist as persist_nord_activity
    persist_nord_activity(conn,doc_id,sha)
    from sdrm_historical_pilot import persist as persist_sdrm_historical
    persist_sdrm_historical(conn,doc_id,sha)
    from prangins_thematic import persist as persist_prangins_thematic
    persist_prangins_thematic(conn,doc_id,sha)
    return doc_id


def run(args):
    sources=json.loads((ROOT/'reviewed_sources.json').read_text())
    args.output.mkdir(parents=True,exist_ok=True)
    conn=psycopg2.connect(os.environ['RE_LLM_DB_URL'],connect_timeout=15) if args.persist else None
    report={'operation_id':str(uuid.uuid4()),'documents':[],'errors':[],'coverage_complete':False}
    try:
        for source in sources:
            try:
                data,archive_evidence=acquire(source);sha=hashlib.sha256(data).hexdigest()
                enumerate_vectors=source.get('enumerate_vectors',True)
                pages=inspect(data,enumerate_vectors=enumerate_vectors,**({'max_pages':source['max_pages']} if 'max_pages' in source else {}))
                if archive_evidence and pages:pages[0]['archive_evidence']=archive_evidence
                if source.get('source_review') and pages:
                    pages[0]['source_review']=source['source_review']
                name=f"{source['commune_bfs']}-{sha[:12]}"
                (args.output/(name+'.pdf')).write_bytes(data)
                (args.output/(name+'-inspection.json')).write_text(json.dumps(pages,ensure_ascii=False))
                doc_id=persist(conn,source,sha,pages) if conn else None
                result={'commune_bfs':source['commune_bfs'],'covered_commune_bfs':coverage_members(source),'document_id':doc_id,'sha256':sha,'pages':len(pages),
                        'vector_pages':sum(p['vector_paths']>0 for p in pages) if enumerate_vectors else None,
                        'vector_inventory_status':'counted' if enumerate_vectors else 'not_evaluated','plan_status':source['plan_status']}
                report['documents'].append(result);print(json.dumps(result),flush=True)
            except Exception as exc:
                if conn:conn.rollback()
                error={'commune_bfs':source['commune_bfs'],'source_url':source['pdf_url'],'error_type':type(exc).__name__}
                response=getattr(exc,'response',None)
                if response is not None:error['http_status']=response.status_code
                report['errors'].append(error)
                print(json.dumps({'source_error':error}),flush=True)
            (args.output/'source-acquisition-report.json').write_text(json.dumps(report,indent=2))
        return report
    finally:
        if conn:conn.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--persist',action='store_true')
    parser.add_argument('--output',type=Path,default=Path('vd-pdcom-output'))
    args=parser.parse_args();report=run(args)
    if report['errors']:raise SystemExit(1)
