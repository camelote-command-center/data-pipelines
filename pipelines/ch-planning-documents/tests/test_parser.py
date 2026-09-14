import io
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import sources
import parser

class SourceContracts(unittest.TestCase):
    def test_multilingual_interlis_and_missing_url(self):
        xml='''<TRANSFER xmlns="http://www.interlis.ch/INTERLIS2.3"><DATASECTION><X>
        <PlansDAffectation_V1_2.DispositionsJuridiques.Document TID="a"><Titre><Multi><Local><Language>fr</Language><Text>Règlement</Text></Local></Multi></Titre><SeulementCommune>2321</SeulementCommune><StatutJuridique>enVigueur</StatutJuridique></PlansDAffectation_V1_2.DispositionsJuridiques.Document>
        <Nutzungsplanung_V1_2.Rechtsvorschriften.Dokument TID="b"><Titel><Multi><Local><Language>de</Language><Text>Bauordnung</Text></Local></Multi></Titel><TextImWeb><Multi><Local><Language>de</Language><Text>https://example.org/doc.pdf</Text></Local></Multi></TextImWeb><Rechtsstatus>AenderungMitVorwirkung</Rechtsstatus></Nutzungsplanung_V1_2.Rechtsvorschriften.Dokument>
        </X></DATASECTION></TRANSFER>'''
        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            with zipfile.ZipFile(f.name,'w') as z:z.writestr('sample.xtf',xml)
            rows=sources.parse_xtf_zip(f.name,'FR','https://example.org/data.zip')
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0]['commune_bfs'],2321)
        self.assertIsNone(rows[0]['document_url'])
        self.assertEqual(rows[1]['document_url'],'https://example.org/doc.pdf')
        self.assertEqual(rows[1]['legal_status'],'AenderungMitVorwirkung')
        self.assertEqual(rows[1]['title'],'Bauordnung')
    def test_zh_uses_bfs_label_and_docid_not_internal_ids(self):
        html='''<select name="gemeinden[id][]"><option value="38">Zürich (261)</option></select>
        <table><tbody><tr id="document_row_999"><td><a href="/documents/999">123</a></td><td><a href="/getDoc?docid=123">pdf</a></td><td>Nutzungsplanung</td><td>RV</td><td data-table-sort-value="38">Zürich</td><td>BZO</td><td>aufgehoben</td><td>01.01.2020</td><td>2020</td><td>2</td></tr></tbody></table>'''
        row=sources.parse_zh_listing(html)[0]
        self.assertEqual(row['commune_bfs'],261)
        self.assertEqual(row['source_key'],'123')
        self.assertEqual(row['legal_status'],'aufgehoben')
        self.assertIn('docid=123',row['document_url'])
    def test_empty_zh_fails_loudly(self):
        with self.assertRaises(ValueError):sources.parse_zh_listing('<html>maintenance</html>')
    def test_chunks_preserve_page_numbers_and_all_text(self):
        text='a'*4500
        chunks=parser.chunk_pages([{'page_number':3,'text':text},{'page_number':4,'text':'b'}])
        self.assertEqual([x[0] for x in chunks],[3,3,3,4])
        self.assertEqual(''.join(t for _,t in chunks),text+'b')
    def test_pdf_text_sanitizes_postgres_incompatible_characters(self):
        self.assertEqual(parser.clean_text('abc\x00déf'), 'abcdéf')
    def test_dashboard_dispatch_log_is_adopted(self):
        monitor=object.__new__(parser.Monitor)
        monitor.dataset={'id':'dataset','startup_id':'owner'}
        calls=[]
        def call(method,table,**kwargs):
            calls.append((method,kwargs));return [{'id':'dispatch-log'}]
        monitor.call=call
        monitor.begin('new-run','https://github.com/test/actions/runs/1')
        self.assertEqual(monitor.log_id,'dispatch-log')
        self.assertEqual([x[0] for x in calls],['GET','PATCH'])
    def test_failure_restores_last_verified_success(self):
        monitor=object.__new__(parser.Monitor)
        monitor.code=parser.CODE
        monitor.dataset={'id':'dataset','startup_id':'owner'}
        monitor.last_success='2025-09-15T00:00:00+00:00'
        calls=[]
        def call(method,table,**kwargs):
            calls.append((table,kwargs));return [{'id':'dataset'}]
        monitor.call=call
        monitor.finish('run',{'catalogued':12},False)
        self.assertEqual(calls[0][1]['json']['status'],'failed')
        self.assertEqual(calls[1][1]['json']['last_acquired_at'],monitor.last_success)
        self.assertNotIn('last_db_update_at',calls[1][1]['json'])
    def test_private_url_rejected(self):
        for url in ['file:///etc/passwd','http://localhost/a','https://127.0.0.1/a','http://u:p@example.org/a']:
            with self.assertRaises(ValueError):parser.safe_url(url)
    def test_scan_keeps_page_and_requires_ocr(self):
        from pypdf import PdfWriter
        w=PdfWriter();w.add_blank_page(width=100,height=100);stream=io.BytesIO();w.write(stream)
        with patch.object(parser,'fetch',return_value=(stream.getvalue(),'https://example.org/a','application/pdf')):
            r=parser.extract('https://example.org/a')
        self.assertEqual(r['extraction_status'],'needs_ocr')
        self.assertEqual(r['pages'][0]['page_number'],1)
    def test_untrusted_html_never_promoted_as_regulation(self):
        with patch.object(parser,'fetch',return_value=(b'<main>'+b'a'*400+b'</main>','https://example.org','text/html')):
            self.assertEqual(parser.extract('https://example.org')['extraction_status'],'html_unverified')

class BoundedRuns(unittest.TestCase):
    def test_budget_stops_new_work_then_hard_limit(self):
        t=[0.0];b=parser.Budget(10,grace_minutes=5,clock=lambda:t[0])
        self.assertTrue(b.accepting());self.assertFalse(b.hard_expired())
        t[0]=10*60;self.assertFalse(b.accepting());self.assertFalse(b.hard_expired())
        t[0]=15*60;self.assertTrue(b.hard_expired())
        self.assertLessEqual(parser.Budget(10,grace_minutes=5,clock=lambda:0.0).document_deadline(),15*60)
    def test_unbounded_budget_never_stops(self):
        b=parser.Budget(0);self.assertTrue(b.accepting());self.assertFalse(b.hard_expired())
    def test_bounded_map_stops_submitting_when_budget_closes(self):
        import concurrent.futures
        state={'open':True};seen=[]
        def fn(x):
            seen.append(x)
            if len(seen)>=2:state['open']=False
            return x
        submitted=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            out=list(parser.bounded_map(pool,fn,range(10),concurrency=1,accepting=lambda:state['open'],submitted=submitted))
        self.assertEqual(out,[0,1]);self.assertEqual(submitted,[2])
    def test_document_deadline_is_enforced(self):
        from pypdf import PdfWriter
        w=PdfWriter();w.add_blank_page(width=100,height=100);stream=io.BytesIO();w.write(stream)
        with patch.object(parser,'fetch',return_value=(stream.getvalue(),'https://example.org/a','application/pdf')):
            with self.assertRaises(ValueError) as ctx:parser.extract('https://example.org/a',deadline=0)
        self.assertEqual(str(ctx.exception),'document_time_limit')
    def test_partial_run_is_neither_success_nor_error(self):
        monitor=object.__new__(parser.Monitor)
        monitor.code='ch_planning_document_text'
        monitor.dataset={'id':'dataset','startup_id':'owner'}
        monitor.last_success=None
        calls=[]
        def call(method,table,**kwargs):
            calls.append((table,kwargs));return [{'id':'dataset'}]
        monitor.call=call
        monitor.finish('run',{'catalogued':12},False,partial=True)
        self.assertEqual(calls[0][1]['json']['status'],'partial')
        self.assertIsNone(calls[0][1]['json']['error_message'])
        self.assertEqual(calls[1][1]['json']['status'],'active')
        self.assertIsNone(calls[1][1]['json']['last_error'])
        self.assertIsNone(calls[1][1]['json']['last_acquired_at'])
        self.assertNotIn('last_db_update_at',calls[1][1]['json'])
    def test_ocr_page_timeout_keeps_document_as_needs_ocr(self):
        import subprocess
        from pypdf import PdfWriter
        w=PdfWriter();w.add_blank_page(width=100,height=100);w.add_blank_page(width=100,height=100)
        for p in w.pages:
            from pypdf.generic import NameObject, DecodedStreamObject
            s=DecodedStreamObject();s.set_data(b'q Q');p[NameObject('/Contents')]=w._add_object(s)
        stream=io.BytesIO();w.write(stream)
        def slow(*a,**k):raise subprocess.TimeoutExpired(cmd=a[0],timeout=k.get('timeout'))
        with patch.object(parser,'fetch',return_value=(stream.getvalue(),'https://example.org/a','application/pdf')), \
             patch.dict('os.environ',{'PLANNING_OCR':'1'}), patch.object(parser.shutil,'which',return_value='/usr/bin/x'), \
             patch.object(parser.subprocess,'run',side_effect=slow):
            r=parser.extract('https://example.org/a')
        self.assertEqual(r['extraction_status'],'needs_ocr')
        self.assertEqual(len(r['pages']),2)
        self.assertEqual({p['method'] for p in r['pages']},{'tesseract-timeout-v1'})
    def test_partial_never_overrides_complete(self):
        monitor=object.__new__(parser.Monitor)
        monitor.code='ch_planning_document_text';monitor.dataset={'id':'d','startup_id':'o'};monitor.last_success=None
        calls=[];monitor.call=lambda m,t,**k:(calls.append((t,k)) or [{'id':'d'}])
        monitor.finish('run',{},True,partial=True)
        self.assertEqual(calls[0][1]['json']['status'],'success')

if __name__=='__main__':unittest.main()
