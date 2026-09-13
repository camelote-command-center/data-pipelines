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

if __name__=='__main__':unittest.main()
