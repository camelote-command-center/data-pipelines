import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import candidate_batch as batch

class CandidateBatchTests(unittest.TestCase):
    def test_scanned_pdf_retains_ocr_need_and_preview(self):
        import pymupdf,json
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'scan.pdf'
            with pymupdf.open() as doc:
                doc.new_page();doc.save(path)
            batch.extract(path);pages=json.loads(path.with_suffix('.json').read_text())
            self.assertTrue(pages[0]['needs_ocr'])
            self.assertTrue((path.parent/pages[0]['preview']).exists())

    def test_timeout_is_durable_failure_not_promotion(self):
        row=dict(commune_bfs=5805,source_url='https://example.org/plan.pdf',evidence_url='https://example.org',link_text='PDCom')
        with tempfile.TemporaryDirectory() as tmp,patch('candidate_batch.download',return_value=b'%PDF-test'),patch('candidate_batch.subprocess.run',side_effect=subprocess.TimeoutExpired('extract',120)):
            result=batch.process(row,Path(tmp))
        self.assertEqual(result['status'],'extraction_failed')
        self.assertEqual(result['review_status'],'pending')
        self.assertNotIn('pages',result)
