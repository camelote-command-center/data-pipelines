import sys
from pathlib import Path
import unittest
import pymupdf
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import acquire_sources

class SourcePageLimit(unittest.TestCase):
    def test_large_reviewed_compilation_needs_explicit_limit(self):
        with pymupdf.open() as doc:
            for _ in range(501):doc.new_page(width=20,height=20)
            data=doc.tobytes()
        with self.assertRaisesRegex(ValueError,'page_limit'):
            acquire_sources.inspect(data,enumerate_vectors=False)
        pages=acquire_sources.inspect(data,enumerate_vectors=False,max_pages=800)
        self.assertEqual(len(pages),501)
        self.assertEqual(pages[-1]['page_number'],501)
        self.assertTrue(all(p['vector_inventory_status']=='not_evaluated' for p in pages))

    def test_limit_is_bounded_integer(self):
        for limit in [0,-1,1001,True,'800',None]:
            with self.assertRaisesRegex(ValueError,'invalid_page_limit'):
                acquire_sources.inspect(b'',max_pages=limit)

    def test_pdf_nul_glyph_remains_visible_and_jsonb_safe(self):
        from unittest.mock import patch
        with pymupdf.open() as doc:
            doc.new_page()
            data=doc.tobytes()
        with patch.object(pymupdf.Page,'get_text',return_value='Mesures\x00 de planification'):
            page=acquire_sources.inspect(data,enumerate_vectors=False)[0]
        self.assertEqual(page['text'],'Mesures\ufffd de planification')
        self.assertEqual(page['nul_replacements'],1)
