import sys
from pathlib import Path
import unittest
from unittest.mock import patch
import pymupdf
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from acquire_sources import inspect

class InspectionMode(unittest.TestCase):
    def fixture(self):
        doc=pymupdf.open();page=doc.new_page();page.insert_text((30,30),'Source text');page.draw_rect((40,40,80,80));return doc.tobytes()

    def test_text_only_preserves_content_without_enumerating_drawings(self):
        data=self.fixture()
        with patch.object(pymupdf.Page,'get_drawings',side_effect=AssertionError('must not enumerate')):
            pages=inspect(data,enumerate_vectors=False)
        self.assertIn('Source text',pages[0]['text'])
        self.assertIsNone(pages[0]['vector_paths'])
        self.assertEqual(pages[0]['vector_inventory_status'],'not_evaluated')

    def test_existing_vector_inventory_remains_default(self):
        pages=inspect(self.fixture())
        self.assertGreater(pages[0]['vector_paths'],0)
        self.assertEqual(pages[0]['vector_inventory_status'],'counted')
