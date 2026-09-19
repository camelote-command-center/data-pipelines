import sys
import unittest
from pathlib import Path
import pymupdf as fitz
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from acquire_sources import inspect


class FullMapTextTest(unittest.TestCase):
    def test_legend_after_dense_parcel_labels_is_preserved(self):
        doc = fitz.open()
        page = doc.new_page(width=1000, height=14000)
        text = "Parcel reference 1234567890\n" * 1800 + "LEGEND: INDICATIVE BOUNDARY ONLY"
        page.insert_text((20, 20), text, fontsize=4)
        expected = page.get_text()
        self.assertGreater(len(expected), 30000)
        self.assertIn("LEGEND: INDICATIVE BOUNDARY ONLY", expected)
        result = inspect(doc.tobytes(), enumerate_vectors=False)
        self.assertEqual(result[0]["text"], expected)
        doc.close()
