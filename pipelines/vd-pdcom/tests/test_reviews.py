import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from reviews import matching_review

class ReviewContracts(unittest.TestCase):
    def test_changed_bytes_do_not_inherit_approval(self):
        self.assertIsNone(matching_review('8f42da46-9b59-5b72-aa5b-13522e418682','changed'))
    def test_reservation_travels_with_approval(self):
        review=matching_review('8f42da46-9b59-5b72-aa5b-13522e418682','bd095d80d2284073a93ef7e108f89b083d32fb361c97b689809522b7b8f64c86')
        self.assertEqual(review['plan_status'],'approved_with_reservation')
        self.assertIn('redimensionnement',review['reservation'])
        self.assertEqual(review['receiver_release'],'not_ready')
        self.assertEqual([p['page_number'] for p in review['map_pages']],[99,100,101,102])
