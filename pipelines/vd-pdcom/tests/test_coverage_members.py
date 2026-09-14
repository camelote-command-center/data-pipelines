import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from acquire_sources import coverage_members
class CoverageMembers(unittest.TestCase):
    def test_shared_plan_keeps_every_commune(self):
        self.assertEqual(coverage_members({'commune_bfs':5624,'commune_bfs_list':[5583,5624,5635],'scope':'intercommunal'}),[5583,5624,5635])
    def test_single_plan_cannot_silently_claim_other_communes(self):
        with self.assertRaisesRegex(ValueError,'intercommunal_scope'):
            coverage_members({'commune_bfs':5624,'commune_bfs_list':[5624,5635],'scope':'communal'})
    def test_duplicate_members_rejected(self):
        with self.assertRaisesRegex(ValueError,'invalid_commune_coverage'):
            coverage_members({'commune_bfs':5624,'commune_bfs_list':[5624,5624],'scope':'intercommunal'})
