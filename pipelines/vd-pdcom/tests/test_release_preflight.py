import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from release_preflight import sector_blockers

class ReleasePreflightTests(unittest.TestCase):
    def test_nonfinite_or_nonpositive_precision_blocks(self):
        for value in (float('nan'), float('inf'), -float('inf'), 0, -1, None, True):
            with self.subTest(value=value):
                self.assertIn('source_precision_unresolved', sector_blockers({'source_precision_m': value}))

    def test_private_receiver_parity_does_not_validate_sector(self):
        blockers=sector_blockers({'private_receiver_matches':True,'valid_geometry':True,'plan_status':'approved'})
        self.assertIn('sector_validation_not_recorded',blockers)
        self.assertIn('validator_missing',blockers)
        self.assertIn('source_precision_unresolved',blockers)
        self.assertNotIn('private_receiver_missing_or_different',blockers)

    def test_approved_reserved_source_preserves_spatial_gates(self):
        blockers=sector_blockers({'plan_status':'approved_with_reservation','boundary_review_pairs':1})
        self.assertNotIn('exact_version_approval_unresolved',blockers)
        self.assertIn('parcel_boundary_review_pending',blockers)
        self.assertIn('sector_validation_not_recorded',blockers)

    def test_nominal_validation_does_not_hide_missing_geometry_or_receiver(self):
        blockers=sector_blockers({'review_status':'validated','validated_by':'reviewer','source_precision_m':2,'plan_status':'consultation'})
        self.assertIn('geometry_invalid_or_not_4326',blockers)
        self.assertIn('private_receiver_missing_or_different',blockers)
        self.assertIn('exact_version_approval_unresolved',blockers)

if __name__=='__main__':unittest.main()
