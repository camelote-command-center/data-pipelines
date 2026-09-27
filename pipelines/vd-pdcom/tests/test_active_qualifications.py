import copy
from datetime import date
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_release_manifest as fixtures
from active_qualifications import project
from release_manifest import validate

TODAY = date(2026, 9, 22)

class ActiveQualificationTests(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.ReleaseManifestTests()
        fixture.setUp()
        self.sector, self.manifest = fixture.sector, fixture.m
        self.row = {k: self.manifest[k] for k in
                    ('sector_id', 'document_id', 'source_sha256', 'geometry_sha256')}
        self.row.update(id=self.manifest['manifest_id'], manifest=self.manifest,
                        manifest_sha256=validate(self.manifest, self.sector, TODAY))

    def run_projection(self):
        return project([self.sector], [self.row], TODAY)

    def test_valid_contract_is_only_sector_eligibility_without_mutation(self):
        before = copy.deepcopy((self.sector, self.row))
        result = self.run_projection()
        self.assertEqual(result['counts']['eligible_sectors'], 1)
        for key in ('publication_authorized', 'parcel_release', 'commune_completion_authorized'):
            self.assertIs(result[key], False)
        self.assertEqual(before, (self.sector, self.row))

    def test_no_record_is_explicitly_ineligible(self):
        result = project([self.sector], [], TODAY)
        self.assertEqual(result['counts']['eligible_sectors'], 0)
        self.assertEqual(result['sectors'][0]['blocking_gates'], ['qualification_not_recorded'])

    def test_stale_source_geometry_status_or_supersession_fail_closed(self):
        for key, value in [('source_sha256', 'c'*64), ('geometry_sha256', 'c'*64),
                           ('plan_status', 'consultation'), ('review_status', 'superseded')]:
            with self.subTest(key=key):
                sector = {**self.sector, key: value}
                self.assertEqual(project([sector], [self.row], TODAY)['counts']['eligible_sectors'], 0)

    def test_expiration_is_rechecked_without_changing_manifest(self):
        result = project([self.sector], [self.row], date(2026, 10, 21))
        self.assertIn('review_not_current', result['manifests'][0]['blocking_gates'])
        self.assertEqual(result['counts']['eligible_sectors'], 0)

    def test_stored_digest_and_denormalized_identity_are_verified(self):
        for key, value in [('manifest_sha256', 'c'*64), ('source_sha256', 'c'*64),
                           ('id', '00000000-0000-0000-0000-000000000099')]:
            with self.subTest(key=key):
                result = project([self.sector], [{**self.row, key: value}], TODAY)
                self.assertEqual(result['counts']['eligible_sectors'], 0)

    def test_changed_body_with_old_digest_is_rejected(self):
        self.manifest['reviewer'] = 'Different named reviewer'
        self.assertIn('manifest_sha256_mismatch', self.run_projection()['manifests'][0]['blocking_gates'])

    def test_live_negative_currentness_cannot_be_overridden(self):
        self.sector['validation_evidence'] = {'municipal_currentness': {
            'classification': 'historical_not_current_municipal_reference'}}
        result = self.run_projection()
        self.assertEqual(result['counts']['eligible_sectors'], 0)
        self.assertIn('live_source_not_current_municipal_reference', result['sectors'][0]['blocking_gates'])
        self.sector['validation_evidence']['municipal_currentness']['classification'] = 'new_undefined_status'
        self.assertIn('live_currentness_policy_reconciliation_required', self.run_projection()['sectors'][0]['blocking_gates'])

    def test_new_valid_record_does_not_erase_expired_record_evidence(self):
        newer = copy.deepcopy(self.row)
        newer['id'] = newer['manifest']['manifest_id'] = '00000000-0000-0000-0000-000000000004'
        newer['manifest']['valid_until'] = '2026-11-20'
        newer['manifest_sha256'] = validate(newer['manifest'], self.sector, TODAY)
        result = project([self.sector], [newer, self.row], date(2026, 10, 21))
        self.assertEqual(result['counts']['eligible_sectors'], 1)
        self.assertEqual(result['counts']['eligible_manifests'], 1)
        self.assertEqual(result['sectors'][0]['active_manifest_ids'], [newer['id']])
        self.assertEqual(len(result['manifests']), 2)
        self.assertIn('review_not_current', result['manifests'][0]['blocking_gates'])

    def test_malformed_live_evidence_fails_closed(self):
        self.sector['validation_evidence'] = []
        self.assertEqual(self.run_projection()['counts']['eligible_sectors'], 0)

    def test_missing_sector_or_malformed_record_does_not_qualify(self):
        result = project([], [self.row], TODAY)
        self.assertIn('sector_missing', result['manifests'][0]['blocking_gates'])
        self.row['manifest'] = None
        self.assertEqual(self.run_projection()['counts']['eligible_sectors'], 0)

if __name__ == '__main__':
    unittest.main()
