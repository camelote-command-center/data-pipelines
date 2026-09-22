"""Guard source uncertainties that must survive annual annotation replay."""
import json
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reviewed_batches import ROOT, annotate


def metrics(doc):
    batch = json.loads((ROOT / (doc + '.json')).read_text())
    pages = [{'page_number': n} for n in range(1, batch['page_count'] + 1)]
    return next(x['reviewed_operational_metrics'] for x in annotate(pages, len(pages), batch) if 'reviewed_operational_metrics' in x)


class SDRMReviewContract(unittest.TestCase):
    def test_source_blanks_and_conflicting_totals_survive(self):
        data = metrics('a2386483-8861-5e98-8631-9ea1adb6dca3')
        for row in data['records']:
            if row['site_id'] == 'L1':
                self.assertIsNone(row['stated_capacity'])
            if row['site_id'] in {'L1', 'D7', 'E5'}:
                self.assertIsNone(row['inhabitants_jobs_per_ha'])
        conflicts = {(r['scenario'], r['prefix']): r['difference'] for r in data['commune_sum_audits'] if r['difference']}
        self.assertEqual(conflicts, {(1, 'P'): -1, (1, 'E'): 1})

    def test_candidate_components_do_not_become_available_capacity(self):
        data = metrics('860247ca-7943-5cfd-a948-7f1d70fed5d9')
        by_id = {r['site_id']: r for r in data['rows']}
        for sid in {'D1', 'L11', 'P7', 'M7', 'D7'}:
            self.assertEqual(by_id[sid]['exception'], 'semantic_exclusion_from_available_development_capacity')
        self.assertEqual({r['site_id'] for r in data['rows'] if r.get('area_screen_only') == 'exceeds_5_percent'}, {'D1', 'L10', 'M4', 'P6'})
        for row in data['rows']:
            self.assertFalse(row['delivery_eligible'])
            self.assertEqual(row['geographic_validation'], 'pending')
            if 'component_index' in row:
                props = data['source_boundary_batch']['features'][row['component_index']]['properties']
                self.assertEqual(props['semantic_class'], row['semantic_class'])
                self.assertEqual(props['path_index'], row['source_path_index'])
        self.assertIn('NOT geographic', data['source_boundary_batch']['coordinate_system'])
