import copy,json,unittest
from pathlib import Path
import _pipeline_path  # noqa: F401  (adds the pipeline dir to sys.path)
import prangins_transport_native as adapter

class TransportEvidenceTest(unittest.TestCase):
    def setUp(self):
        p=Path(__file__).resolve().parents[1]/'reports/prangins-transport-maps/native.json'
        if not p.exists():p=Path(__file__).with_name('transport-batch.json')
        self.batch=json.loads(p.read_text())
    def test_frozen_batch(self):
        b=adapter.validate(self.batch)
        self.assertEqual(len(b['features']),6)
        self.assertEqual(sum(len(f['paths']) for f in b['features']),17)
        self.assertTrue(all(f['feature_kind']=='source_native_line' for f in b['features']))
    def test_geometry_cannot_expand_outside_hull(self):
        b=copy.deepcopy(self.batch);b['features'][0]['paths'][0]['geometry_base_pdf']={'type':'LineString','coordinates':[[0,0],[1,1]]}
        with self.assertRaisesRegex(ValueError,'outside_training_hull'):adapter.validate(b)
    def test_no_parcel_link_or_buffers(self):
        for flag in ['parcel_links_allowed','buffer_allowed']:
            b=copy.deepcopy(self.batch);b['semantic_allowlist'][0][flag]=True
            with self.assertRaisesRegex(ValueError,'no_parcels_or_buffer'):adapter.validate(b)
    def test_control_omission_rejected(self):
        b=copy.deepcopy(self.batch);b['ground_control_evidence']['controls'].pop()
        with self.assertRaisesRegex(ValueError,'control_identity'):adapter.validate(b)
    def test_ground_transform_change_rejected(self):
        b=copy.deepcopy(self.batch);b['affine'][4]+=1
        with self.assertRaisesRegex(ValueError,'independent_fit'):adapter.validate(b)
    def test_unsupported_path_added_rejected(self):
        b=copy.deepcopy(self.batch);b['features'][0]['paths'][0]['path_id']='49:232331'
        with self.assertRaisesRegex(ValueError,'source_membership'):adapter.validate(b)
    def test_new_identity_partition_change_rejected(self):
        b=copy.deepcopy(self.batch);r=next(r for r in b['ground_control_evidence']['controls'] if r['identity_source_artifact']!='page49-network-ground-fit.json');r['role']='heldout' if r['role']=='training' else 'training'
        with self.assertRaisesRegex(ValueError,'new_identity_role'):adapter.validate(b)
if __name__=='__main__':unittest.main()
