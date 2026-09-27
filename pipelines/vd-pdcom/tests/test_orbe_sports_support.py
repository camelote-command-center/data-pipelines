import copy,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import orbe_sports_support as a
from native_closed_paths import closed_polygon,flatten_cubic
class NativeSupportTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_scope(self):
  self.assertEqual(self.b['features'][0]['path_ids'],[9507]);self.assertEqual(len(self.b['features'][0]['expected_pairs']),7)
 def test_unknown_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_public_rejected(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_curve_mutation(self):self.reject(lambda b:b['source_native_proof']['native_commands'][0][1].__setitem__(0,0))
 def test_coarse_tolerance(self):self.reject(lambda b:b['source_native_proof'].update(curve_tolerance_pdf_points=1))
 def test_source_operator_identity(self):self.reject(lambda b:b['source_native_proof'].update(drawing_index=9506))
 def test_complete_ledger(self):self.reject(lambda b:b['control_identity_ledger']['all_rows'].pop())
 def test_role_mutation(self):self.reject(lambda b:b['control_identity_ledger']['all_rows'][0].update(role='other'))
 def test_convention_explicit(self):self.reject(lambda b:b['control_identity_ledger']['all_rows'][0].update(source_index_convention='extended'))
 def test_decision(self):self.reject(lambda b:b.update(independent_geographic_decision='pending'))
 def test_transform(self):self.reject(lambda b:b['affine'].__setitem__(4,0))
 def test_wrong_object_type(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0]['attributes'].update(GENRE_TXT='unknown'))
 def test_noop_other(self):self.assertIsNone(a.persist(None,a.DOC,'wrong'))
 def test_open_contour_not_closed_implicitly(self):
  with self.assertRaisesRegex(ValueError,'open'):closed_polygon([['l',[0,0],[1,0]],['l',[1,0],[1,1]],['l',[1,1],[0,1]]])
 def test_disconnected_rejected(self):
  with self.assertRaisesRegex(ValueError,'disconnected'):closed_polygon([['l',[0,0],[1,0]],['l',[2,0],[0,0]]])
 def test_line_ring_unchanged(self):
  self.assertEqual(closed_polygon([['l',[0,0],[1,0]],['l',[1,0],[1,1]],['l',[1,1],[0,0]]]).area,.5)
 def test_collinear_overshoot_not_flattened(self):
  self.assertGreater(len(flatten_cubic([0,0],[10,0],[-10,0],[1,0])),2)
if __name__=='__main__':unittest.main()
