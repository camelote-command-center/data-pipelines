import copy,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import lutry_cartographic as a
class LutryCartographicTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_scope(self):self.assertEqual(len(self.b['features'][0]['expected_pairs']),32)
 def test_unknown_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_no_public(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_source_reservations(self):self.reject(lambda b:b.update(source_plan_status='approved'))
 def test_applicability_unresolved(self):self.reject(lambda b:b.update(reservation_applicability='cleared'))
 def test_no_exact_policy(self):self.reject(lambda b:b['representation'].update(exact_policy_extent=True))
 def test_no_gap_join(self):self.reject(lambda b:b['representation'].update(gaps_joined=True))
 def test_notch_retained(self):self.reject(lambda b:b['representation'].update(green_notch_preserved=False))
 def test_original_roles(self):self.reject(lambda b:b['frozen_grid']['points'][4].update(split='train'))
 def test_fixed_pixels(self):self.reject(lambda b:b['frozen_grid']['points'][0]['pixel'].__setitem__(0,1096))
 def test_original_trace(self):self.reject(lambda b:b['trace']['geometry_pixels']['coordinates'][0][0].__setitem__(0,100))
 def test_datum(self):self.reject(lambda b:b['trace']['official_reframe_responses'][0]['lv95'].__setitem__(0,0))
 def test_no_global_accuracy(self):self.reject(lambda b:b['alignment'].update(holdout_rmse_m=0))
 def test_reference_kind(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0]['attributes'].update(GENRE_TXT='unknown'))
 def test_reference_geometry(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0]['reference_geometry'].update(coordinates=[[[0,0],[1,0],[1,1],[0,0]]]))
 def test_noop_other(self):self.assertIsNone(a.persist(None,a.DOC,'wrong'))
if __name__=='__main__':unittest.main()
