import copy,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import saint_legier_raster as a
class RasterSupportTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_scope(self):self.assertEqual(len(self.b['features'][0]['expected_pairs']),23)
 def test_unknown_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_public_rejected(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_predecessor_only(self):self.reject(lambda b:b.update(predecessor_scope_only=False))
 def test_no_morphology(self):self.reject(lambda b:b['raster_representation'].update(morphology='close'))
 def test_notch_retained(self):self.reject(lambda b:b['raster_representation'].update(yellow_notch_preserved=False))
 def test_holes_explicit(self):self.reject(lambda b:b['raster_representation']['omitted_interior_color_holes'].update(east=0))
 def test_uncertain_edges(self):self.reject(lambda b:b['raster_representation'].update(all_edges_uncertain=False))
 def test_grid_full(self):self.reject(lambda b:b['frozen_grid']['controls'].pop())
 def test_unresolved_landmarks_retained(self):self.reject(lambda b:b['landmark_review'].pop(1))
 def test_conversion_replay(self):self.reject(lambda b:b['conversion_replay']['checks'].pop())
 def test_transform(self):self.reject(lambda b:b['grid_fit']['matrix'][2].__setitem__(0,0))
 def test_large_simplification(self):self.reject(lambda b:b['raster_representation'].update(simplification_epsilon_base_pixels=20))
 def test_reference_type(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0]['attributes'].update(GENRE_TXT='unknown'))
 def test_noop_other(self):self.assertIsNone(a.persist(None,a.DOC,'wrong'))
if __name__=='__main__':unittest.main()
