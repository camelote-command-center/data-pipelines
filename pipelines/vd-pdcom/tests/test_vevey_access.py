import copy,unittest
import vevey_access as a
class VeveyAccessTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,edit):
  b=copy.deepcopy(self.b);edit(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_reviewed_symbol_footprints(self):
  self.assertEqual(len(self.b['features'][0]['source_paths']),211)
  self.assertEqual(len(a.PATHS),1)
 def test_no_parcel_association(self):self.reject(lambda b:b.update(parcel_links_allowed=True))
 def test_no_precision_claim(self):self.reject(lambda b:b.update(source_precision_m=.002))
 def test_no_refit(self):self.reject(lambda b:b['fit']['offset'].__setitem__(0,1))
 def test_no_topology_hold_promotion(self):self.reject(lambda b:b['features'][0]['source_paths'][0].update(index=61050))
 def test_no_manufactured_closure(self):self.reject(lambda b:b['features'][0]['source_paths'][0]['native_items'].pop())
 def test_no_new_area(self):
  self.reject(lambda b:b['features'][0]['geometry_lv95']['coordinates'][0][0][0].__setitem__(0,0))
 def test_literal_fill(self):self.reject(lambda b:b['features'][0]['source_paths'][0].update(fill=[1,1,0]))
 def test_unrelated_noop(self):self.assertIsNone(a.persist(None,'unrelated',a.SHA))
if __name__=='__main__':unittest.main()
