import copy,unittest
import _pipeline_path  # noqa: F401  (adds the pipeline dir to sys.path)
import vevey_mobility as a
class VeveyMobilityTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def test_reviewed_batch(self):
  self.assertEqual(sum(len(f['source_paths']) for f in self.b['features']),54)
  self.assertEqual(len({a.sector_id(k) for k in a.PATHS}),7)
 def reject(self,edit):
  b=copy.deepcopy(self.b);edit(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_precision(self):self.reject(lambda b:b.update(source_precision_m=.002))
 def test_status(self):self.reject(lambda b:b.update(source_plan_status='unverified'))
 def test_no_links(self):self.reject(lambda b:b.update(parcel_links_allowed=True))
 def test_partition(self):self.reject(lambda b:b['frozen_controls']['controls'][0].update(role='held'))
 def test_no_source_join(self):self.reject(lambda b:b['features'][0]['source_paths'][0]['native_items'][0][1].__setitem__(0,0))
 def test_wrong_clip(self):self.reject(lambda b:b['features'][0]['source_paths'][0].update(source_visible_geometry={'type':'LineString','coordinates':[[0,0],[1,1]]}))
 def test_closure_not_solid(self):
  f=next(f for f in self.b['features'] if f['key']=='modal_distribution_possible_motor_closure')
  self.assertEqual([p['index'] for p in f['source_paths']],[61332])
  self.reject(lambda b:next(f for f in b['features'] if f['key']=='modal_distribution_possible_motor_closure')['source_paths'][0].update(dashes='[] 0'))
 def test_no_legend_promotion(self):self.reject(lambda b:b['features'][0]['source_paths'][0].update(index=61788))
 def test_unrelated_source_noop(self):self.assertIsNone(a.persist(None,'unrelated',a.SHA))
if __name__=='__main__':unittest.main()
