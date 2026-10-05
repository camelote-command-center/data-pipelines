import copy,unittest
import vevey_active_ground_floor as a

class VeveyActiveGroundFloorTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.batch=a.load_artifacts()
 def test_frozen_scope(self):
  self.assertEqual([p['index'] for p in self.batch['features'][0]['source_paths']],[95078,95079])
  self.assertFalse(self.batch['parcel_links_allowed'])
 def test_private_line_limits(self):
  for key,value in [('source_precision_m',.01),('buffer_allowed',True),('parcel_links_allowed',True),('complete_map',True),('publication_status','public')]:
   b=copy.deepcopy(self.batch);b[key]=value
   with self.assertRaises(ValueError):a.validate(b)
 def test_no_legend_or_held_path_promotion(self):
  for idx in [95080,95081,96164]:
   b=copy.deepcopy(self.batch);b['features'][0]['source_paths'][0]['index']=idx
   with self.assertRaises(ValueError):a.validate(b)
 def test_native_stroke_and_ancestry_preserved(self):
  for field,value in [('dashes','[1 1] 0'),('stroke_opacity',.5),('width',2.)]:
   b=copy.deepcopy(self.batch);b['features'][0]['source_paths'][0]['native_operator'][field]=value
   with self.assertRaises(ValueError):a.validate(b)
  b=copy.deepcopy(self.batch);b['features'][0]['source_paths'][0]['clip_identity']['full_native_ancestry']=[]
  with self.assertRaises(ValueError):a.validate(b)
 def test_whole_source_and_accounting_preserved(self):
  b=copy.deepcopy(self.batch);b['features'][0]['source_paths'][0]['native_items'][0][1][0]+=1
  with self.assertRaises(ValueError):a.validate(b)
  b=copy.deepcopy(self.batch);b['source_native_proof']['candidates'].pop()
  with self.assertRaises(ValueError):a.validate(b)
 def test_mask_state_cannot_be_assumed_clear(self):
  b=copy.deepcopy(self.batch);b['source_native_proof']['raw_graphics_state_proof']['records'][0]['graphics_state_before_source_q']['SMask']='unknown'
  with self.assertRaises(ValueError):a.validate(b)
