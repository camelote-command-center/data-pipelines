import copy
import unittest
import _pipeline_path  # noqa: F401  (adds the pipeline dir to sys.path)
import vevey_equipment as a

class VeveyEquipmentTests(unittest.TestCase):
 def test_exact_reviewed_fixture(self):
  b=a.load_artifacts();assert sum(len(f['source_paths']) for f in b['features'])==12
  assert sum(len(f['expected_pairs']) for f in b['features'])==52
  assert len({a.sector_id(k) for k in a.KEYS})==1
 
 def test_no_source_or_qualification_promotion(self):
  for field,value in [('source_plan_status','unverified'),('source_precision_m',.002),('publication_status','public'),('source_scope_communes',[5884])]:
   b=copy.deepcopy(a.load_artifacts());b[field]=value
   with self.assertRaises(ValueError):a.validate(b)
 
 def test_reserved_control_cannot_move_after_fit(self):
  b=copy.deepcopy(a.load_artifacts());b['frozen_controls']['controls'][-1]['ground_centroid'][0]+=1
  with self.assertRaises(ValueError):a.validate(b)
 
 def test_legend_cannot_be_promoted(self):
  b=copy.deepcopy(a.load_artifacts());b['features'][0]['source_paths'][0]['index']=96160
  with self.assertRaises(ValueError):a.validate(b)
 
 def test_curve_tolerance_cannot_change(self):
  b=copy.deepcopy(a.load_artifacts());b['curve_tolerance_pdfpoint']=.03
  with self.assertRaises(ValueError):a.validate(b)

 def test_outside_hull_source_not_eligible(self):
  b=copy.deepcopy(a.load_artifacts());p=b['features'][0]['source_paths'][0];p['geometry']['coordinates'][0][0][0]-=1000
  with self.assertRaises(ValueError):a.validate(b)
 
 def test_pair_scope_cannot_expand_to_neighbor(self):
  b=copy.deepcopy(a.load_artifacts());b['features'][0]['expected_pairs'][0]['attributes']['NO_COM_FED']=5884
  with self.assertRaises(ValueError):a.validate(b)
 
 def test_unknown_category_rejected(self):
  with self.assertRaises(ValueError):a.sector_id('mixed_hatching')
