import copy,unittest
import vevey_historic as historic,vevey_mixed as mixed

class VeveyHistoricMixedTests(unittest.TestCase):
 def test_frozen_scope(self):
  for a,n,pairs in [(historic,367,369),(mixed,333,508)]:
   b=a.load_artifacts();self.assertEqual(len(b['features'][0]['source_paths']),n);self.assertEqual(len(b['features'][0]['expected_pairs']),pairs)
 def test_no_qualification_or_public_release(self):
  for a in [historic,mixed]:
   for field,value in [('source_precision_m',.002),('publication_status','public'),('source_plan_status','draft'),('source_scope_communes',[5884])]:
    b=copy.deepcopy(a.load_artifacts());b[field]=value
    with self.assertRaises(ValueError):a.validate(b)
 def test_frozen_control_identity(self):
  for a in [historic,mixed]:
   b=copy.deepcopy(a.load_artifacts());b['frozen_controls']['controls'][-1]['ground_centroid'][0]+=1
   with self.assertRaises(ValueError):a.validate(b)
 def test_legend_cannot_be_policy_area(self):
  for a,idx in [(historic,96146),(mixed,98845)]:
   b=copy.deepcopy(a.load_artifacts());b['features'][0]['source_paths'][0]['index']=idx
   with self.assertRaises(ValueError):a.validate(b)
 def test_source_contour_cannot_be_adjusted(self):
  for a in [historic,mixed]:
   b=copy.deepcopy(a.load_artifacts());b['features'][0]['source_paths'][0]['geometry']['coordinates'][0][0][0]-=100
   with self.assertRaises(ValueError):a.validate(b)
 def test_clip_ancestry_preserved(self):
  for a in [historic,mixed]:
   b=copy.deepcopy(a.load_artifacts());p=b['features'][0]['source_paths'][0];p['source_clip_removed_area_pdfpoint2']+=1
   with self.assertRaises(ValueError):a.validate(b)
 def test_no_extra_parcel_membership(self):
  for a in [historic,mixed]:
   b=copy.deepcopy(a.load_artifacts());b['features'][0]['expected_pairs'].pop()
   with self.assertRaises(ValueError):a.validate(b)
 def test_manual_colour_discrepancy_remains_disclosed(self):
  b=copy.deepcopy(historic.load_artifacts());b['literal_semantic_review']['semantic_mapping']['not_exact_rgb_match']=False
  with self.assertRaises(ValueError):historic.validate(b)
 def test_pattern_stroke_cannot_replace_fill(self):
  b=copy.deepcopy(mixed.load_artifacts());b['features'][0]['source_paths'][0]['raw_pattern_fill_proof']['raw_fill_statement']='h\nS\nQ'
  with self.assertRaises(ValueError):mixed.validate(b)
 def test_pattern_resource_review_cannot_remain_pending(self):
  b=copy.deepcopy(mixed.load_artifacts());b['source_native_proof']['native_pattern_resources']['interpretation_pending']=True
  with self.assertRaises(ValueError):mixed.validate(b)
 def test_half_pattern_cannot_be_promoted(self):
  b=copy.deepcopy(mixed.load_artifacts());p=b['features'][0]['source_paths'][0];p['full_pattern_native_components']=[x for x in p['full_pattern_native_components'] if x.get('fill')!=[.5405203104019165,.2225680947303772,.5856565237045288]]
  with self.assertRaises(ValueError):mixed.validate(b)
 def test_topology_holds_cannot_disappear(self):
  for a in [historic,mixed]:
   b=copy.deepcopy(a.load_artifacts());b['source_native_proof']['candidates']=[p for p in b['source_native_proof']['candidates'] if p['disposition']!='native_topology_hold']
   with self.assertRaises(ValueError):a.validate(b)
