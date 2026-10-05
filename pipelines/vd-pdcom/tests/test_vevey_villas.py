import copy, unittest
import vevey_villas as a

class VeveyVillasTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls): cls.batch=a.load_artifacts()
 def test_reviewed_green_scope(self):
  self.assertEqual(len(self.batch['features'][0]['source_paths']),52)
  self.assertEqual(len(self.batch['features'][0]['expected_pairs']),430)
  self.assertFalse(self.batch['literal_semantic_review']['orange_potential_overlay_included'])
 def test_fail_closed_changes(self):
  mutations=[lambda b:b.update(source_precision_m=.002),lambda b:b.update(publication_status='public'),lambda b:b['literal_semantic_review'].update(orange_potential_overlay_included=True),lambda b:b['literal_semantic_review'].update(qualified=True),lambda b:b['features'][0]['source_paths'][0]['raw_pattern_fill_proof'].update(raw_fill_statement='h\nS\nQ'),lambda b:b['features'][0]['source_paths'][0].update(index=99727),lambda b:b['features'][0]['source_paths'][0].update(source_clip_removed_area_pdfpoint2=100),lambda b:b['features'][0]['expected_pairs'].pop(),lambda b:b['source_native_proof']['native_pattern_resources']['P0'].update(cell_stream='changed')]
  for mutate in mutations:
   with self.subTest(mutation=mutate):
    b=copy.deepcopy(self.batch);mutate(b)
    with self.assertRaises(ValueError):a.validate(b)
 def test_held_supports_cannot_disappear(self):
  b=copy.deepcopy(self.batch);b['source_native_proof']['candidates']=[x for x in b['source_native_proof']['candidates'] if x['disposition']!='native_topology_hold']
  with self.assertRaises(ValueError):a.validate(b)
 def test_white_underpaint_required(self):
  b=copy.deepcopy(self.batch);p=b['features'][0]['source_paths'][0];p['source_paint_operator_ids']=[i for i in p['source_paint_operator_ids'] if b['source_native_proof']['native_paint_operators'][str(i)]['native_operator']['fill']!=[1.,1.,1.]]
  with self.assertRaises(ValueError):a.validate(b)
 def test_green_opacity_preserved(self):
  b=copy.deepcopy(self.batch);p=b['features'][0]['source_paths'][0]
  for i in p['source_paint_operator_ids']:
   op=b['source_native_proof']['native_paint_operators'][str(i)]['native_operator']
   if op['fill']!=[1.,1.,1.]:op['fill_opacity']=1.
  with self.assertRaises(ValueError):a.validate(b)
 def test_full_ancestry_required(self):
  b=copy.deepcopy(self.batch);p=b['features'][0]['source_paths'][0];op=b['source_native_proof']['native_paint_operators'][str(p['source_paint_operator_ids'][0])];op['full_native_ancestry']=[]
  with self.assertRaises(ValueError):a.validate(b)
