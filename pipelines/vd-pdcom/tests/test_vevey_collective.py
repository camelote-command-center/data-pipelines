import copy, unittest
import _pipeline_path  # noqa: F401  (adds the pipeline dir to sys.path)
import vevey_collective as a

class VeveyCollectiveTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls): cls.batch=a.load_artifacts()
 def test_reviewed_scope(self):
  self.assertEqual(len(self.batch['features'][0]['source_paths']),43)
  self.assertEqual(len(self.batch['features'][0]['expected_pairs']),224)
 def test_fail_closed_changes(self):
  mutations=[lambda b:b.update(source_precision_m=.002),lambda b:b.update(publication_status='public'),lambda b:b['literal_semantic_review'].update(qualified=True),lambda b:b['features'][0]['source_paths'][0]['raw_pattern_fill_proof'].update(raw_fill_statement='h\nS\nQ'),lambda b:b['features'][0]['source_paths'][0].update(index=98856),lambda b:b['features'][0]['source_paths'][0].update(source_clip_removed_area_pdfpoint2=100),lambda b:b['features'][0]['expected_pairs'].pop(),lambda b:b['source_native_proof']['native_pattern_resources']['P1'].update(cell_stream='changed')]
  for mutate in mutations:
   with self.subTest(mutation=mutate):
    b=copy.deepcopy(self.batch);mutate(b)
    with self.assertRaises(ValueError):a.validate(b)
 def test_pattern_cell_cannot_replace_original_support(self):
  b=copy.deepcopy(self.batch);p=b['features'][0]['source_paths'][0];op=b['source_native_proof']['native_paint_operators'][str(p['source_paint_operator_ids'][0])];p['index']=op['full_native_ancestry'][-1]
  with self.assertRaises(ValueError):a.validate(b)
 def test_full_ancestry_required(self):
  b=copy.deepcopy(self.batch);p=b['features'][0]['source_paths'][0];op=b['source_native_proof']['native_paint_operators'][str(p['source_paint_operator_ids'][0])];op['full_native_ancestry']=[]
  with self.assertRaises(ValueError):a.validate(b)
