import copy,json,unittest
from pathlib import Path
import _pipeline_path  # noqa: F401  (adds the pipeline dir to sys.path)
import vd_pdcom_text as bridge
import mixed_text
ROOT=Path(__file__).resolve().parents[1]
class VeytauxTests(unittest.TestCase):
 def setUp(self):
  self.b=json.loads((ROOT/'reports/veytaux-mixed-text/bundle.json').read_text());self.e=json.loads((ROOT/'reports/veytaux-mixed-text/representations.json').read_text())
 def test_exact18_representations_and_ten_amendments(self):
  self.assertEqual(bridge.assemble(self.b['review'],self.b['pages'],self.b['byte_count']),self.b)
  mixed_text.validate_delivery(self.b['metadata'],self.b['chunks'])
  self.assertEqual(len(self.b['chunks']),18)
  reps=mixed_text.representations(self.e);self.assertEqual(sum(x['method']==mixed_text.SPAN_METHOD for x in reps),8)
  amendments=[x['source_provenance'] for x in reps if x['method'].startswith('separately')]
  self.assertEqual([x['amendment_number'] for x in amendments],list(range(1,11)))
  self.assertEqual([x['target_physical_page'] for x in amendments],[7,8,17,19,21,22,23,24,27,29])
 def test_raw_span_bounds_fail_closed(self):
  for start,end in [(-1,10),(False,10),(0,True),(10,10),(0,100000)]:
   e=copy.deepcopy(self.e);s=e['pages'][19]['raw_ocr_spans'][0];s.update(start=start,end=end)
   with self.assertRaisesRegex(ValueError,'mixed_span_bounds_invalid'):mixed_text.representations(e)
 def test_raw_span_method_or_text_identity_changed(self):
  for field,value in [('method','raw_fullpage_tesseract_ocr'),('text_sha256','0'*64),('end',5)]:
   e=copy.deepcopy(self.e);e['pages'][19]['raw_ocr_spans'][0][field]=value
   with self.assertRaisesRegex(ValueError,'mixed_span_identity_changed'):mixed_text.representations(e)
 def test_full_raw_ocr_hash_required(self):
  e=copy.deepcopy(self.e);e['pages'][19]['raw_ocr_text']+='silent change'
  with self.assertRaisesRegex(ValueError,'mixed_raw_ocr_hash_changed'):mixed_text.representations(e)
 def test_transcript_offsets_and_exact_substring_required(self):
  for field,value in [('transcript_start',-1),('transcript_end',True),('transcript_end',999999),('transcript_start',1)]:
   e=copy.deepcopy(self.e);e['localized_representations'][0][field]=value
   with self.assertRaisesRegex(ValueError,'mixed_transcript_span_'):mixed_text.representations(e)
 def test_prior_checked_in_bundles_unchanged(self):
  n=0
  for path in (ROOT/'reports').rglob('*bundle.json'):
   if 'veytaux-mixed-text' in str(path):continue
   b=json.loads(path.read_text())
   if isinstance(b,dict) and all(k in b for k in ('review','pages','byte_count','chunks')):
    self.assertEqual(bridge.assemble(b['review'],b['pages'],b['byte_count']),b);n+=1
  self.assertGreaterEqual(n,7)

 def test_overlapping_or_duplicate_spans_rejected(self):
  e=copy.deepcopy(self.e);e['pages'][19]['raw_ocr_spans']*=2
  with self.assertRaisesRegex(ValueError,'mixed_span_overlap_or_reorder'):mixed_text.representations(e)
 def test_explicit_wrong_span_text_rejected(self):
  e=copy.deepcopy(self.e);e['pages'][19]['raw_ocr_spans'][0]['text']='wrong'
  with self.assertRaisesRegex(ValueError,'mixed_span_text_changed'):mixed_text.representations(e)

 def test_duplicate_amendment_transcript_rejected(self):
  e=copy.deepcopy(self.e);e['localized_representations'].append(copy.deepcopy(e['localized_representations'][0]))
  with self.assertRaisesRegex(ValueError,'mixed_transcript_overlap_or_duplicate'):mixed_text.representations(e)
 def test_missing_amendment_transcript_rejected(self):
  e=copy.deepcopy(self.e);e['localized_representations'].pop()
  with self.assertRaisesRegex(ValueError,'mixed_transcript_coverage_incomplete'):mixed_text.representations(e)
