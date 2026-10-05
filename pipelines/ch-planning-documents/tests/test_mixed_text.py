import copy,json,unittest
from pathlib import Path
import vd_pdcom_text as bridge
import mixed_text
ROOT=Path(__file__).resolve().parents[1]
class MixedTextTests(unittest.TestCase):
 def setUp(self):
  self.b=json.loads((ROOT/'reports/pully-mixed-text/bundle.json').read_text());self.r=self.b['review']
 def test_exact_bundle_and_representations(self):
  self.assertEqual(bridge.assemble(self.r,self.b['pages'],self.b['byte_count']),self.b)
  mixed_text.validate_delivery(self.b['metadata'],self.b['chunks'])
  self.assertEqual(len(self.b['chunks']),70)
  self.assertEqual(sum(len(p['representations']) for p in self.b['pages']),56)
  self.assertEqual(len(bridge.page_extraction_report(self.b)['withheld_mixed_pages']),21)
  methods=[c['metadata']['representation']['method'] for c in self.b['chunks']]
  self.assertEqual(methods.count('separately_labeled_assistant_image_grounded_transcription'),2)
  self.assertEqual(methods.count('raw_tesseract_localized_recovery'),2)
  self.assertFalse(self.r['signed_approval']['handwritten_signatures_observed'])
 def test_page_or_representation_tampering_rejected(self):
  for change in ('held','method','approval','text','drop'):
   pages=copy.deepcopy(self.b['pages'])
   if change=='held':pages[0]['text']='injected map text'
   elif change=='drop':pages[20]['representations'].pop()
   else:
    rep=pages[20]['representations'][-1]
    if change=='method':rep['method']='raw_fullpage_tesseract_ocr'
    elif change=='approval':rep['approval_class']='white_nonapproved_context'
    else:rep['text']='silently corrected'
   with self.assertRaisesRegex(ValueError,'mixed_page_or_representation_changed'):bridge.assemble(self.r,pages,self.b['byte_count'])
 def test_future_reserved_or_amendment_mode_rejected(self):
  for field,value in [('source_role','approved_amendment'),('plan_status','approved_with_reservation'),('scope','intercommunal'),('native_page_selection',{'partial':True})]:
   r=copy.deepcopy(self.r);r[field]=value
   with self.assertRaisesRegex(ValueError,'mixed_'):bridge.validate_review(r)
 def test_delivery_missing_duplicate_order_identity_rejected(self):
  for change in ('missing','duplicate','index','id'):
   chunks=copy.deepcopy(self.b['chunks'])
   if change=='missing':chunks.pop()
   elif change=='duplicate':chunks.append(copy.deepcopy(chunks[-1]))
   elif change=='index':chunks[0]['chunk_index']=1
   else:chunks[0]['id']='wrong'
   with self.assertRaisesRegex(ValueError,'mixed_delivery_sequence'):mixed_text.validate_delivery(self.b['metadata'],chunks)
 def test_delivery_caveats_and_representation_label_required(self):
  for key,value in [('mixed_representations',{}),('representation',{}),('approval_scope',{})]:
   chunks=copy.deepcopy(self.b['chunks']);chunks[0]['metadata'][key]=value
   with self.assertRaisesRegex(ValueError,'mixed_delivery_'):mixed_text.validate_delivery(self.b['metadata'],chunks)
 def test_prior_checked_in_bundles_identical(self):
  n=0
  for path in (ROOT/'reports').rglob('*bundle.json'):
   if 'pully-mixed-text' in str(path):continue
   b=json.loads(path.read_text())
   if isinstance(b,dict) and all(k in b for k in ('review','pages','byte_count','chunks')):
    self.assertEqual(bridge.assemble(b['review'],b['pages'],b['byte_count']),b,str(path));n+=1
  self.assertEqual(n,6)
