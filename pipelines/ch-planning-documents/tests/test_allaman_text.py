import copy
import json
from pathlib import Path
import unittest
from unittest.mock import MagicMock
import _pipeline_path  # noqa: F401  (adds the pipeline dir to sys.path)
import vd_pdcom_text as bridge
import allaman_text as m
ROOT=Path(__file__).resolve().parents[1]
class AllamanTests(unittest.TestCase):
 def setUp(self):
  self.b=json.loads((ROOT/'reports/allaman-text/bundle.json').read_text());self.r=self.b['review']
 def test_exact_existing_pdf_text_bundle(self):
  self.assertEqual(bridge.assemble(self.r,self.b['pages'],self.b['byte_count']),self.b)
  self.assertEqual((len(self.b['pages']),len(self.b['chunks'])),(99,35))
  self.assertFalse(self.b['metadata']['full_visual_map_extraction']);self.assertIsNone(self.b['metadata']['map_pages'])
  self.assertEqual(len(bridge.page_extraction_report(self.b)['withheld_native_pages']),73)
  import selected_native
  selected_native.validate_delivery(self.b['metadata'],[c['metadata'] for c in self.b['chunks']])
 def test_held_content_or_status_cannot_be_assembled(self):
  for field,value in [('text','injected held map label'),('status','native_text_extracted')]:
   pages=copy.deepcopy(self.b['pages']);pages[0][field]=value
   with self.assertRaisesRegex(ValueError,'native_selection_withheld_content'):bridge.assemble(self.r,pages,self.b['byte_count'])
 def test_selected_text_or_provenance_cannot_change(self):
  for field,value in [('text','rewritten policy'),('native_selection_provenance',{})]:
   pages=copy.deepcopy(self.b['pages']);pages[6][field]=value
   with self.assertRaisesRegex(ValueError,'native_selection_'):bridge.assemble(self.r,pages,self.b['byte_count'])
 def test_ordinary_native_cannot_use_withheld_status(self):
  r=copy.deepcopy(self.r);del r['native_page_selection']
  with self.assertRaisesRegex(ValueError,'page_status_required'):bridge.assemble(r,self.b['pages'],self.b['byte_count'])
 def test_selection_rejects_regional_or_ocr_scope(self):
  for key,value in [('scope','intercommunal'),('text_extraction',{'mode':'reviewed_ocr'})]:
   r=copy.deepcopy(self.r);r[key]=value
   with self.assertRaisesRegex(ValueError,'native_selection_'):bridge.validate_review(r)
 def test_delivery_rejects_caveat_and_held_page_mutation(self):
  import selected_native
  for field,value in [('native_page_selection',{}),('native_selection_provenance',self.r['native_page_selection']['pages'][0])]:
   chunks=copy.deepcopy([c['metadata'] for c in self.b['chunks']]);chunks[0][field]=value
   with self.assertRaisesRegex(ValueError,'native_delivery_'):selected_native.validate_delivery(self.b['metadata'],chunks)
 def test_prior_frozen_bundles_assemble_identically(self):
  for path in (ROOT/'reports').rglob('*bundle.json'):
   b=json.loads(path.read_text())
   if isinstance(b,dict) and all(k in b for k in ('review','pages','byte_count','chunks')):
    self.assertEqual(bridge.assemble(b['review'],b['pages'],b['byte_count']),b,str(path))
 def connection(self,status='unverified'):
  before={'id':m.DOCUMENT,'sha256':m.SHA,'page_count':99,'source_url':self.r['source_url'],'plan_status':status,'downloaded_at':'original-time','inspection':{'original':'retained'}}
  after=dict(before,plan_status='approved');c=MagicMock();q=c.cursor.return_value.__enter__.return_value;q.fetchone.side_effect=[(before,),(after,)];q.rowcount=1;return c,q,before
 def test_status_only_transition_preserves_original_evidence(self):
  c,q,before=self.connection();result=m.promote_source(c,self.r)
  self.assertTrue(result['changed']);self.assertTrue(result['only_plan_status_changed']);c.commit.assert_not_called()
  updates=[x for x in q.execute.call_args_list if str(x.args[0]).startswith('UPDATE')];self.assertEqual(len(updates),1)
  self.assertEqual(updates[0].args[1][1].adapted,before)
 def test_approved_status_replay_is_noop(self):
  c,q,_=self.connection('approved');self.assertFalse(m.promote_source(c,self.r)['changed'])
  self.assertFalse(any(str(x.args[0]).startswith('UPDATE') for x in q.execute.call_args_list))
 def test_other_source_cannot_be_promoted(self):
  r=copy.deepcopy(self.r);r['vd_document_id']='8f42da46-9b59-5b72-aa5b-13522e418682';c=MagicMock()
  with self.assertRaisesRegex(ValueError,'allaman_review_identity'):m.promote_source(c,r)
  c.cursor.assert_not_called()
 def test_unexpected_prior_status_rejected(self):
  c,q,_=self.connection('consultation')
  with self.assertRaisesRegex(ValueError,'unexpected_prior_status'):m.promote_source(c,self.r)
  self.assertFalse(any(str(x.args[0]).startswith('UPDATE') for x in q.execute.call_args_list))
 def test_registered_byte_identity_mismatch_prevents_update(self):
  c,q,before=self.connection();before['sha256']='0'*64;q.fetchone.side_effect=[(before,)]
  with self.assertRaisesRegex(ValueError,'registered_identity'):m.promote_source(c,self.r)
  self.assertFalse(any(str(x.args[0]).startswith('UPDATE') for x in q.execute.call_args_list))
 def test_source_chronology_change_fails_guard(self):
  c,q,before=self.connection();after=dict(before,plan_status='approved',downloaded_at='changed-time');q.fetchone.side_effect=[(before,),(after,)]
  with self.assertRaisesRegex(ValueError,'source_other_fields_changed'):m.promote_source(c,self.r)
  c.commit.assert_not_called()
