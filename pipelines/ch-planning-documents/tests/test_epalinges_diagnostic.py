import copy
import json
from pathlib import Path
import unittest
from unittest.mock import MagicMock
import vd_pdcom_text as bridge
import epalinges_diagnostic as m
ROOT=Path(__file__).resolve().parents[1]
class DiagnosticTests(unittest.TestCase):
 def setUp(self):
  self.b=json.loads((ROOT/'reports/epalinges-diagnostic-text/bundle.json').read_text());self.r=self.b['review']
 def test_exact_existing_pdf_text_bundle(self):
  self.assertEqual(bridge.assemble(self.r,self.b['pages'],self.b['byte_count']),self.b)
  self.assertEqual((len(self.b['pages']),len(self.b['chunks'])),(144,201))
  self.assertFalse(self.b['metadata']['full_visual_map_extraction']);self.assertIsNone(self.b['metadata']['map_pages'])
  for c in self.b['chunks']:
   self.assertEqual(c['metadata']['reservations'][0]['cross_part_document_id'],'8f42da46-9b59-5b72-aa5b-13522e418682')
   self.assertEqual(c['metadata']['reservations'][0]['cross_part_pdf_pages'],[21,22])
   self.assertIn('Diagnostic volume',c['metadata']['diagnostic_vintage_limit'])
 def connection(self,status='unverified'):
  before={'id':m.DOCUMENT,'sha256':m.SHA,'page_count':144,'source_url':self.r['source_url'],'plan_status':status,'downloaded_at':'original-time','inspection':{'original':'retained'}}
  after=dict(before,plan_status='approved_with_reservation');c=MagicMock();q=c.cursor.return_value.__enter__.return_value;q.fetchone.side_effect=[(before,),(after,)];q.rowcount=1;return c,q,before
 def test_status_only_transition_preserves_original_evidence(self):
  c,q,before=self.connection();result=m.promote_source(c,self.r)
  self.assertTrue(result['changed']);self.assertTrue(result['only_plan_status_changed']);c.commit.assert_not_called()
  updates=[x for x in q.execute.call_args_list if str(x.args[0]).startswith('UPDATE')];self.assertEqual(len(updates),1)
  self.assertEqual(updates[0].args[1][1].adapted,before)
 def test_approved_status_replay_is_noop(self):
  c,q,_=self.connection('approved_with_reservation');self.assertFalse(m.promote_source(c,self.r)['changed'])
  self.assertFalse(any(str(x.args[0]).startswith('UPDATE') for x in q.execute.call_args_list))
 def test_other_source_cannot_be_promoted(self):
  r=copy.deepcopy(self.r);r['vd_document_id']='8f42da46-9b59-5b72-aa5b-13522e418682';c=MagicMock()
  with self.assertRaisesRegex(ValueError,'diagnostic_review_identity'):m.promote_source(c,r)
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
  c,q,before=self.connection();after=dict(before,plan_status='approved_with_reservation',downloaded_at='changed-time');q.fetchone.side_effect=[(before,),(after,)]
  with self.assertRaisesRegex(ValueError,'source_other_fields_changed'):m.promote_source(c,self.r)
  c.commit.assert_not_called()
