import copy,json,unittest
from pathlib import Path
from unittest.mock import MagicMock
import _pipeline_path  # noqa: F401  (adds the pipeline dir to sys.path)
import vd_pdcom_text as bridge
import mixed_text
import historical_former_commune as h
ROOT=Path(__file__).resolve().parents[1]
class HistoricalTests(unittest.TestCase):
 def setUp(self):
  self.b=json.loads((ROOT/'reports/st-legier-historical-text/bundle.json').read_text());self.r=self.b['review']
 def test_exact_historical_bundle_and_visible_prefix(self):
  self.assertEqual(bridge.assemble(self.r,self.b['pages'],self.b['byte_count']),self.b)
  mixed_text.validate_delivery(self.b['metadata'],self.b['chunks'])
  self.assertEqual(bridge.source_kind(self.r),h.SOURCE);self.assertEqual(len(self.b['chunks']),18)
  self.assertIsNone(self.r['commune_bfs']);self.assertEqual(self.r['plan_status'],'unverified')
  for c in self.b['chunks']:self.assertTrue(c['content'].startswith(h.LABEL+'\n\n'))
 def test_active_scope_status_and_caveat_fail_closed(self):
  for field,value in [('commune_bfs',5888),('commune_bfs',5892),('scope','municipal'),('plan_status','approved'),('source_role','approved_main'),('source_sha256','0'*64)]:
   r=copy.deepcopy(self.r);r[field]=value
   with self.assertRaisesRegex(ValueError,'historical_'):bridge.validate_review(r)
 def test_historical_semantics_exactly_pinned(self):
  for key,value in [('former_commune_bfs',5892),('registry_tracking_bfs',5888),('human_readable_caveat',''),('amendment_incorporation','verified'),('page_specific_limits',{'arbitrary':'nonempty'})]:
   r=copy.deepcopy(self.r);r['historical_scope'][key]=value
   with self.assertRaisesRegex(ValueError,'historical_'):bridge.validate_review(r)
  r=copy.deepcopy(self.r);r['signed_approval']['council']='2004-01-01'
  with self.assertRaisesRegex(ValueError,'historical_reviewed_semantics_changed'):bridge.validate_review(r)
 def test_chunk_parser_prefix_or_scope_change_rejected(self):
  for field,value in [('parser',bridge.SOURCE),('commune_bfs',5892),('historical_scope',{})]:
   chunks=copy.deepcopy(self.b['chunks']);chunks[0]['metadata'][field]=value
   with self.assertRaisesRegex(ValueError,'historical_'):mixed_text.validate_delivery(self.b['metadata'],chunks)
  chunks=copy.deepcopy(self.b['chunks']);chunks[0]['content']=chunks[0]['content'].split('\n\n',1)[1]
  with self.assertRaisesRegex(ValueError,'mixed_delivery_sequence'):mixed_text.validate_delivery(self.b['metadata'],chunks)
 def test_historical_delivery_without_mixed_flag_rejected(self):
  for mode in (None,False,{}):
   meta=copy.deepcopy(self.b['metadata']);meta['mixed_representations']=mode
   c=MagicMock();q=c.cursor.return_value.__enter__.return_value;q.fetchone.return_value=(h.SOURCE,meta)
   with self.assertRaisesRegex(ValueError,'historical_mixed_representation_required'):bridge.deliver(c,self.b['document_id'])
 def test_historical_delivery_cannot_be_relabelled_current(self):
  c=MagicMock();q=c.cursor.return_value.__enter__.return_value;q.fetchone.return_value=(bridge.SOURCE,self.b['metadata'])
  with self.assertRaisesRegex(ValueError,'source_kind_scope_mismatch'):bridge.deliver(c,self.b['document_id'])
 def test_registered_tracking_is_not_territory(self):
  for rows in [[(5888,'unverified')],[(5892,'full')],[(5892,'unverified'),(5888,'unverified')]]:
   q=MagicMock();q.fetchall.return_value=rows
   with self.assertRaisesRegex(ValueError,'historical_tracking_relation_changed'):h.validate_registered_mapping(q)
 def test_prior_checked_in_bundles_unchanged(self):
  n=0
  for path in (ROOT/'reports').rglob('*bundle.json'):
   if 'st-legier-historical-text' in str(path):continue
   b=json.loads(path.read_text())
   if isinstance(b,dict) and all(k in b for k in ('review','pages','byte_count','chunks')):
    self.assertEqual(bridge.assemble(b['review'],b['pages'],b['byte_count']),b);n+=1
  self.assertGreaterEqual(n,8)
