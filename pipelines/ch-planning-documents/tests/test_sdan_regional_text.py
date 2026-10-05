import copy
import json
from pathlib import Path
import unittest
from unittest.mock import MagicMock
import _pipeline_path  # noqa: F401  (adds the pipeline dir to sys.path)
import vd_pdcom_text as m
ROOT=Path(__file__).resolve().parents[1]
class RegionalTests(unittest.TestCase):
 def setUp(self):
  self.b=json.loads((ROOT/'reports/sdan-regional-text/bundle.json').read_text())
 def test_exact_fixture_and_all_chunk_caveats(self):
  b=self.b
  self.assertEqual(m.assemble(b['review'],b['pages'],b['byte_count']),b)
  self.assertEqual(len(b['chunks']),64)
  self.assertIsNone(b['metadata']['commune_bfs'])
  for c in b['chunks']:
   m.validate_regional_scope(c['metadata'])
   self.assertEqual(c['metadata']['currentness_caveat'],b['metadata']['currentness_caveat'])
 def test_regional_scope_and_caveats_mandatory(self):
  for field,value in [('scope','municipal'),('currentness_caveat',''),('member_communes',[5724]),('commune_bfs',5724),('no_parcel_rights',False)]:
   r=copy.deepcopy(self.b['review']);r[field]=value
   with self.assertRaises(ValueError):m.validate_regional_scope(r)
 def test_delivery_rejects_missing_chunk_caveat_before_receiver_write(self):
  conn=MagicMock();c=conn.cursor.return_value.__enter__.return_value
  c.fetchone.return_value=(m.REGIONAL_SOURCE,self.b['metadata'])
  bad=copy.deepcopy(self.b['chunks'][0]['metadata']);bad.pop('currentness_caveat')
  c.fetchall.return_value=[(bad,)]
  with self.assertRaisesRegex(ValueError,'regional_historical_currentness'):m.deliver(conn,self.b['document_id'])
  self.assertFalse(any('UPDATE' in str(x) or 'INSERT' in str(x) for x in c.execute.call_args_list))
 def test_delivery_rejects_consistent_but_wrong_scope(self):
  conn=MagicMock();c=conn.cursor.return_value.__enter__.return_value
  bad=copy.deepcopy(self.b['metadata']);bad['scope']='municipal';c.fetchone.return_value=(m.REGIONAL_SOURCE,bad)
  with self.assertRaisesRegex(ValueError,'regional_scope_required'):m.deliver(conn,self.b['document_id'])
 def test_existing_municipal_fixture_unchanged(self):
  p=ROOT/'reports/lausanne-approved-text'
  r=json.loads((p/'review.json').read_text())
  self.assertEqual(m.source_kind(r),m.SOURCE)
