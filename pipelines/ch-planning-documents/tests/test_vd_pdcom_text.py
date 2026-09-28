import copy
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('vd_pdcom_text',ROOT/'vd_pdcom_text.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class CorpusContractTests(unittest.TestCase):
 def setUp(self):
  self.r=json.loads((ROOT/'reports/lausanne-approved-text/review.json').read_text());self.r['page_count']=3
  self.pages=[{'page_number':1,'text':'Historical capacity2013. '+'A'*2300,'status':'native_text_extracted','method':'pymupdf_native_text'}, {'page_number':2,'text':'','status':'no_native_text','method':'pymupdf_native_text'}, {'page_number':3,'text':'','status':'not_extracted_image_page','method':'pymupdf_native_text'}]
 def test_all_physical_pages_preserved_without_fake_text(self):
  b=m.assemble(self.r,self.pages,123)
  self.assertEqual(len(b['pages']),3);self.assertEqual(len(b['chunks']),2)
  self.assertEqual([c['page_number'] for c in b['chunks']],[1,1]);self.assertEqual(b['metadata']['page_manifest'][2]['status'],'not_extracted_image_page')
  self.assertEqual(b['metadata']['extraction_status'],'partial_native_text')
 def test_chunk_approval_and_vintage_limits_survive(self):
  b=m.assemble(self.r,self.pages,123)
  for c in b['chunks']:
   self.assertFalse(c['metadata']['all_prose_is_binding']);self.assertFalse(c['metadata']['spatial_qualification']);self.assertFalse(c['metadata']['commune_complete'])
   self.assertEqual(c['metadata']['approval_scope'],self.r['signed_approval']);self.assertEqual(c['metadata']['diagnostic_vintage_limit'],self.r['diagnostic_vintage_limit']);self.assertTrue(c['metadata']['citation'].endswith('#page=1'))
 def test_duplicate_and_missing_physical_pages_fail(self):
  for pages in [self.pages[:-1],[self.pages[0]]*3,list(reversed(self.pages))]:
   with self.assertRaisesRegex(ValueError,'physical_pages'):m.assemble(self.r,pages,123)
 def test_blank_corpus_never_searchable(self):
  for p in self.pages:p['text']=''
  with self.assertRaisesRegex(ValueError,'no_searchable_text'):m.assemble(self.r,self.pages,123)
 def test_supplement_cannot_inherit_approval(self):
  self.r['source_role']='action_programmes'
  with self.assertRaisesRegex(ValueError,'approved_main'):m.assemble(self.r,self.pages,123)
 def test_approval_provenance_required(self):
  self.r['signed_approval']['visually_verified']=False
  with self.assertRaisesRegex(ValueError,'approval_scope'):m.assemble(self.r,self.pages,123)
 def test_unapproved_source_fails(self):
  self.r['plan_status']='unverified'
  with self.assertRaisesRegex(ValueError,'approval_required'):m.assemble(self.r,self.pages,123)
 def test_official_host_bound(self):
  self.r['source_url']='https://example.com/false.pdf'
  with self.assertRaisesRegex(ValueError,'official_source_url'):m.assemble(self.r,self.pages,123)
 def test_exact_replay_ids_and_bytes(self):
  a=m.assemble(self.r,self.pages,123);b=m.assemble(copy.deepcopy(self.r),copy.deepcopy(self.pages),123)
  self.assertEqual(a,b)
  self.r['source_sha256']='0'*64;c=m.assemble(self.r,self.pages,123)
  self.assertNotEqual(c['document_id'],a['document_id']);self.assertNotEqual(c['version_id'],a['version_id'])
 def test_no_blanket_binding_or_geometry(self):
  for field in ['all_prose_is_binding','spatial_qualification']:
   r=copy.deepcopy(self.r);r[field]=True
   with self.assertRaisesRegex(ValueError,'no_blanket'):m.assemble(r,self.pages,123)
 def test_conflicting_existing_provenance_fails(self):
  class Cursor:
   def execute(self,*args):pass
   def fetchone(self):return ('stable-id',{'publisher':'wrong-register'})
  with self.assertRaisesRegex(ValueError,'immutable_provenance_conflict'):m.checked_insert(Cursor(),'knowledge_ch','documents',{'id':'stable-id','raw_metadata':{'publisher':'municipality'}})
 def test_modified_serialized_bundle_rejected_before_sql(self):
  b=m.assemble(self.r,self.pages,123);bad=copy.deepcopy(b);bad['chunks'][0]['content']='injected text';conn=MagicMock()
  with patch.object(m,'build_bundle',return_value=b):
   with self.assertRaisesRegex(ValueError,'bundle_not_exact_pdf_extraction'):m.persist(conn,bad,'operation','verified.pdf')
  conn.cursor.assert_not_called()
 def test_scoped_monitor_never_updates_dataset_freshness(self):
  conn=MagicMock();cursor=conn.cursor.return_value.__enter__.return_value
  before=(None,None,None,123,None,'active',None)
  cursor.fetchall.return_value=[('dataset-id',*before)];cursor.fetchone.side_effect=[('operation',),before]
  m.monitor(conn,'operation','partial',{'physical_pages':153})
  statements=[str(c.args[0]) for c in cursor.execute.call_args_list]
  self.assertTrue(any('UPDATE public.datasets' in s for s in statements))
  self.assertFalse(any('UPDATE public.datasets SET last_acquired_at=now' in s for s in statements))
  self.assertTrue(any('UPDATE public.acquisition_logs' in s for s in statements))
  with self.assertRaisesRegex(ValueError,'scoped_monitor_phase'):m.monitor(conn,'operation','success',{})
 def test_bounded_run_does_not_store_commit_acknowledgment(self):
  conn=MagicMock();cursor=conn.cursor.return_value.__enter__.return_value
  cursor.fetchone.return_value=('partial','timestamp',{'document_id':'doc','national_complete':False})
  m.finish_run(conn,'operation',{'document_id':'doc','committed':False})
  inserted=cursor.execute.call_args_list[0].args[1][0].adapted
  self.assertNotIn('committed',inserted);self.assertFalse(inserted['national_complete'])
 def test_pdf_hash_checked_before_extract(self):
  with self.assertRaisesRegex(ValueError,'pdf_hash_mismatch'):m.build_bundle(ROOT/'README.md',self.r)

if __name__=='__main__':unittest.main()
