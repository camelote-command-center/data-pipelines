import copy
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch
import vd_pdcom_text as m
import reviewed_ocr as o
ROOT=Path(__file__).resolve().parents[1]
class ReviewedOCRTests(unittest.TestCase):
 def setUp(self):
  self.b=json.loads((ROOT/'reports/lutry-reviewed-ocr/part-2-bundle.json').read_text())
 def test_exact_three_part_corpus_with_all_holds(self):
  count=0;pages=0;selected=0
  for part in (1,2,3):
   b=json.loads((ROOT/f'reports/lutry-reviewed-ocr/part-{part}-bundle.json').read_text())
   self.assertEqual(m.assemble(b['review'],b['pages'],b['byte_count']),b)
   self.assertEqual(b['metadata']['parser'],m.OCR_SOURCE)
   self.assertEqual(b['metadata']['extraction_status'],'partial_reviewed_ocr')
   o.validate_delivery(b['metadata'],[x['metadata'] for x in b['chunks']])
   for page in b['pages']:
    if page['status']=='ocr_withheld':
     self.assertEqual(page['text'],'')
     self.assertNotIn(page['page_number'],[x['page_number'] for x in b['chunks']])
    else:selected+=1
   count+=len(b['chunks']);pages+=len(b['pages'])
  self.assertEqual((pages,selected,count),(86,53,68))
 def test_ocr_caveats_required(self):
  for key,value in [('ocr_quality','native'),('requires_source_image_verification',False),('corpus_limits',{})]:
   r=copy.deepcopy(self.b['review']);r[key]=value
   with self.assertRaises(ValueError):m.validate_review(r)
 def test_artifact_cannot_escape_review_directory(self):
  r=copy.deepcopy(self.b['review']);r['text_extraction']['artifact']='../../other.json'
  with self.assertRaisesRegex(ValueError,'ocr_artifact_path'):m.validate_review(r)
 def test_artifact_mutation_rejected_before_pdf_pages_read(self):
  r=copy.deepcopy(self.b['review']);r['text_extraction']['sha256']='0'*64
  with self.assertRaisesRegex(ValueError,'ocr_artifact_hash'):o.load_pages(MagicMock(),r)
 def test_lost_caveat_prevents_delivery(self):
  for key in ('reservations','corpus_limits','requires_source_image_verification'):
   chunks=[copy.deepcopy(x['metadata']) for x in self.b['chunks']];chunks[0].pop(key)
   with self.assertRaisesRegex(ValueError,'ocr_chunk_caveat'):o.validate_delivery(self.b['metadata'],chunks)
 def test_lost_ocr_mode_prevents_receiver_write(self):
  c=MagicMock();q=c.cursor.return_value.__enter__.return_value;meta=copy.deepcopy(self.b['metadata']);meta.pop('text_extraction');q.fetchone.return_value=(m.OCR_SOURCE,meta)
  with self.assertRaisesRegex(ValueError,'ocr_delivery_mode'):m.deliver(c,self.b['document_id'])
  self.assertFalse(any('INSERT' in str(x) or 'UPDATE' in str(x) for x in q.execute.call_args_list))
 def test_chunk_artifact_and_quality_not_invented(self):
  for key,value in [('confidence',99),('include_in_search',False)]:
   chunks=[copy.deepcopy(x['metadata']) for x in self.b['chunks']];chunks[0]['ocr_provenance'][key]=value
   with self.assertRaisesRegex(ValueError,'ocr_chunk_provenance'):o.validate_delivery(self.b['metadata'],chunks)
 def test_exact_ocr_text_retained(self):
  for part in (1,2,3):
   evidence=json.loads((ROOT/f'reports/lutry-reviewed-ocr/part-{part}-ocr.json').read_text())
   b=json.loads((ROOT/f'reports/lutry-reviewed-ocr/part-{part}-bundle.json').read_text())
   for e,p in zip(evidence['pages'],b['pages']):
    self.assertEqual(hashlib.sha256(e['raw_ocr_text'].encode()).hexdigest(),e['text_sha256'])
    self.assertEqual(p['text'],e['raw_ocr_text'] if e['include_in_search'] else '')

 def test_regional_scope_cannot_be_mislabelled_municipal_ocr(self):
  for key,value in [('scope','intercommunal'),('commune_bfs',None)]:
   r=copy.deepcopy(self.b['review']);r[key]=value
   with self.assertRaisesRegex(ValueError,'ocr_municipal_scope'):m.validate_review(r)
   meta=copy.deepcopy(self.b['metadata']);meta[key]=value
   with self.assertRaisesRegex(ValueError,'ocr_municipal_scope'):o.validate_delivery(meta,[x['metadata'] for x in self.b['chunks']])

 def test_cross_part_approval_and_reservation_cannot_point_elsewhere(self):
  for field in ('signed_approval','reservations'):
   r=copy.deepcopy(self.b['review']);ref=r[field] if field=='signed_approval' else r[field][0];ref['evidence_source_sha256']='0'*64
   with self.assertRaisesRegex(ValueError,'ocr_cross_document_evidence'):m.validate_review(r)

 def test_receipt_distinguishes_held_ocr_from_empty_native_text(self):
  result=m.page_extraction_report(self.b)
  self.assertEqual(result,{'withheld_ocr_pages':[4,6,8,9,10,11,12,13,14,15,16,21]})
  native={'review':{},'pages':[{'page_number':1,'text':'native'},{'page_number':2,'text':''}]}
  self.assertEqual(m.page_extraction_report(native),{'empty_native_text_pages':[2]})
