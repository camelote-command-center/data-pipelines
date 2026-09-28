import unittest
from commune_review_progress import projection
class ProgressTests(unittest.TestCase):
 def setUp(self):
  self.c={'delivery_status':'not_ready','extraction_status':'downloaded','evidence':[{'document_id':'d','version_review':'pending'},{'other':'historical'}],'blocker':'prior hold','review_note':'prior review'}
  self.r={'document_id':'d','source_sha256':'sha','status':'approved','evidence_uri':'original.pdf#page=149','scope':'municipal policy, not all prose','currentness_and_completeness_resolved':False}
 def run_projection(self,c=None,r=None):return projection(c or self.c,'d','sha',['s'],r or self.r)
 def test_preserves_prior_evidence_and_holds_and_idempotent(self):
  p=self.run_projection();self.assertEqual(p['evidence'][:2],self.c['evidence']);self.assertEqual(p['blocker'],'prior hold');self.assertTrue(p['review_note'].startswith('prior review\n'));self.assertIsNone(self.run_projection(dict(self.c,**p)))
 def test_never_downgrades_or_promotes_completed_state(self):
  for key,value in [('delivery_status','verified'),('delivery_status','pending'),('extraction_status','validated')]:
   with self.assertRaises(ValueError):self.run_projection(dict(self.c,**{key:value}))
 def test_no_implicit_currentness_or_wrong_source(self):
  for key,value in [('source_sha256','different'),('currentness_and_completeness_resolved',True),('scope','')]:
   with self.assertRaises(ValueError):self.run_projection(r=dict(self.r,**{key:value}))
 def test_conflicting_event_cannot_silently_rewrite(self):
  p=self.run_projection();p['evidence'][-1]['private_sector_ids']=['different']
  with self.assertRaises(ValueError):self.run_projection(dict(self.c,**p))
if __name__=='__main__':unittest.main()
