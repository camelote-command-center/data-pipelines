import unittest,uuid
from unittest.mock import MagicMock
import prangins_category_correction as p
SID=str(uuid.uuid5(uuid.NAMESPACE_URL,p.DOC+'#indicative-category-2026-09-22#wooded_cordons_create'))
class CorrectionTests(unittest.TestCase):
 def test_exact_old(self):self.assertEqual(p.load_old()['path_ids'],[143,144,147])
 def test_wrong_id_before_read(self):
  c=MagicMock()
  with self.assertRaisesRegex(ValueError,'wrong_sector'):p.correct(c,str(uuid.uuid4()),'{}')
  c.execute.assert_not_called()
 def test_replay_no_mutation(self):
  c=MagicMock();c.fetchone.return_value=(p.DOC,'review_required',None,None,True,False)
  self.assertFalse(p.correct(c,SID,'{}'));self.assertEqual(c.execute.call_count,1)
 def test_unknown_geometry_no_mutation(self):
  c=MagicMock();c.fetchone.return_value=(p.DOC,'review_required',None,None,False,False)
  with self.assertRaisesRegex(ValueError,'unknown_geometry'):p.correct(c,SID,'{}')
  self.assertEqual(c.execute.call_count,1)
 def test_qualified_no_mutation(self):
  c=MagicMock();c.fetchone.return_value=(p.DOC,'validated',1,'reviewer',False,True)
  with self.assertRaisesRegex(ValueError,'sector_state'):p.correct(c,SID,'{}')
  self.assertEqual(c.execute.call_count,1)
 def test_changed_old_keys_no_mutation(self):
  c=MagicMock();c.fetchone.return_value=(p.DOC,'review_required',None,None,False,True);c.fetchall.return_value=[]
  with self.assertRaisesRegex(ValueError,'old_membership'):p.correct(c,SID,'{}')
  self.assertFalse(any('DELETE' in x.args[0] for x in c.execute.call_args_list))
if __name__=='__main__':unittest.main()
