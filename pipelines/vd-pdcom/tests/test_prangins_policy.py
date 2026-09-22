import unittest,sys,copy,json
from pathlib import Path
from unittest.mock import MagicMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import prangins_policy as p
class PolicyTests(unittest.TestCase):
 def test_scope(self):
  b=p.load();self.assertEqual(len(b['rows']),23);self.assertFalse(b['context']['affected_existing_geometry_established']);self.assertEqual(b['context']['currentness'],'unresolved')
 def test_other_source_noop(self):self.assertIsNone(p.persist(None,p.DOC,'changed'))
 def test_hash_guard(self):
  with patch.object(Path,'read_bytes',return_value=b'{}'):
   with self.assertRaises(ValueError):p.load()
 def test_source_guard(self):
  co=MagicMock();c=co.cursor.return_value.__enter__.return_value;c.fetchone.return_value=('changed',)
  with self.assertRaises(ValueError):p.persist(co,p.DOC,p.SHA)
  self.assertFalse(any(x.args[0].startswith('UPDATE') for x in c.execute.call_args_list))
 def test_membership_guard(self):
  co=MagicMock();c=co.cursor.return_value.__enter__.return_value;c.fetchone.return_value=(p.SHA,);c.fetchall.return_value=[]
  with self.assertRaises(ValueError):p.persist(co,p.DOC,p.SHA)
  self.assertFalse(any(x.args[0].startswith('UPDATE') for x in c.execute.call_args_list))
 def test_evidence_only_mutation(self):
  b=p.load();co=MagicMock();c=co.cursor.return_value.__enter__.return_value;c.fetchone.return_value=(p.SHA,);c.fetchall.return_value=[(r['id'],r['label']) for r in b['rows']];c.rowcount=23
  result=p.persist(co,p.DOC,p.SHA);self.assertFalse(result['geometry_changed']);q=[x.args[0] for x in c.execute.call_args_list if x.args[0].startswith('UPDATE')];self.assertEqual(len(q),1);self.assertEqual(q[0].split(' SET ')[1].split('=')[0],'validation_evidence');self.assertIn('IS DISTINCT FROM',q[0])
if __name__=='__main__':unittest.main()
