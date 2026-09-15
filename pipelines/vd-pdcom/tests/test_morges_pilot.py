import pathlib,sys,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
import morges_pilot as m
class MorgesSafety(unittest.TestCase):
 def test_wrong_source_does_not_call_reference_route(self):
  with patch.object(m,'refresh',side_effect=AssertionError('network must not run')):
   self.assertIsNone(m.persist(None,m.DOC,'changed bytes'))
   self.assertIsNone(m.persist(None,'other document',m.SHA))
 def test_partial_hatch_paths_cannot_become_candidates(self):
  for index in [8261,8641,5556]:
   with self.assertRaises(ValueError):m.sector_id(index)
 def test_frozen_review_scope_and_shared_parcel_are_preserved(self):
  qa,a,features,frozen=m.load_artifacts()
  self.assertEqual(len(features),6)
  sets={k:{r['egrid'] for r in v['0']['pairs']} for k,v in qa['sectors'].items()}
  self.assertEqual(sum(map(len,sets.values())),62)
  self.assertEqual(len(set.union(*sets.values())),61)
  self.assertEqual(sets['7935']&sets['8039'],{'CH798388424597'})
  self.assertEqual(len({m.sector_id(i) for i in m.PATHS}),6)
if __name__=='__main__':unittest.main()
