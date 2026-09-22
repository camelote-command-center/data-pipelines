import copy,unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import prangins_reinforcement as p
class ThematicTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=p.load_artifacts()
 def reject(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):p.validate(b)
 def test_scope(self):
  self.assertEqual(len(self.b['features']),1)
  self.assertEqual(sum(len(f['expected_pairs']) for f in self.b['features']),80)
  self.assertEqual(len({p.sector_id(k) for k in p.KEYS}),1)
 def test_source(self):self.reject(lambda b:b.update(source_sha256='wrong'))
 def test_public(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_missing_group(self):self.reject(lambda b:b['features'].pop())
 def test_duplicate_prior(self):self.reject(lambda b:b['features'][0]['properties'][0].update(id='fill:999999'))
 def test_transfer(self):self.reject(lambda b:b['affine'].__setitem__(4,30))
 def test_union(self):self.reject(lambda b:b['features'][0].update(geometry_lv95=b['features'][0]['source_paths'][0]['geometry_lv95']))
 def test_invalid(self):self.reject(lambda b:b['features'][0].update(invalid_reference_envelope_overlap=['invalid']))
 def test_type(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0]['attributes'].update(GENRE_TXT='unknown'))
 def test_semantics(self):self.reject(lambda b:b['features'][0]['properties'][0].update(category='medium'))
 def test_noop_other(self):self.assertIsNone(p.persist(None,p.DOC,'changed'))
 def test_conflicting_tile_rejected_before_sectors(self):
  from unittest.mock import MagicMock,patch
  conn=MagicMock();c=conn.cursor.return_value.__enter__.return_value
  c.fetchone.side_effect=[(p.SHA,102),(1,)]
  with patch.object(p,'refresh',return_value={'snapshot_id':'00000000-0000-0000-0000-000000000001'}):
   with self.assertRaisesRegex(ValueError,'conflicting_tile'):p.persist(conn,p.DOC,p.SHA)
  self.assertFalse(any('INSERT INTO bronze_ch.vd_pdcom_sectors' in call.args[0] for call in c.execute.call_args_list))
 def test_policy_not_reclassified(self):self.reject(lambda b:b['features'][0]['policy_status'].update(state='current_development'))
 def test_support_membership(self):self.reject(lambda b:b['features'][0]['path_ids'].append('fill:1'))
if __name__=='__main__':unittest.main()
