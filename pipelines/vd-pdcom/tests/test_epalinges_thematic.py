import copy,unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import epalinges_thematic as p
class ThematicTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=p.load_artifacts()
 def reject(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):p.validate(b)
 def test_scope(self):
  self.assertEqual(len(self.b['features']),8)
  self.assertEqual(sum(len(f['expected_pairs']) for f in self.b['features']),683)
  self.assertEqual(len({p.sector_id(k) for k in p.KEYS}),8)
 def test_source(self):self.reject(lambda b:b.update(source_sha256='wrong'))
 def test_public(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_missing_group(self):self.reject(lambda b:b['features'].pop())
 def test_duplicate_prior(self):self.reject(lambda b:b['features'][0]['path_ids'].append('99:33'))
 def test_transfer(self):self.reject(lambda b:b['page_transfers'][0]['pdf_to_page99'].__setitem__(4,30))
 def test_union(self):self.reject(lambda b:b['features'][0].update(geometry_lv95=b['features'][1]['geometry_lv95']))
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
 def test_legacy_replay_scoped(self):
  from unittest.mock import patch
  import epalinges_pilot as old
  class Cursor:
   def __init__(self):self.queries=[]
   def __enter__(self):return self
   def __exit__(self,*a):pass
   def execute(self,q,args=None):self.queries.append((q,args))
   def fetchone(self):return (1,1)
  class Conn(Cursor):
   def cursor(self):return self
  conn=Conn();features=[{'properties':{'extended_drawing_index':n,'label':'old'},'geometry':{}} for n in (33,44,390,402,415,426)]
  import json
  with patch.object(Path,'read_text',side_effect=[json.dumps({'withheld_boundary_rmse_m':1}),json.dumps({'features':features})]):old.persist(conn,p.DOC,p.SHA)
  inserts=[(q,a) for q,a in conn.queries if 'WITH p AS MATERIALIZED' in q]
  self.assertEqual(len(inserts),1);q,args=inserts[0]
  self.assertIn('WHERE id=ANY(%s::uuid[])',q);self.assertEqual(len(args[0]),6)
  self.assertTrue(set(args[0]).isdisjoint({p.sector_id(k) for k in p.KEYS}))
if __name__=='__main__':unittest.main()
