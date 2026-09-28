import copy,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import arc_en_ciel_native as a
class ArcNativeTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_context_scope_is_pinned(self):
  from official_references import ARC_CONTEXT,validate_context_declaration
  validate_context_declaration(a.DOC,5635,ARC_CONTEXT)
  for doc,bfs,dec in [('wrong',5635,ARC_CONTEXT),(a.DOC,5606,ARC_CONTEXT),(a.DOC,5635,{**ARC_CONTEXT,'source_communes':[5583,5624,5635]}),(a.DOC,5635,{**ARC_CONTEXT,'source_sha256':'changed'})]:
   with self.assertRaises(ValueError):validate_context_declaration(doc,bfs,dec)
 def test_live_scope_guards_before_http(self):
  from unittest.mock import MagicMock,patch
  from official_references import refresh,ARC_CONTEXT
  cases=[('source',[('changed',)],[(5583,True),(5624,True)]),('associations',[(a.SHA,)],[(5583,True)]),('notcurrent',[(a.SHA,),(False,)],[(5583,True),(5624,True)])]
  for name,ones,rows in cases:
   conn=MagicMock();cur=conn.cursor.return_value.__enter__.return_value;cur.fetchone.side_effect=ones;cur.fetchall.return_value=rows
   with patch('official_references.requests.get') as http:
    with self.assertRaises(ValueError):refresh(conn,a.DOC,5635,[2532000,1154000,2533000,1155000],context_declaration=ARC_CONTEXT)
    http.assert_not_called()
  conn=MagicMock();conn.cursor.return_value.__enter__.return_value.fetchone.return_value=(0,)
  with patch('official_references.requests.get') as http:
   with self.assertRaises(ValueError):refresh(conn,a.DOC,5635,[2532000,1154000,2533000,1155000])
   http.assert_not_called()
 def test_exact_attribution(self):self.assertEqual(a.ATTRIBUTION,{'A1':[5583,5624],'A2':[5583,5624],'B1':[5583,5624],'B2':[5583],'D1':[5583],'D2':[5583]})
 def test_six_faces_104_pairs(self):self.assertEqual(sum(len(f['expected_pairs']) for f in self.b['features']),104)
 def test_unknown_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_no_public(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_unsigned_original(self):self.reject(lambda b:b.update(source_plan_status='approved'))
 def test_source_scope_not_neighbour_references(self):self.reject(lambda b:b.update(source_scope_communes=[5583,5624,5635]))
 def test_no_unreviewed_faces(self):self.reject(lambda b:b['features'][0].update(key='C11'))
 def test_reserved_roles(self):self.reject(lambda b:b['frozen_grid']['controls'][1].update(role='train'))
 def test_original_shape(self):self.reject(lambda b:b['source_native_proof']['selected'][0]['geometry']['coordinates'][0][0].__setitem__(0,0))
 def test_datum(self):self.reject(lambda b:b['source_native_proof']['selected'][0]['official_reframe_responses'][0]['lv95'].__setitem__(0,0))
 def test_no_global_accuracy(self):self.reject(lambda b:b['alignment'].update(holdout_rmse_m=0))
 def test_reference_kind(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0]['attributes'].update(GENRE_TXT='unknown'))
 def test_reference_geometry(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0]['reference_geometry'].update(coordinates=[[[0,0],[1,0],[1,1],[0,0]]]))
 def test_noop_other(self):self.assertIsNone(a.persist(None,a.DOC,'wrong'))
if __name__=='__main__':unittest.main()
