import unittest,copy
from unittest.mock import MagicMock
import prangins_native_final as p
class NativeTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=p.load_artifacts()
 def bad(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):p.validate(b)
 def test_scope(self):self.assertEqual(len(self.b['features']),7);self.assertEqual(len({p.sector_id(f['key']) for f in self.b['features']}),len(self.b['features']))
 def test_wrong_source(self):self.bad(lambda b:b.update(source_sha256='x'))
 def test_no_public(self):self.bad(lambda b:b.update(publication_status='public'))
 def test_no_precision(self):self.bad(lambda b:b.update(source_precision_m=1))
 def test_no_buffer(self):self.bad(lambda b:b['semantic_allowlist'][0].update(buffer_allowed=True))
 def test_no_parcels(self):self.bad(lambda b:b['semantic_allowlist'][0].update(parcel_links_allowed=True))
 def test_transform(self):self.bad(lambda b:b['affine'].__setitem__(4,0))
 def test_missing_class(self):self.bad(lambda b:b['features'].pop())
 def test_kind(self):self.bad(lambda b:b['features'][0].update(feature_kind='polygon'))
 def test_membership(self):self.bad(lambda b:b['features'][0]['paths'].pop())
 def test_label(self):self.bad(lambda b:b['features'][0].update(label='capacity'))
 def test_collection(self):self.bad(lambda b:b['features'][0].update(geometry_lv95=b['features'][1]['geometry_lv95']))
 def test_wrong_document_no_read(self):self.assertIsNone(p.persist(None,'other',p.SHA))
 def test_line_not_polygon(self):
  with self.assertRaisesRegex(ValueError,'line_type'):p.combine([{'geometry_lv95':{'type':'Point','coordinates':[0,0]}}],'source_native_line')
 def test_point_classification(self):
  fs={f['key']:f for f in self.b['features']}
  self.assertEqual([p['path_id'] for p in fs['33']['paths']],[4143])
  self.assertEqual([p['path_id'] for p in fs['34']['paths']],[4142])
  self.assertEqual(set(p['path_id'] for p in fs['47']['paths']),{2654,2655,2658})
  self.assertEqual(set(p['path_id'] for p in fs['48']['paths']),{2656,2657})
  self.assertNotIn(2646,[p['path_id'] for p in fs['59']['paths']])
 def test_crossing_anchor(self):
  def mutate(b):
   f=next(f for f in b['features'] if f['key']=='47');f['paths'][0]['support']['vertices'][0]=[0,0]
  self.bad(mutate)
 def test_star_symmetry(self):
  def mutate(b):
   f=next(f for f in b['features'] if f['key']=='33');f['paths'][0]['support'][0]['start_vertices'][0]=[0,0]
  self.bad(mutate)
 def test_circle_quarters(self):
  def mutate(b):
   f=next(f for f in b['features'] if f['key']=='35');f['paths'][0]['support'][0]['start_vertices'].pop()
  self.bad(mutate)
 def test_stored_source_no_insert(self):
  co=MagicMock();cur=co.cursor.return_value.__enter__.return_value;cur.fetchone.return_value=('wrong',1)
  with self.assertRaisesRegex(ValueError,'stored_source'):p.persist(co,p.DOC,p.SHA)
  self.assertFalse(any('INSERT' in x.args[0] for x in cur.execute.call_args_list))
if __name__=='__main__':unittest.main()
