import unittest,copy
from unittest.mock import MagicMock
import prangins_package_native as p
class NativeTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=p.load_artifacts()
 def bad(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):p.validate(b)
 def test_scope(self):self.assertEqual(len(self.b['features']),4);self.assertEqual(len({p.sector_id(f['key']) for f in self.b['features']}),len(self.b['features']))
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
 def test_collection(self):self.bad(lambda b:b['features'][0].update(geometry_lv95={'type':'Point','coordinates':[0,0]}))
 def test_wrong_document_no_read(self):self.assertIsNone(p.persist(None,'other',p.SHA))
 def test_line_not_polygon(self):
  with self.assertRaisesRegex(ValueError,'line_type'):p.combine([{'geometry_lv95':{'type':'Point','coordinates':[0,0]}}],'source_native_line')
 def test_stored_source_no_insert(self):
  co=MagicMock();cur=co.cursor.return_value.__enter__.return_value;cur.fetchone.return_value=('wrong',1)
  with self.assertRaisesRegex(ValueError,'stored_source'):p.persist(co,p.DOC,p.SHA)
  self.assertFalse(any('INSERT' in x.args[0] for x in cur.execute.call_args_list))
 def test_frame_translation(self):self.bad(lambda b:b['features'][0]['translation'].__setitem__(0,0))
 def test_hull_rejects_geometry_outside(self):
  from shapely.affinity import translate
  from shapely.geometry import shape,mapping
  self.bad(lambda b:b['features'][0]['paths'][0].update(geometry_base_pdf=mapping(translate(shape(b['features'][0]['paths'][0]['geometry_base_pdf']),1000,1000))))
if __name__=='__main__':unittest.main()
