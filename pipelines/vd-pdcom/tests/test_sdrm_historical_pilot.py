import copy,unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import sdrm_historical_pilot as p
class SDRMHistoricalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.base=p.load_artifacts()
    def case(self,fn):
        b=copy.deepcopy(self.base);fn(b)
        with self.assertRaises(ValueError):p.validate_artifacts(b)
    def test_exact_scope_and_idempotent_ids(self):
        self.assertEqual(len(self.base['features']),16)
        self.assertEqual(len({p.sector_id(x) for x in p.SITES}),16)
        self.assertEqual(p.sector_id('D2'),p.sector_id('D2'))
        with self.assertRaises(ValueError):p.sector_id('L9')
    def test_changed_source_rejected(self):self.case(lambda b:b.update(source_sha256='0'*64))
    def test_missing_holdout_rejected(self):self.case(lambda b:b['fit']['controls'].pop())
    def test_failed_holdout_rejected(self):self.case(lambda b:next(c for c in b['fit']['controls'] if c['role']=='holdout').update(error_m=20))
    def test_moved_geometry_rejected(self):self.case(lambda b:b['features'][0]['geometry_lv95']['coordinates'][0][0].__setitem__(0,2530000))
    def test_unapproved_semantics_rejected(self):self.case(lambda b:b['features'][0].update(source_semantic_class='high_landscape_value_preserve'))
    def test_public_release_rejected(self):self.case(lambda b:b['features'][0].update(publication_status='public'))
    def test_invalid_reference_not_repaired(self):self.case(lambda b:b['features'][0]['references'][0].update(invalid_reference_geometries=[{'egrid':'bad'}]))
    def test_duplicate_pair_rejected(self):self.case(lambda b:b['features'][0]['references'][0]['pairs'].append(copy.deepcopy(b['features'][0]['references'][0]['pairs'][0])))
    def test_unknown_type_rejected(self):self.case(lambda b:b['features'][0]['references'][0]['pairs'][0]['attributes'].update(GENRE_TXT='unknown'))
    def test_changed_source_no_runtime_write(self):
        self.assertIsNone(p.persist(None,p.DOC,'changed'))
        self.assertIsNone(p.persist(None,'other',p.SHA))
if __name__=='__main__':unittest.main()
