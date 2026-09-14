import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from cadastral_types import classify
class CadastralTypes(unittest.TestCase):
    def feature(self,e='CH314581837868',kind='parcelle privée',bfs=5401):
        return {'properties':{'EGRID':e,'GENRE_TXT':kind,'NO_COM_FED':bfs}}
    def test_surface_right_keeps_distinct_egrid(self):
        expected={'CH314581837868':5401,'CH400745598377':5401}
        rows=[self.feature(),self.feature('CH400745598377','DDP superficie')]
        out=classify({'features':rows},expected,{})
        self.assertEqual(out['CH314581837868'][0],'bien_fonds')
        self.assertEqual(out['CH400745598377'][0],'ddp_superficie')
    def test_missing_and_unrecognized_are_explicit_unknown(self):
        expected={'CH314581837868':5401,'CH400745598377':5401}
        out=classify({'features':[self.feature(kind='new type')]},expected,{})
        self.assertEqual(out['CH314581837868'][1]['status'],'unrecognized_kind')
        self.assertEqual(out['CH400745598377'],('unknown',{'status':'not_found'}))
    def test_duplicate_or_unexpected_identity_rejected(self):
        for rows in [[self.feature(),self.feature()],[self.feature('CH400745598377')]]:
            with self.assertRaises(ValueError):classify({'features':rows},{'CH314581837868':5401},{})
    def test_mismatched_commune_rejected(self):
        with self.assertRaises(ValueError):classify({'features':[self.feature(bfs=5725)]},{'CH314581837868':5401},{})
    def test_truncated_and_error_responses_rejected(self):
        for payload in [{'features':[],'exceededTransferLimit':True},{'error':{}}]:
            with self.assertRaises(ValueError):classify(payload,{}, {})
