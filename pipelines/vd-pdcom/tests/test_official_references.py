import pathlib,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from official_references import validate
from cadastral_types import resolve_identities
from bex_pilot import persist,SHA,DOC
class OfficialReferences(unittest.TestCase):
    def feature(self,bfs=5402):return {'properties':{'EGRID':'CH123456789012','NO_COM_FED':bfs},'geometry':{'type':'Polygon','coordinates':[]}}
    def test_count_and_truncation_fail_closed(self):
        for response,count in [({'features':[self.feature()]},2),({'features':[self.feature()],'exceededTransferLimit':True},1),({'features':[]},0)]:
            with self.assertRaises(ValueError):validate(response,count,5402)
    def test_duplicate_and_wrong_commune_rejected(self):
        for fs in [[self.feature(),self.feature()],[self.feature(5401)]]:
            with self.assertRaises(ValueError):validate({'features':fs},len(fs),5402)
    def test_proven_reference_resolves_missing_mirror(self):
        e='CH123456789012';expected,refs=resolve_identities([(e,None,5402,'snapshot')]);self.assertEqual(expected,{e:5402});self.assertEqual(refs,{e:['snapshot']})
    def test_conflict_or_missing_proof_still_rejected(self):
        e='CH123456789012'
        for row in [(e,5401,5402,'snapshot'),(e,None,None,None),(e,5402,None,'snapshot')]:
            with self.assertRaises(ValueError):resolve_identities([row])
    def test_changed_source_cannot_replay_geometry(self):
        self.assertIsNone(persist(None,DOC,'changed'));self.assertIsNone(persist(None,'other',SHA))
if __name__=='__main__':unittest.main()
