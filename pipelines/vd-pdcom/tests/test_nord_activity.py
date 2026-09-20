import copy,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import nord_activity_pilot as pilot

class NordActivityArtifacts(unittest.TestCase):
    def test_supported_scope_and_outward_conversion(self):
        features,evidence,qa,frozen=pilot.load_artifacts()
        self.assertEqual(len(features),4)
        self.assertEqual(sum(len(q['pairs']) for q in qa.values()),25)
        for code in ('47','85'):
            self.assertEqual(evidence[code]['source_semantics']['intention'],'Reconversion en autre zone à bâtir')
        self.assertTrue(any(f['properties']['GENRE_TXT']=='DDP superficie' for f in frozen))

    def test_invalid_independent_reference_cannot_be_promoted(self):
        original=Path.read_text
        for field,value in [('kind','DDP superficie'),('iou',.98),('hausdorff_m',5),('hausdorff_m',float('nan'))]:
            with self.subTest(field=field,value=value):
                def altered(path,*args,**kwargs):
                    raw=original(path,*args,**kwargs)
                    if path.name=='evidence.json':
                        obj=json.loads(raw);obj['47']['independent_shape_holdout'][field]=value;return json.dumps(obj)
                    return raw
                with patch.object(Path,'read_text',altered):
                    with self.assertRaisesRegex(ValueError,'control_review'):pilot.load_artifacts()

    def test_wrong_source_does_not_write(self):
        self.assertIsNone(pilot.persist(None,pilot.DOC,'changed-source'))
        self.assertIsNone(pilot.persist(None,'another-document',pilot.SHA))

if __name__=='__main__':unittest.main()
