import copy,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import nord_activity_pilot as pilot

class NordActivityArtifacts(unittest.TestCase):
    def test_supported_scope_and_outward_conversion(self):
        features,evidence,qa,frozen=pilot.load_artifacts()
        self.assertEqual(len(features),17)
        self.assertEqual(sum(len(qa[k]['pairs']) for k in ('47','85','103','12')),25)
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

    def test_complete_holdouts_reproduced_from_frozen_current_geometry(self):
        from shapely.geometry import shape
        from shapely.ops import unary_union
        from shapely import wkb,hausdorff_distance
        features,evidence,qa,frozen=pilot.load_artifacts()
        refs={f['properties']['EGRID']:f for f in frozen}
        for f in features:
            code=f['properties']['site_code'];h=evidence[code]['independent_shape_holdout']
            keys=[p['egrid'] for p in h['parcels']] if h['kind']=='land_parcel_union' else [h['egrid']]
            self.assertTrue(set(keys)<={p['egrid'] for p in qa[code]['pairs']})
            for k in keys:self.assertEqual(refs[k]['properties']['GENRE_TXT'],'parcelle privée')
            reference=unary_union([wkb.loads(bytes.fromhex(refs[k]['properties']['geom_ewkb'])) for k in keys])
            source=shape(f['geometry'])
            self.assertGreater(source.intersection(reference).area/source.union(reference).area,.98)
            self.assertLess(hausdorff_distance(source.boundary,reference.boundary,densify=.05),5)

    def test_partial_or_rights_union_cannot_be_promoted(self):
        original=Path.read_text
        for mutation in ('right','partial','duplicate'):
            def altered(path,*args,**kwargs):
                raw=original(path,*args,**kwargs)
                if path.name=='evidence.json':
                    obj=json.loads(raw);h=obj['16']['independent_shape_holdout']
                    if mutation=='right':h['parcels'][0]['kind']='DDP superficie'
                    if mutation=='partial':h['partial_land_parcels']=[{'egrid':'unresolved'}]
                    if mutation=='duplicate':h['parcels'].append(h['parcels'][0])
                    return json.dumps(obj)
                return raw
            with patch.object(Path,'read_text',altered):
                with self.assertRaisesRegex(ValueError,'control_review'):pilot.load_artifacts()

    def test_wrong_source_does_not_write(self):
        self.assertIsNone(pilot.persist(None,pilot.DOC,'changed-source'))
        self.assertIsNone(pilot.persist(None,'another-document',pilot.SHA))

if __name__=='__main__':unittest.main()


def test_source_semantic_corrections():
    import nord_activity_pilot as n
    _,ev,_,_=n.load_artifacts()
    assert ev['85']['source_semantics']['label']=='ZAL 85 — Bioley-Magnoux'
    assert ev['82']['source_semantics']['intention']==['reconversion_out_of_activity']
    assert n.checked_label('85','ZAL 85 — Chêne-Pâquier','ZAL 85 — Bioley-Magnoux')=='ZAL 85 — Bioley-Magnoux'
    import pytest
    with pytest.raises(ValueError):n.checked_label('85','unexpected','ZAL 85 — Bioley-Magnoux')
    with pytest.raises(ValueError):n.checked_label('82','ZAL 85 — Chêne-Pâquier','ZAL 85 — Bioley-Magnoux')
