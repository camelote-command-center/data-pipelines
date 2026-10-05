import copy
import json
import math
from pathlib import Path
import unittest
from unittest.mock import patch
from shapely.affinity import affine_transform
from shapely.geometry import MultiPolygon, shape
from exact_source_nodes import node_existing_vertices

ROOT=Path(__file__).resolve().parents[1]

class ExactSourceNodesTests(unittest.TestCase):
    def setUp(self):
        self.review=json.loads((ROOT/'reports/epalinges-fallow-junction/review.json').read_text())
        self.paths=self.review['junction_diagnostic']['paths']
        self.geometries=[x['original_pdf'] for x in self.paths]

    def test_real_source_exact_junction_and_support_conservation(self):
        original=copy.deepcopy(self.geometries)
        result=node_existing_vertices(self.geometries)
        self.assertEqual(self.geometries,original)
        self.assertEqual(result['insertions'],[{'polygon_index':2,'edge_index':2,
            'existing_source_vertex':[423.2460021972656,513.2969970703125],
            'exact_rational_parameter':'158367/316801'}])
        for raw,noded,frozen in zip(original,result['geometries'],self.paths):
            self.assertTrue(shape(raw).equals(shape(noded)))
            self.assertEqual(shape(raw).area,shape(noded).area)
            self.assertTrue(shape(noded).equals_exact(shape(frozen['noded_pdf']),0))
        self.assertEqual(node_existing_vertices(result['geometries'])['insertions'],[])

    def test_frozen_transform_roundoff_regression(self):
        a=self.review['alignment'];angle,scale,tx,ty=a['parameters']
        c,s=math.cos(angle),math.sin(angle);p,q=a['pdf_origin_y_flipped'],a['lv95_origin'];h=841.8900146484375
        m=[scale*c,scale*s,scale*s,-scale*c,q[0]+tx-scale*c*p[0]-scale*s*(h-p[1]),q[1]+ty-scale*s*p[0]+scale*c*(h-p[1])]
        transfer=self.review['page_transfer']['pdf_to_page99']
        transform=lambda g:affine_transform(affine_transform(shape(g),transfer),m)
        self.assertFalse(MultiPolygon([transform(g) for g in self.geometries]).is_valid)
        transformed=[transform(g) for g in node_existing_vertices(self.geometries)['geometries']]
        self.assertTrue(MultiPolygon(transformed).is_valid)
        for actual,frozen in zip(transformed,self.paths):
            self.assertTrue(actual.equals_exact(shape(frozen['noded_lv95']),0))
        geographic=MultiPolygon([shape(x['noded4326']) for x in self.paths])
        self.assertTrue(geographic.is_valid)
        self.assertEqual(len(geographic.geoms),3)
        self.assertTrue(all(len(g.interiors)==0 for g in geographic.geoms))

    def test_near_collinear_is_not_snapped(self):
        a={'type':'Polygon','coordinates':[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}
        b={'type':'Polygon','coordinates':[[[2+1e-12,1],[3,0],[3,2],[2+1e-12,1]]]}
        self.assertEqual(node_existing_vertices([a,b])['insertions'],[])

    def test_invalid_overlapping_holes_and_nonfinite_sources_rejected(self):
        bad=copy.deepcopy(self.geometries);bad[0]['coordinates'][0][1][0]=float('nan')
        with self.assertRaises(ValueError):node_existing_vertices(bad)
        bad=copy.deepcopy(self.geometries);bad[0]['coordinates'][0][0][0]=True
        with self.assertRaises(ValueError):node_existing_vertices(bad)
        bad=copy.deepcopy(self.geometries);bad[0]['coordinates'].append(bad[0]['coordinates'][0])
        with self.assertRaises(ValueError):node_existing_vertices(bad)
        with self.assertRaises(ValueError):node_existing_vertices([self.geometries[0],self.geometries[0]])
        with self.assertRaises(ValueError):node_existing_vertices([])

    def test_existing_delivery_route_still_rejects_fallow(self):
        import epalinges_thematic
        batch=epalinges_thematic.load_artifacts()
        self.assertEqual(len(batch['features']),7)
        self.assertEqual(sum(len(x['expected_pairs']) for x in batch['features']),676)
        with self.assertRaises(ValueError):epalinges_thematic.sector_id('102:fallow')
        altered=copy.deepcopy(batch)
        altered['features'].append({'key':'102:fallow'})
        with self.assertRaises(ValueError):epalinges_thematic.validate(altered)

    def test_regeneration_rejects_changed_review_and_source_bytes(self):
        import epalinges_fallow_research as research
        review_bytes=research.REVIEW.read_bytes()
        with patch.object(Path,'read_bytes',return_value=b'changed review'):
            with self.assertRaisesRegex(ValueError,'fallow_review_changed'):research.extract('unused.pdf')
        with patch.object(Path,'read_bytes',side_effect=[review_bytes,b'changed source']):
            with self.assertRaisesRegex(ValueError,'fallow_source_hash'):research.extract('unused.pdf')

    def test_review_fixture_remains_held(self):
        self.assertEqual(self.review['state'],'held_research_only_not_delivery_eligible')
        self.assertIsNone(self.review['source_precision_m'])
        self.assertFalse(self.review['qualification'])
        self.assertTrue(self.review['no_production_adapter'])
        self.assertEqual(self.review['path_ids'],['102:46','102:47','102:48'])

if __name__=='__main__':unittest.main()
