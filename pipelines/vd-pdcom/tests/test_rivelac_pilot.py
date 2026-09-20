import pathlib,sys,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
import rivelac_pilot as r
from shapely.geometry import shape
class RivelacSafety(unittest.TestCase):
 def test_changed_source_cannot_refresh_references(self):
  with patch.object(r,'refresh',side_effect=AssertionError('network forbidden')):
   self.assertIsNone(r.persist(None,r.DOC,'changed'))
   self.assertIsNone(r.persist(None,'different',r.SHA))
 def test_unresolved_sites_not_admitted(self):
  for code in ['2.9','2.10','4']:
   with self.assertRaises(ValueError):r.sector_id(code)
 def test_qa_preserves_partial_land_and_road_edges(self):
  fs,ev,qa,refs=r.load_artifacts()
  self.assertEqual(sum(len(x['pairs']) for x in qa.values()),19)
  self.assertTrue(all(shape(f['geometry']).is_valid for f in fs))
  land=next(p for p in qa['2.4']['pairs'] if p['official_attributes']['NUMERO']=='1133')
  self.assertLess(land['overlap_m2']/land['parcel_area_m2'],.7)
  self.assertEqual(sum(p['official_attributes']['GENRE_TXT']=='DP communal' for q in qa.values() for p in q['pairs']),6)
  self.assertTrue(all(q['outside_commune_m2']==0 for q in qa.values()))
 def test_semantics_and_failed_descriptor_matches_are_retained(self):
  fs,ev,qa,refs=r.load_artifacts()
  self.assertEqual(ev['2.4']['source_semantics']['current_za_ha'],0)
  self.assertIn('largement bâti',ev['2.11']['source_semantics']['label'])
  self.assertFalse(ev['2.11']['source_semantics']['vacancy_inference'])
  for e in ev.values():
   self.assertGreater(e['full_descriptor_fit']['holdout_rmse_m'],100)
   self.assertLess(e['maximum_consistent_control_error_m'],5)
   self.assertTrue(any(c['identity_review']=='wrong_identity' for c in e['controls']))
if __name__=='__main__':unittest.main()
