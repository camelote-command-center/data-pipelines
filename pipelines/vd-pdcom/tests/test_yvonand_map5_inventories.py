import copy,sys,unittest
from pathlib import Path
from unittest.mock import MagicMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yvonand_map5_inventories as a
class Map5Tests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_accepted(self):
  self.assertEqual([len(f['components'])for f in self.b['features']],[34,2,1]);self.assertEqual([len(f['expected_pairs'])for f in self.b['features']],[36,55,1])
 def test_source_hash(self):self.reject(lambda b:b.update(source_sha256='wrong'))
 def test_unknown_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_private(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_currentness(self):self.reject(lambda b:b['municipal_currentness'].update(classification='current'))
 def test_inventory_authority(self):self.reject(lambda b:b['source_native_contract'].update(literal_inventory_warning='authoritative'))
 def test_frame(self):self.reject(lambda b:b['registration']['frame'].update(scale=1))
 def test_no_dropped_control(self):self.reject(lambda b:b['registration']['controls'].pop())
 def test_no_control_reselection(self):self.reject(lambda b:b['registration']['controls'][0].update(source5_path=841))
 def test_actual_source5_scalar(self):self.reject(lambda b:b['source5_reserved_diagnostic'].update(rmse_m=.5241450954613012))
 def test_reserved_denominator(self):self.reject(lambda b:b['source5_reserved_diagnostic'].update(original_denominator=8))
 def test_whole_holds(self):self.reject(lambda b:b['whole_geometry_holds'].pop())
 def test_native_invalid_hold(self):self.reject(lambda b:b['native_exceptions'].pop())
 def test_no_category_completeness(self):self.reject(lambda b:b['features'][0].update(category_complete=True))
 def test_literal_category(self):self.reject(lambda b:b['features'][1].update(literal_legend='Projeté'))
 def test_no_visible_pigment_claim(self):self.reject(lambda b:b['source_native_contract']['components'][0].update(visible_paint_area=True))
 def test_pattern_children(self):self.reject(lambda b:b['source_native_contract']['components'][0]['paint_children'].pop())
 def test_union(self):self.reject(lambda b:b['features'][0].update(geometry_lv95=b['features'][0]['components'][0]['geometry_lv95']))
 def test_no_tiny_overlap_filter(self):self.reject(lambda b:b['features'][0]['expected_pairs'].pop())
 def test_reference_count(self):self.reject(lambda b:b['official_reference'].update(count=665))
 def test_context_only(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0].update(contextual_only=False))
 def test_boundary_heuristic(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0].update(uncertainty_buffer_m=1))
 def test_no_false_ddp(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0].update(cadastral_object_kind='ddp_superficie'))
 def test_exact_one_page(self):
  co=MagicMock();co.cursor.return_value.__enter__.return_value.fetchone.return_value=(a.SHA,2)
  with self.assertRaisesRegex(ValueError,'stored_source'):a.persist(co,a.DOC,a.SHA)
 def test_wrong_source_noop(self):self.assertIsNone(a.persist(None,a.DOC,'wrong'))
 def test_qualification_currentness_gate(self):
  from active_qualifications import currentness_blockers
  self.assertEqual(currentness_blockers({'validation_evidence':{'municipal_currentness':self.b['municipal_currentness']}}),['live_currentness_policy_reconciliation_required'])
 def test_replay_scalar_drift(self):
  co=MagicMock();c=co.cursor.return_value.__enter__.return_value;f=self.b['features'][0];label='Yvonand 2008 — inventaire indicatif partiel, surpeints conservés ; seuls les plans originaux font foi — '+f['label'];c.fetchone.side_effect=[(a.SHA,1),(0,),(0,),(0,),(True,True),(label,'review_required',None,None,True,False)];c.fetchall.return_value=[(p['egrid'],)for p in f['expected_pairs']]
  with patch.object(a,'refresh',return_value={'snapshot_id':'00000000-0000-0000-0000-000000000001'}):
   with self.assertRaisesRegex(ValueError,'existing_sector_changed'):a.persist(co,a.DOC,a.SHA)
if __name__=='__main__':unittest.main()
