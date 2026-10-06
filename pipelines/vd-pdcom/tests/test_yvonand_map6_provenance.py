import copy,sys,unittest
from pathlib import Path
from unittest.mock import MagicMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yvonand_map6_provenance as a
class Map6Tests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_accepted(self):
  self.assertEqual([len(f['components'])for f in self.b['features']],[28,41,3,10,4,1]);self.assertEqual(sum(len(f['expected_pairs'])for f in self.b['features']),831)
 def test_source_hash(self):self.reject(lambda b:b.update(source_sha256='wrong'))
 def test_unknown_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_private(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_currentness(self):self.reject(lambda b:b['municipal_currentness'].update(classification='current'))
 def test_nonadditive_disclosure(self):self.reject(lambda b:b['prior_source7_overlap'].update(warning='new physical area'))
 def test_overlap_metrics(self):self.reject(lambda b:b['prior_source7_overlap']['rows'][0].update(intersection_m2=0))
 def test_frame(self):self.reject(lambda b:b['registration']['frame'].update(scale=1))
 def test_no_dropped_control(self):self.reject(lambda b:b['registration']['controls'].pop())
 def test_no_control_reselection(self):self.reject(lambda b:b['registration']['controls'][0].update(source6_path=1783))
 def test_actual_source6_scalar(self):self.reject(lambda b:b['source6_reserved_diagnostic'].update(rmse_m=.5905953528792778))
 def test_reserved_denominator(self):self.reject(lambda b:b['source6_reserved_diagnostic'].update(original_denominator=8))
 def test_whole_holds(self):self.reject(lambda b:b['whole_geometry_holds'].pop())
 def test_full_paint_denominator(self):self.reject(lambda b:b['native_batches'][0]['full_selected_fill_classification'].pop())
 def test_projected_fragment_hold(self):self.reject(lambda b:b['native_batches'][1]['projected_exceptions'].pop())
 def test_existing_projected_status(self):self.reject(lambda b:b['features'][0].update(source_status='Projeté'))
 def test_no_category_completeness(self):self.reject(lambda b:b['features'][0].update(category_complete=True))
 def test_literal_category(self):self.reject(lambda b:b['features'][1].update(literal_legend='Projeté'))
 def test_union(self):self.reject(lambda b:b['features'][0].update(geometry_lv95=b['features'][0]['components'][0]['geometry_lv95']))
 def test_no_tiny_overlap_filter(self):self.reject(lambda b:b['features'][0]['expected_pairs'].pop())
 def test_original_tile_count(self):self.reject(lambda b:b['original_count_responses'][0]['response'].update(count=1))
 def test_context_only(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0].update(contextual_only=False))
 def test_boundary_heuristic(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0].update(uncertainty_buffer_m=1))
 def test_ddp_not_land(self):
  def change(b):next(p for f in b['features']for p in f['expected_pairs']if p['cadastral_object_kind']=='ddp_superficie')['cadastral_object_kind']='bien_fonds'
  self.reject(change)
 def test_exact_one_page(self):
  co=MagicMock();co.cursor.return_value.__enter__.return_value.fetchone.return_value=(a.SHA,2)
  with self.assertRaisesRegex(ValueError,'stored_source'):a.persist(co,a.DOC,a.SHA)
 def test_wrong_source_noop(self):self.assertIsNone(a.persist(None,a.DOC,'wrong'))
 def test_qualification_currentness_gate(self):
  from active_qualifications import currentness_blockers
  self.assertEqual(currentness_blockers({'validation_evidence':{'municipal_currentness':self.b['municipal_currentness']}}),['live_currentness_policy_reconciliation_required'])
 def test_replay_scalar_drift(self):
  co=MagicMock();c=co.cursor.return_value.__enter__.return_value;f=self.b['features'][0];label='Yvonand 2008 — provenance indicative partielle, surpeints conservés, surfaces non additives avec feuille 7 — '+f['label'];c.fetchone.side_effect=[(a.SHA,1),(0,),(0,),(0,),(True,True),(label,'review_required',None,None,True,False)];c.fetchall.return_value=[(p['egrid'],)for p in f['expected_pairs']]
  with patch.object(a,'refresh',return_value={'snapshot_id':'00000000-0000-0000-0000-000000000001'}):
   with self.assertRaisesRegex(ValueError,'existing_sector_changed'):a.persist(co,a.DOC,a.SHA)
if __name__=='__main__':unittest.main()
