import copy,sys,unittest
from pathlib import Path
from unittest.mock import MagicMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yvonand_commercial_support as a
class CommercialSupportTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,change):
  b=copy.deepcopy(self.b);change(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_complete_scope(self):
  self.assertEqual([f['drawing_index']for f in self.b['features']],[18,65]);self.assertEqual(len(self.b['expected_pairs']),163)
 def test_precision_unknown(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_not_public(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_no_current_rights(self):self.reject(lambda b:b['currentness'].update(current_zoning_established=True))
 def test_literal_semantics(self):self.reject(lambda b:b.update(category_literal='Building capacity'))
 def test_no_partial_component(self):self.reject(lambda b:b['features'].pop())
 def test_no_paint_repair(self):self.reject(lambda b:b['features'][0].update(subtraction_or_repair_performed=True))
 def test_no_visible_pixel_claim(self):self.reject(lambda b:b['features'][0].update(visible_paint_area_claim=True))
 def test_no_refit(self):self.reject(lambda b:b['registration'].update(new_geographic_fit=True))
 def test_prior_denominators_preserved(self):self.reject(lambda b:b['registration'].update(source7_identity_ledger_denominator=19))
 def test_control_identity_preserved(self):self.reject(lambda b:b['registration']['controls'][0].update(original_role='reserved'))
 def test_native_paint_unchanged(self):self.reject(lambda b:b['features'][0]['original_paint_record'].update(fill=[0,1,0]))
 def test_whole_union_required(self):self.reject(lambda b:b.update(geometry_lv95_research_only=b['features'][0]['geometry_lv95_research_only']))
 def test_complete_pair_membership(self):self.reject(lambda b:b['expected_pairs'].pop())
 def test_official_geometry_identity(self):self.reject(lambda b:b['expected_pairs'][0]['attributes'].update(GENRE_TXT='unknown'))
 def test_source_mismatch_noop(self):self.assertIsNone(a.persist(None,a.DOC,'wrong'))
 def test_source_exact_single_page(self):
  conn=MagicMock();conn.cursor.return_value.__enter__.return_value.fetchone.return_value=(a.SHA,2)
  with self.assertRaisesRegex(ValueError,'stored_source'):a.persist(conn,a.DOC,a.SHA)
 def test_source7_diagnostic_not_accuracy(self):self.reject(lambda b:b['alignment'].update(holdout_rmse_m=.524))
 def test_filtered_reserved_outlier_retained(self):self.reject(lambda b:b['source3_reserved_diagnostic']['controls'].pop(5))
 def test_no_inherited_source7_scalar(self):self.reject(lambda b:b['source3_reserved_diagnostic'].update(rmse_m=.5241450954613012))
 def test_unknown_currentness_blocks_future_qualification(self):
  from active_qualifications import currentness_blockers
  self.assertEqual(currentness_blockers({'validation_evidence':{'municipal_currentness':self.b['municipal_currentness']}}),['live_currentness_policy_reconciliation_required'])
if __name__=='__main__':unittest.main()
