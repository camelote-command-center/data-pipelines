import copy,sys,unittest
from pathlib import Path
from unittest.mock import MagicMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yvonand_equipment_green as a
class EquipmentGreenTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,change):
  b=copy.deepcopy(self.b);change(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_accepted_partial_batch(self):
  self.assertEqual([len(f['components'])for f in self.b['features']],[26,4]);self.assertEqual([len(f['expected_pairs'])for f in self.b['features']],[23,15]);self.assertEqual(len({p['egrid']for f in self.b['features']for p in f['expected_pairs']}),35)
 def test_source_hash(self):self.reject(lambda b:b.update(source_sha256='wrong'))
 def test_private_unknown_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_not_public(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_unknown_currentness(self):self.reject(lambda b:b['municipal_currentness'].update(classification='current'))
 def test_unchanged_registration(self):self.reject(lambda b:b['affine'].__setitem__(4,0))
 def test_unchanged_outlier_diagnostic(self):self.reject(lambda b:b['source3_reserved_diagnostic']['controls'].pop(5))
 def test_all_five_holds(self):self.reject(lambda b:b['exceptions'].pop())
 def test_no_whole_category_claim(self):self.reject(lambda b:b['features'][0].update(category_complete=True))
 def test_literal_existing_not_projected_green(self):self.reject(lambda b:b['features'][1].update(literal_legend='Espace public de verdure à créer'))
 def test_no_excluded_tent(self):self.reject(lambda b:b['features'][0]['path_ids'].append(5478))
 def test_full_fill_denominator(self):self.reject(lambda b:b['full_selected_fill_classification'].pop())
 def test_native_path_identity(self):self.reject(lambda b:b['features'][0]['components'][0]['source_record'].update(fill=[0,1,0]))
 def test_no_source_clip(self):self.reject(lambda b:b['features'][0]['components'][0].update(source_clipped=True))
 def test_no_visible_area_claim(self):self.reject(lambda b:b['features'][1]['components'][0].update(visible_fill_area_claim=True))
 def test_no_hull_shortcut(self):self.reject(lambda b:b['features'][0]['components'][0].update(whole_training_hull=False))
 def test_union_not_first_component(self):self.reject(lambda b:b['features'][0].update(geometry_lv95=b['features'][0]['components'][0]['geometry_lv95']))
 def test_no_tiny_overlap_filter(self):self.reject(lambda b:b['features'][1]['expected_pairs'].pop())
 def test_ddp_kind_preserved(self):
  def change(b):next(p for p in b['features'][1]['expected_pairs']if p['cadastral_object_kind']=='ddp_superficie')['cadastral_object_kind']='bien_fonds'
  self.reject(change)
 def test_boundary_review_required(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0].update(boundary_review_required=False))
 def test_no_contextual_rights(self):self.reject(lambda b:b['features'][1]['expected_pairs'][0].update(contextual_only=False))
 def test_source_exact_one_page(self):
  conn=MagicMock();conn.cursor.return_value.__enter__.return_value.fetchone.return_value=(a.SHA,2)
  with self.assertRaisesRegex(ValueError,'stored_source'):a.persist(conn,a.DOC,a.SHA)
 def test_wrong_source_noop(self):self.assertIsNone(a.persist(None,a.DOC,'wrong'))
 def test_currentness_gate_unchanged(self):
  from active_qualifications import currentness_blockers
  self.assertEqual(currentness_blockers({'validation_evidence':{'municipal_currentness':self.b['municipal_currentness']}}),['live_currentness_policy_reconciliation_required'])
 def test_replay_rejects_scalar_drift(self):
  conn=MagicMock();c=conn.cursor.return_value.__enter__.return_value;f=self.b['features'][0]
  label='Yvonand 2008 — support historique indicatif, surpeints conservés, hors droits à bâtir — '+f['label']
  c.fetchone.side_effect=[(a.SHA,1),(0,),(0,),(0,),(True,True),(label,'review_required',None,None,True,False)]
  c.fetchall.return_value=[(p['egrid'],)for p in f['expected_pairs']]
  with patch.object(a,'refresh',return_value={'snapshot_id':'00000000-0000-0000-0000-000000000001'}):
   with self.assertRaisesRegex(ValueError,'existing_sector_changed'):a.persist(conn,a.DOC,a.SHA)
if __name__=='__main__':unittest.main()
