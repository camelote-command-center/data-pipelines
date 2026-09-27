import copy,unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yvonand_existing_vocations as a
class HistoricalVocationTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.b=a.load_artifacts()
 def reject(self,fn):
  b=copy.deepcopy(self.b);fn(b)
  with self.assertRaises(ValueError):a.validate(b)
 def test_scope(self):
  self.assertEqual(len(self.b['features']),4)
  self.assertEqual(sum(len(f['source_paths']) for f in self.b['features']),8)
  self.assertEqual(sum(len(f['expected_pairs']) for f in self.b['features']),163)
 def test_changed_source(self):self.reject(lambda b:b.update(source_sha256='wrong'))
 def test_public(self):self.reject(lambda b:b.update(publication_status='public'))
 def test_precision(self):self.reject(lambda b:b.update(source_precision_m=1))
 def test_filtered_residual_not_accuracy(self):self.reject(lambda b:b['alignment'].update(holdout_rmse_m=.524))
 def test_refit(self):self.reject(lambda b:b['alignment'].update(fit_changed=True))
 def test_source_rotation(self):self.reject(lambda b:b['native_pdf_contract'].update(page_rotation=0))
 def test_unchanged_affine(self):self.reject(lambda b:b['affine'].__setitem__(4,2544325))
 def test_failure_ledger_retained(self):self.reject(lambda b:b['full_source_identity_ledger']['rows'].pop())
 def test_original_roles(self):self.reject(lambda b:b['full_source_identity_ledger']['rows'][0].update(role='other'))
 def test_independent_decision(self):self.reject(lambda b:b['independent_qa'].update(decision='pending'))
 def test_current_rights_rejected(self):self.reject(lambda b:b['features'][0]['policy_status'].update(state='current_zoning'))
 def test_legend_mutation(self):self.reject(lambda b:b['features'][0].update(label='Development capacity'))
 def test_missing_path(self):self.reject(lambda b:b['features'][0]['path_ids'].pop())
 def test_source_transform(self):self.reject(lambda b:b['features'][0]['source_paths'][0].update(geometry_lv95_research_only={'type':'Polygon','coordinates':[[[0,0],[1,0],[1,1],[0,0]]]}))
 def test_hull_flag(self):self.reject(lambda b:b['features'][0]['source_paths'][0].update(whole_visually_reviewed_training_hull=False))
 def test_invalid_reference(self):self.reject(lambda b:b['features'][0].update(invalid_reference_envelope_overlap=['x']))
 def test_object_type(self):self.reject(lambda b:b['features'][0]['expected_pairs'][0]['attributes'].update(GENRE_TXT='unknown'))
 def test_noop_other_source(self):self.assertIsNone(a.persist(None,a.DOC,'other'))
 def test_tile_conflict_before_sector(self):
  from unittest.mock import MagicMock,patch
  conn=MagicMock();c=conn.cursor.return_value.__enter__.return_value;c.fetchone.side_effect=[(a.SHA,1),(1,)]
  with patch.object(a,'refresh',return_value={'snapshot_id':'00000000-0000-0000-0000-000000000001'}):
   with self.assertRaisesRegex(ValueError,'conflicting_tile'):a.persist(conn,a.DOC,a.SHA)
  self.assertFalse(any('INSERT INTO bronze_ch.vd_pdcom_sectors' in z.args[0] for z in c.execute.call_args_list))
if __name__=='__main__':unittest.main()
