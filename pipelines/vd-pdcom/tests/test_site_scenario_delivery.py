import copy,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from site_scenario_delivery import validate_rows

class ScenarioDeliveryContract(unittest.TestCase):
    def setUp(self):
        p=Path(__file__).resolve().parents[1]/'reviewed_batches/a2386483-8861-5e98-8631-9ea1adb6dca3.json'
        b=json.loads(p.read_text());m=b['document_annotations']['reviewed_operational_metrics']
        self.rows=[dict(document_id=b['document_id'],site_id=r['site_id'],scenario=r['scenario'],commune_bfs=r['commune_bfs'],page_number=r['source_page'],review_status='historical_scenario_not_current_capacity',publication_status='internal_review_only',source_evidence={'record':r,'limits':m['limits']}) for r in m['records']]
    def test_missing_duplicate_or_released_data_fails_closed(self):
        validate_rows(self.rows)
        with self.assertRaises(ValueError):validate_rows(self.rows[:-1])
        bad=copy.deepcopy(self.rows);bad[1]=bad[0]
        with self.assertRaises(ValueError):validate_rows(bad)
        for k,v in [('publication_status','released'),('commune_bfs',9999),('review_status','validated')]:
            bad=copy.deepcopy(self.rows);bad[0][k]=v
            with self.assertRaises(ValueError):validate_rows(bad)
    def test_removing_limits_fails_closed(self):
        self.rows[0]['source_evidence']['limits']=[]
        with self.assertRaises(ValueError):validate_rows(self.rows)
