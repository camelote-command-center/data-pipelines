import json,sys,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import parser,morges_pilot
class PilotSpatialRouting(unittest.TestCase):
    def test_morges_map_is_owned_by_pilot_stage(self):
        pilots=json.loads((parser.ROOT/'pilot_sources.json').read_text());reviewed=json.loads((parser.ROOT/'reviewed_sources.json').read_text())
        source=next(s for s in pilots if s['commune_bfs']==5642)
        self.assertFalse(any(s['pdf_url']==source['pdf_url'] for s in reviewed))
        with patch.object(parser,'persist_morges_pilot',return_value={'sectors':6,'parcel_pairs':62}) as refresh:
            result=parser.persist_pilot_spatial(None,5642,morges_pilot.DOC,morges_pilot.SHA)
            refresh.assert_called_once_with(None,morges_pilot.DOC,morges_pilot.SHA)
            self.assertEqual(result['morges_pilot']['parcel_pairs'],62)
    def test_other_pilot_does_not_run_morges(self):
        with patch.object(parser,'persist_morges_pilot',side_effect=AssertionError),patch.object(parser,'persist_spatial_pilot',return_value={'sectors':15}) as prangins:
            self.assertEqual(parser.persist_pilot_spatial(None,5586,'lausanne','sha'),{})
            self.assertEqual(parser.persist_pilot_spatial(None,5725,'prangins','sha'),{'spatial_pilot':{'sectors':15}})
            prangins.assert_called_once_with(None,'prangins','sha')
