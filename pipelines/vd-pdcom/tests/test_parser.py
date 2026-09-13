import importlib.util
from pathlib import Path
import unittest
import sys
sys.path.insert(0,str(Path(__file__).parents[1]))

spec=importlib.util.spec_from_file_location('vd_parser',Path(__file__).parents[1]/'parser.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
HEADER='HistoricalCode,BfsCode,ValidFrom,ValidTo,Level,Parent,Name,ShortName,Inscription,Radiation,Rec_Type_fr,Rec_Type_de\n'
BASE='22,22,,,1,,Vaud,VD,,,,\n100,2222,,,2,22,District,District,,,,\n'

class RosterContracts(unittest.TestCase):
    def test_uses_bfs_not_historical_number(self):
        r=m.parse_roster(HEADER+BASE+'11800,5586,,,3,100,Lausanne,Lausanne,,,,\n')
        self.assertEqual(r[0]['commune_bfs'],5586)
        self.assertEqual(r[0]['historical_code'],11800)
    def test_canton_parent_chain_not_name(self):
        text=HEADER+BASE+'1,1,,,1,,Zurich,ZH,,,,\n2,12,,,2,1,District,District,,,,\n3,5586,,,3,2,Lausanne,Lausanne,,,,\n4,5642,,,3,100,Morges,Morges,,,,\n'
        self.assertEqual([r['commune_bfs'] for r in m.parse_roster(text)],[5642])
    def test_duplicates_block_census(self):
        with self.assertRaises(ValueError):m.parse_roster(HEADER+BASE+'3,5586,,,3,100,A,A,,,,\n4,5586,,,3,100,B,B,,,,\n')
    def test_empty_response_never_retires_every_commune(self):
        with self.assertRaises(ValueError):m.parse_roster(HEADER+BASE)
    def test_html_is_not_pdf(self):
        with self.assertRaises(ValueError):m.inspect_pdf(b'<html>not found</html>')
    def test_changed_pdf_needs_template_review(self):
        self.assertEqual(m.extract_candidates(b'changed bytes',5586)['status'],'template_review_required')

if __name__=='__main__':unittest.main()
