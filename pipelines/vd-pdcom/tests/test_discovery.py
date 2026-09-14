import sys
from pathlib import Path
import unittest
from unittest.mock import patch
from bs4 import BeautifulSoup
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import discovery

class DiscoveryContracts(unittest.TestCase):
    def test_names_match_census_without_canton_suffix(self):
        self.assertEqual(discovery.normalize('Chavannes-le-Chêne'),discovery.normalize('Chavannes-le-Chene'))
        self.assertEqual(discovery.normalize('Bussigny (VD)'),discovery.normalize('Bussigny'))
    def test_regulations_are_not_pdcom(self):
        self.assertFalse(discovery.PLAN.search('reglement communal des constructions.pdf'))
        self.assertTrue(discovery.PLAN.search('Plan directeur communal'))
        self.assertFalse(discovery.PLAN.search('PDC-Travaux-soumis-enquete-publique.pdf'))
    def test_private_addresses_rejected(self):
        with patch('discovery.socket.getaddrinfo',return_value=[(2,1,6,'',('127.0.0.1',443))]):
            with self.assertRaisesRegex(ValueError,'non_public_address'):
                discovery.public_url('https://example.ch')
    def test_empty_crawl_preserves_unresolved_evidence(self):
        detail=BeautifulSoup('<a href="https://www.example.ch">www.example.ch</a>','html.parser')
        home=BeautifulSoup('<a href="/reglement.pdf">Règlement communal</a>','html.parser')
        with patch('discovery.fetch_html',side_effect=[('https://www.ucv.ch/a',detail,'a'),('https://www.example.ch',home,'b')]):
            result=discovery.discover({'commune_bfs':5725,'directory_url':'https://www.ucv.ch/a'})
        self.assertEqual(result['candidates'],[])
        self.assertEqual(len(result['pages']),1)
        self.assertNotIn('confirmed_no_plan',str(result))
    def test_candidate_keeps_referring_page(self):
        detail=BeautifulSoup('<a href="https://www.example.ch">www.example.ch</a>','html.parser')
        home=BeautifulSoup('<a href="/plan.pdf">Plan directeur communal</a>','html.parser')
        with patch('discovery.fetch_html',side_effect=[('https://www.ucv.ch/a',detail,'a'),('https://www.example.ch',home,'b')]):
            result=discovery.discover({'commune_bfs':5725,'directory_url':'https://www.ucv.ch/a'})
        self.assertEqual(result['candidates'][0]['evidence_url'],'https://www.example.ch')
        self.assertEqual(result['candidates'][0]['review_status'],'pending')

    def test_html_base_resolves_relative_document(self):
        detail=BeautifulSoup('<a href="https://www.example.ch">www.example.ch</a>','html.parser')
        home=BeautifulSoup('<base href="https://www.example.ch/"><a href="uploads/plan.pdf">Plan directeur communal</a>','html.parser')
        with patch('discovery.fetch_html',side_effect=[('https://www.ucv.ch/a',detail,'a'),('https://www.example.ch/nested/page/',home,'b')]):
            result=discovery.discover({'commune_bfs':5725,'directory_url':'https://www.ucv.ch/a'})
        self.assertEqual(result['candidates'][0]['source_url'],'https://www.example.ch/uploads/plan.pdf')
