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

    def crawl_home(self, home, following=()):
        detail=BeautifulSoup('<a href="https://www.example.ch">www.example.ch</a>','html.parser')
        with patch('discovery.fetch_html',side_effect=[('https://www.ucv.ch/a',detail,'a'),('https://www.example.ch',home,'b'),*following]):
            return discovery.discover({'commune_bfs':5716,'directory_url':'https://www.ucv.ch/a'})

    def test_embedded_table_plan_link_and_numeric_download(self):
        import json
        home=BeautifulSoup('<table></table>','html.parser')
        home.table['data-entities']=json.dumps({'data':[{'name':'Plan directeur communal','_downloadBtn':'<a href="/_rte/publikation/123">Téléchargement</a>'}]})
        page=BeautifulSoup('<h1>Plan directeur communal</h1><a href="/_doc/456">Téléchargement</a>','html.parser')
        result=self.crawl_home(home,[('https://www.example.ch/formulaires/123',page,'c')])
        by_url={c['source_url']:c for c in result['candidates']}
        self.assertIn('https://www.example.ch/_rte/publikation/123',by_url)
        self.assertEqual(by_url['https://www.example.ch/_doc/456']['evidence_url'],'https://www.example.ch/formulaires/123')
        self.assertEqual(by_url['https://www.example.ch/_doc/456']['kind'],'landing')
        self.assertEqual(result['errors'],[])

    def test_embedded_regulation_is_not_pdcom(self):
        import json
        page=BeautifulSoup('<table></table>','html.parser')
        page.table['data-entities']=json.dumps({'data':[{'name':'Plan de zones 1999','_downloadBtn':'<a href="/_doc/123">Téléchargement</a>'}]})
        self.assertEqual(self.crawl_home(page)['candidates'],[])

    def test_bad_embedded_data_does_not_hide_normal_links(self):
        page=BeautifulSoup('<a href="/pdcom.pdf">PDCom</a><table data-entities="broken"></table>','html.parser')
        result=self.crawl_home(page)
        self.assertEqual(len(result['candidates']),1)
        self.assertEqual(result['errors'][0]['reason'],'invalid_embedded_json')

    def test_embedded_limits_and_unsafe_urls(self):
        import json
        page=BeautifulSoup('<table></table>','html.parser')
        page.table['data-entities']=json.dumps({'data':[{'name':'PDCom','_downloadBtn':'<a href="javascript:alert(1)">Plan</a>'}]*1001})
        result=self.crawl_home(page)
        self.assertEqual(result['candidates'],[])
        self.assertEqual(result['errors'][0]['reason'],'embedded_row_limit')
        page.table['data-entities']='x'*500001
        self.assertEqual(self.crawl_home(page)['errors'][0]['reason'],'embedded_data_size_limit')

    def test_contended_transaction_lock_is_released_and_connections_closed(self):
        from unittest.mock import MagicMock
        from types import SimpleNamespace
        import tempfile
        main,lock=MagicMock(),MagicMock()
        lock.cursor.return_value.__enter__.return_value.fetchone.return_value=(False,)
        main.cursor.return_value.__enter__.return_value.fetchone.return_value=(300,)
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ',{'RE_LLM_DB_URL':'test-only'}), patch('discovery.psycopg2.connect',side_effect=[main,lock]):
            result=discovery.run(SimpleNamespace(output=Path(tmp)))
        self.assertEqual(result['skipped'],'discovery_already_running')
        lock.cursor.return_value.__enter__.return_value.execute.assert_called_once_with('SELECT pg_try_advisory_xact_lock(572500300)')
        lock.commit.assert_not_called()
        lock.rollback.assert_called_once()
        lock.close.assert_called_once()
        main.close.assert_called_once()

    def test_acquired_lock_released_when_discovery_fails(self):
        from unittest.mock import MagicMock
        from types import SimpleNamespace
        import tempfile
        main,lock=MagicMock(),MagicMock()
        lock.cursor.return_value.__enter__.return_value.fetchone.return_value=(True,)
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ',{'RE_LLM_DB_URL':'test-only'}), patch('discovery.psycopg2.connect',side_effect=[main,lock]), patch('discovery.fetch_html',side_effect=RuntimeError('source unavailable')):
            with self.assertRaisesRegex(RuntimeError,'source unavailable'):
                discovery.run(SimpleNamespace(output=Path(tmp)))
        lock.commit.assert_not_called()
        lock.rollback.assert_called_once()
        lock.close.assert_called_once()
        main.close.assert_called_once()
