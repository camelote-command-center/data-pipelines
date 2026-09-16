import sys
from pathlib import Path
import unittest
from unittest.mock import patch, ANY
import requests
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from acquire_sources import download

class DownloadRetries(unittest.TestCase):
    def http_error(self,status):
        response=requests.Response();response.status_code=status
        return requests.HTTPError(response=response)
    @patch('acquire_sources.time.sleep')
    @patch('acquire_sources._download_once')
    def test_transient_failure_then_success(self,once,sleep):
        once.side_effect=[self.http_error(503),b'%PDF-data']
        self.assertEqual(download('https://example.org/map.pdf',150000000),b'%PDF-data')
        self.assertEqual(once.call_count,2)
        once.assert_called_with('https://example.org/map.pdf',150000000,ANY)
        sleep.assert_called_once_with(2)
    @patch('acquire_sources.time.sleep')
    @patch('acquire_sources._download_once')
    def test_timeout_is_bounded(self,once,sleep):
        once.side_effect=requests.Timeout()
        with self.assertRaises(requests.Timeout):download('https://example.org/map.pdf')
        self.assertEqual(once.call_count,3)
        self.assertEqual([c.args[0] for c in sleep.call_args_list],[2,4])
    @patch('acquire_sources.time.sleep')
    @patch('acquire_sources._download_once')
    def test_permanent_http_and_validation_failures_not_retried(self,once,sleep):
        for error in [self.http_error(403),self.http_error(404),ValueError('unreviewed_redirect_host'),ValueError('pdf_size_limit')]:
            once.reset_mock();once.side_effect=error
            with self.assertRaises(type(error)):download('https://example.org/map.pdf')
            self.assertEqual(once.call_count,1)
        sleep.assert_not_called()

    @patch('acquire_sources.time.sleep')
    @patch('acquire_sources._download_once')
    def test_chunk_interruption_retries_with_same_deadline(self,once,sleep):
        once.side_effect=[requests.exceptions.ChunkedEncodingError(),b'%PDF-ok']
        self.assertEqual(download('https://example.org/map.pdf'),b'%PDF-ok')
        self.assertEqual(once.call_args_list[0].args[2],once.call_args_list[1].args[2])
    @patch('acquire_sources.time.monotonic',side_effect=[0,181])
    @patch('acquire_sources._download_once')
    def test_elapsed_budget_prevents_attempt(self,once,clock):
        with self.assertRaisesRegex(requests.Timeout,'pdf_download_deadline'):download('https://example.org/map.pdf')
        once.assert_not_called()
    @patch('acquire_sources.public_url')
    @patch('acquire_sources.requests.get')
    @patch('acquire_sources.time.monotonic',side_effect=[0,181])
    def test_trickle_stream_is_bounded(self,clock,get,public):
        from acquire_sources import _download_once
        response=get.return_value.__enter__.return_value
        response.is_redirect=False
        response.iter_content.return_value=iter([b'%PDF-data'])
        with self.assertRaisesRegex(requests.Timeout,'pdf_download_deadline'):_download_once('https://example.org/map.pdf',deadline=180)
