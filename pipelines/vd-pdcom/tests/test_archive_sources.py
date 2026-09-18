import io
import unittest
import warnings
import zipfile
from unittest.mock import patch
from acquire_sources import acquire, extract_archive_member

class ArchiveSourceTests(unittest.TestCase):
    def archive(self, entries):
        data=io.BytesIO()
        with warnings.catch_warnings(),zipfile.ZipFile(data,'w',zipfile.ZIP_DEFLATED) as z:
            warnings.simplefilter('ignore',UserWarning)
            for name,content in entries:z.writestr(name,content)
        return data.getvalue()

    def test_exact_member_only_and_provenance(self):
        raw=self.archive([('plan.pdf',b'%PDF-plan'),('other.pdf',b'%PDF-other')])
        with patch('acquire_sources.download',return_value=raw):
            pdf,evidence=acquire({'pdf_url':'https://example.org/annexes.zip','archive_member':'plan.pdf'})
        self.assertEqual(pdf,b'%PDF-plan')
        self.assertEqual(evidence['archive_member'],'plan.pdf')
        self.assertNotEqual(evidence['archive_sha256'],evidence['member_sha256'])

    def test_changed_or_duplicate_member_fails(self):
        for entries in [[('renamed.pdf',b'%PDF-x')],[('plan.pdf',b'%PDF-x'),('plan.pdf',b'%PDF-y')]]:
            with self.assertRaisesRegex(ValueError,'missing_or_ambiguous'):
                extract_archive_member(self.archive(entries),'plan.pdf')

    def test_uncompressed_limit(self):
        raw=self.archive([('plan.pdf',b'%PDF-'+b'x'*10000)])
        with self.assertRaisesRegex(ValueError,'size_limit'):extract_archive_member(raw,'plan.pdf',100)

    def test_non_pdf_and_unsafe_member(self):
        raw=self.archive([('plan.pdf',b'<html>')])
        with self.assertRaisesRegex(ValueError,'not_pdf'):extract_archive_member(raw,'plan.pdf')
        for name in ['/plan.pdf','../plan.pdf','folder/../plan.pdf']:
            with self.assertRaisesRegex(ValueError,'invalid_archive_member'):extract_archive_member(raw,name)

    def test_plain_pdf_unchanged(self):
        with patch('acquire_sources.download',return_value=b'%PDF-plain') as download:
            data,evidence=acquire({'pdf_url':'https://example.org/plan.pdf'})
        self.assertIsNone(evidence)
        download.assert_called_once_with('https://example.org/plan.pdf',100_000_000)
