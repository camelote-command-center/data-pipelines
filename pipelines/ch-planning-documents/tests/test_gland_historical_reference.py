import copy
import hashlib
import json
import unittest
import psycopg2
from pathlib import Path
from unittest.mock import MagicMock, patch
import _pipeline_path  # noqa: F401
import historical_reference as h
import gland_historical_reference_runner as runner
import vd_pdcom_text as bridge


class GlandHistoricalReferenceTests(unittest.TestCase):
    def setUp(self):
        self.bundles = [json.loads((runner.BATCH_ROOT / (s + '-bundle.json')).read_text()) for s in runner.SLUGS]

    def test_exact_two_sources_reassemble_and_deliver_with_visible_caveats(self):
        self.assertEqual(len(h.PINS), 13)
        self.assertEqual(sum(len(b['chunks']) for b in self.bundles), 41)
        self.assertIn(h.SOURCE, bridge.TEXT_SOURCES)
        for b in self.bundles:
            self.assertEqual(bridge.assemble(b['review'], b['pages'], b['byte_count']), b)
            h.validate_delivery(b['metadata'], b['chunks'])
            self.assertEqual(b['metadata']['plan_status'], 'unverified')
            self.assertEqual(b['metadata']['historical_reference']['current_applicability'], 'unverified')
            for chunk in b['chunks']:
                self.assertTrue(chunk['content'].startswith(b['review']['historical_reference']['human_readable_caveat'] + '\n\n'))
                self.assertIn('#page=' + str(chunk['page_number']), chunk['metadata']['citation'])
        self.assertEqual(sum(len(b['pages']) for b in self.bundles), 102)
        self.assertEqual(sum(not p['include_in_search'] for b in self.bundles for p in b['pages']), 61)
        for b in self.bundles:
            self.assertEqual(b['review']['commune_bfs'], 5721)
            for p in b['pages']:
                if p['include_in_search']:
                    chunk = next(c for c in b['chunks'] if c['page_number'] == p['page_number'])
                    self.assertEqual(chunk['content'].split('\n\n', 1)[1], p['text'])
                    self.assertIn('already realized', ' '.join(chunk['metadata']['limits']))

    def test_source_review_scope_status_and_representation_changes_fail_closed(self):
        for b in self.bundles:
            for field, value in [('vd_document_id', '00000000-0000-0000-0000-000000000001'), ('source_sha256', '0' * 64),
                                 ('plan_status', 'approved'), ('source_role', 'approved_main'), ('commune_bfs', 5892),
                                 ('signed_approval', {}), ('limits', []), ('scope', 'intercommunal'),
                                 ('native_page_selection', {'mode': 'reviewed_existing_pdf_text'})]:
                r = copy.deepcopy(b['review']); r[field] = value
                with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'historical_reference_'):
                    bridge.validate_review(r)
            for field, value in [('current_applicability', 'current'), ('source_status_promotion', True),
                                 ('human_readable_caveat', ''), ('selected_pages', [1]), ('fully_certified_pages', 44)]:
                r = copy.deepcopy(b['review']); r['historical_reference'][field] = value
                with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'historical_reference_'):
                    bridge.validate_review(r)

    def test_page_changes_held_content_missing_pages_and_wrong_representation_rejected(self):
        for b in self.bundles:
            selected = next(i for i, p in enumerate(b['pages']) if p['include_in_search'])
            held = next(i for i, p in enumerate(b['pages']) if not p['include_in_search'])
            for index, field, value in [(selected, 'text', 'changed'), (held, 'text', 'invented searchable map text'),
                                        (held, 'include_in_search', True), (selected, 'representation', 'author_native'),
                                        (selected, 'confidence', 99.0)]:
                pages = copy.deepcopy(b['pages']); pages[index][field] = value
                with self.assertRaisesRegex(ValueError, 'historical_reference_pages_not_exact'):
                    bridge.assemble(b['review'], pages, b['byte_count'])
            with self.assertRaisesRegex(ValueError, 'historical_reference_pages_not_exact'):
                bridge.assemble(b['review'], b['pages'][:-1], b['byte_count'])

    def test_delivery_detects_content_labels_chunk_order_and_metadata_tampering(self):
        b = self.bundles[0]
        mutations = [('content', 'claim of present capacity'), ('page_number', 42), ('chunk_index', 99),
                     ('id', '00000000-0000-0000-0000-000000000001')]
        for field, value in mutations:
            chunks = copy.deepcopy(b['chunks']); chunks[0][field] = value
            with self.assertRaisesRegex(ValueError, 'historical_reference_chunk_content'):
                h.validate_delivery(b['metadata'], chunks)
        chunks = copy.deepcopy(b['chunks']); chunks[0]['metadata']['limits'] = []
        with self.assertRaisesRegex(ValueError, 'historical_reference_chunk_content'):
            h.validate_delivery(b['metadata'], chunks)
        with self.assertRaisesRegex(ValueError, 'historical_reference_chunk_content'):
            h.validate_delivery(b['metadata'], b['chunks'][:-1])
        doc = copy.deepcopy(b['metadata']); doc['historical_reference']['territory']['commune_bfs'] = 9999
        with self.assertRaises(ValueError): h.validate_delivery(doc, b['chunks'])

    def test_delivery_cannot_hide_reference_flag_and_relabel_as_current(self):
        b = self.bundles[0]
        for source, drop in [(bridge.SOURCE, False), (bridge.SOURCE, True), (h.SOURCE, True)]:
            conn = MagicMock(); cur = conn.cursor.return_value.__enter__.return_value
            meta = copy.deepcopy(b['metadata'])
            if drop: meta.pop('historical_reference')
            cur.fetchone.return_value = (source, meta)
            with self.assertRaisesRegex(ValueError, 'historical_reference_source_kind_mismatch'):
                bridge.deliver(conn, b['document_id'])
            self.assertFalse(any('UPDATE' in str(c) or 'INSERT' in str(c) for c in cur.execute.call_args_list))

    def test_exact_mapping_requires_unverified_tracking_only(self):
        for b in self.bundles:
            expected = h.PINS[b['review']['vd_document_id']]['tracking_bfs']
            cur = MagicMock(); cur.fetchall.return_value = [(expected, 'unverified')]
            h.validate_registered_mapping(cur, b['review'])
            for rows in [[(expected, 'full')], [(5881, 'unverified')], [(expected, 'unverified'), (9999, 'unverified')]]:
                cur.fetchall.return_value = rows
                with self.assertRaisesRegex(ValueError, 'historical_reference_registry_mapping_changed'):
                    h.validate_registered_mapping(cur, b['review'])

    def test_frozen_artifact_hash_checked_before_parsing(self):
        r = self.bundles[0]['review']
        original = Path.read_bytes
        def changed(path):
            return b'{}' if path.name.endswith('-representations.json') else original(path)
        with patch('pathlib.Path.read_bytes', new=changed):
            with self.assertRaisesRegex(ValueError, 'historical_reference_artifact_changed'): h.evidence(r)

    def test_independent_review_evidence_hash_required(self):
        with patch('pathlib.Path.read_bytes', return_value=b'{}'):
            with self.assertRaisesRegex(ValueError, 'historical_reference_independent_review_changed'):
                h.evidence(self.bundles[0]['review'])

    def test_original_layer_and_runtime_checked(self):
        b = self.bundles[0]; e = h.evidence(b['review'])
        pages = []
        for p in e['pages']:
            page = MagicMock(); page.get_text.return_value = 'wrong original text'
            page.rect.width = p['width']; page.rect.height = p['height']; pages.append(page)
        with self.assertRaisesRegex(ValueError, 'historical_reference_original_layer_changed'):
            h.load_pages(pages, b['review'])

    def test_independent_review_exact_selected_hashes(self):
        qa = json.loads((runner.BATCH_ROOT / 'independent-content-review.json').read_text())
        for b, slug in zip(self.bundles, ('bilan', 'mesures')):
            accepted = qa['sources'][slug]
            self.assertEqual(b['review']['source_sha256'], accepted['source_sha256'])
            self.assertEqual(b['review']['historical_reference']['selected_pages'], accepted['selected_pages'])
            ledger = {p['physical_page']: p for p in accepted['page_ledger']}
            for p in b['pages']:
                self.assertEqual(p['include_in_search'], ledger[p['page_number']]['include_in_search'])
                self.assertEqual(p['original_representation_text_sha256'], ledger[p['page_number']]['raw_ocr_sha256'])
                if p['include_in_search']:
                    self.assertEqual(p['text_sha256'], ledger[p['page_number']]['raw_ocr_sha256'])
                else:
                    self.assertEqual(p['text'], '')

    def test_runner_owner_rejects_nested_commit_and_hashes_code_and_all_source_kinds(self):
        with self.assertRaisesRegex(RuntimeError, 'batch owner'):
            runner.OwnedTransaction(MagicMock()).commit()
        hashes = runner.artifact_hashes(self.bundles)
        self.assertIn(h.SOURCE, hashes['source_kinds'])
        self.assertEqual(set(hashes['implementation']), {'historical_reference.py', 'gland_historical_reference_runner.py', 'vd_pdcom_text.py'})
        self.assertEqual(runner.BASELINE, (15, 1345)); self.assertEqual(runner.FINAL, (17, 1386))

    def test_commit_acknowledgement_loss_records_unknown_without_failed_rollback_claim(self):
        root = Path('/Users/a/LLM_Work/re-llm/vaud-pdcom/oct6-next-original-text')
        ops = {s: {'commit': s + '-op', 'rehearse': s + '-rehearsal'} for s in runner.SLUGS}
        ops['monitor'] = 'monitor-op'
        before = {'guard': 'unchanged'}; hashes = {'code': 'frozen'}; datasets = [{'dataset': 'unchanged'}]
        baseline = {'knowledge_documents': {'rows': 15}, 'knowledge_chunks': {'rows': 1345}}
        final = {'knowledge_documents': {'rows': 17}, 'knowledge_chunks': {'rows': 1386}}
        receipt = {'rolled_back': True, 'operations': ops, 'before': before, 'artifact_hashes': hashes,
                   'national_before': datasets, 'baseline_corpus': baseline}
        source, receiver, monitor = MagicMock(), MagicMock(), MagicMock()
        source.commit.side_effect = psycopg2.OperationalError('simulated lost acknowledgement')
        def read(path, *args, **kwargs):
            if path.name == 'gland-historical-reference-operations.json': return json.dumps(ops)
            if path.name == 'gland-historical-reference-rollback.json': return json.dumps(receipt)
            return '{}'
        with patch('sys.argv', ['runner', '--mode', 'commit', '--evidence-dir', str(root)]), \
             patch.object(Path, 'exists', return_value=True), patch.object(Path, 'read_text', new=read), \
             patch.object(runner, 'load_bundles', return_value=self.bundles), \
             patch.object(runner, 'artifact_hashes', return_value=hashes), \
             patch.object(runner, 'connect', side_effect=[source, receiver, monitor]), \
             patch.object(runner, 'fingerprint', return_value=before), \
             patch.object(runner, 'corpus_parity', side_effect=[baseline, final]), \
             patch.object(runner, 'national', return_value=datasets), \
             patch.object(runner, 'apply', return_value=[]), \
             patch.object(h, 'validate_registered_mapping'), \
             patch.object(bridge, 'monitor') as log, patch.object(runner, 'save') as saved:
            with self.assertRaises(psycopg2.OperationalError): runner.main()
        self.assertEqual([call.args[2] for call in log.call_args_list], ['running'])
        self.assertEqual(saved.call_count, 1)
        path, result = saved.call_args.args
        self.assertEqual(path.name, 'gland-historical-reference-commit-outcome-unknown.json')
        self.assertEqual(result['commit_outcome'], 'unknown')
        self.assertFalse(result['rolled_back']); self.assertFalse(result['automatic_retry_permitted'])
        self.assertEqual(result['operations'], ops)


if __name__ == '__main__': unittest.main()
