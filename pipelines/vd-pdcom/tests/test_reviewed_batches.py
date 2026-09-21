import copy
import json
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reviewed_batches import annotate, matching_batch, ROOT


class ReviewedBatchTests(unittest.TestCase):
    def test_replay_preserves_raw_text_other_metadata_and_is_idempotent(self):
        pages = [{'page_number': 1, 'text': 'original', 'other_review': {'keep': True}}, {'legacy_metadata': 'keep'}]
        batch = {'page_count': 1, 'page_annotations': [{'page_number': 1, 'fields': {'reviewed_map': {'geographic_validation': 'pending'}}}]}
        original = copy.deepcopy(pages)
        result = annotate(pages, 1, batch)
        self.assertEqual(pages, original)
        self.assertEqual(result[0]['text'], 'original')
        self.assertEqual(result[0]['other_review'], {'keep': True})
        self.assertEqual(result[1], pages[1])
        self.assertEqual(annotate(result, 1, batch), result)

    def test_changed_bytes_or_other_document_never_inherit_review(self):
        batch = json.loads(next(ROOT.glob('*.json')).read_text())
        self.assertIsNotNone(matching_batch(batch['document_id'], batch['sha256']))
        self.assertIsNone(matching_batch(batch['document_id'], '0'*64))
        self.assertIsNone(matching_batch('other-document', batch['sha256']))

    def test_missing_duplicate_and_out_of_range_pages_fail(self):
        batch = {'page_count': 2, 'page_annotations': []}
        for pages in [[{'page_number': 1}], [{'page_number': 1}, {'page_number': 1}], [{'page_number': 1}, {'page_number': 3}]]:
            with self.assertRaises(ValueError): annotate(pages, 2, batch)
        with self.assertRaises(ValueError): annotate([], 3, batch)

    def test_review_cannot_write_geometry_or_completion(self):
        for key in ['geometry', 'delivery_status', 'plan_status', 'text', 'page_number']:
            with self.assertRaises(ValueError):
                annotate([{'page_number': 1}], 1, {'page_count': 1, 'page_annotations': [{'page_number': 1, 'fields': {key: 'bad'}}]})

    def test_all_saved_batches_replay_and_retain_review_limits(self):
        batches = [json.loads(p.read_text()) for p in ROOT.glob('*.json')]
        self.assertGreaterEqual(len(batches), 4)  # Registry grows as reviewed batches are added.
        for batch in batches:
            pages = [{'page_number': n} for n in range(1, batch['page_count']+1)]
            result = annotate(pages, len(pages), batch)
            self.assertEqual(annotate(result, len(pages), batch), result)
        operational = next(x for x in batches if x['document_id']=='52cc08d1-5650-5f15-98da-71ec82ff2a32')
        fields = operational['page_annotations'][0]['fields']
        metrics = operational['document_annotations']['reviewed_operational_metrics']['metrics']
        self.assertEqual(len(metrics), 14)
        for item in metrics:
            self.assertIn('not', item['limits'])
            for sector in ['secondary','tertiary']:
                jobs = item['jobs_fte']
                self.assertEqual(jobs['source_current_'+sector]+jobs['additional_'+sector], jobs['future_'+sector])
        jouxtens = next(x for x in batches if x['document_id']=='68f53aea-2f82-5ab4-9f90-2c6ee01d6aff')
        self.assertEqual(sum('reviewed_map' in x['fields'] for x in jouxtens['page_annotations']),18)


if __name__ == '__main__': unittest.main()

class ExistingReviewCompatibilityTests(unittest.TestCase):
    def test_existing_review_keeps_document_level_metadata(self):
        from unittest.mock import MagicMock, patch
        from reviews import apply_review
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ([{'page_number': 1, 'text': 'raw'}, {'reviewed_operational_metrics': {'keep': True}}], 1)
        cursor.rowcount = 1
        review = {'approval_evidence_page': 1, 'map_pages': [], 'plan_status': 'unverified'}
        with patch('reviews.matching_review', return_value=review):
            self.assertTrue(apply_review(conn, 'doc', 'sha'))
        written = cursor.execute.call_args.args[1][1].adapted
        self.assertEqual(written[1], {'reviewed_operational_metrics': {'keep': True}})
