from datetime import datetime, timezone
import unittest
from unittest.mock import Mock, patch

from pipelines.sirene.incremental_sync import (
    ChunkProgress,
    _decode_progress,
    _encode_progress,
    _write_progress,
)


class ChunkProgressTest(unittest.TestCase):
    def test_round_trip_preserves_slice_resume_state(self):
        lo = datetime(2026, 6, 17, tzinfo=timezone.utc)
        hi = datetime(2026, 6, 17, 23, 59, 59, tzinfo=timezone.utc)
        middle = datetime(2026, 6, 17, 12, tzinfo=timezone.utc)
        progress = ChunkProgress(
            lo=lo,
            hi=hi,
            windows=[
                (lo, middle, None, 1144),
                (middle, hi, "68.31Z", 6769),
            ],
            next_index=1,
            companies=1068,
            establishments=1673,
            matched=1144,
        )

        restored = _decode_progress(_encode_progress(progress))

        self.assertEqual(restored, progress)
        self.assertEqual(restored.windows[restored.next_index][2], "68.31Z")

    def test_rejects_out_of_range_resume_index(self):
        lo = datetime(2026, 6, 17, tzinfo=timezone.utc)
        raw = _encode_progress(ChunkProgress(lo=lo, hi=lo, windows=[]))
        raw["next_index"] = 1

        with self.assertRaisesRegex(RuntimeError, "Invalid SIRENE progress index"):
            _decode_progress(raw)

    def test_rejects_unknown_progress_version(self):
        lo = datetime(2026, 6, 17, tzinfo=timezone.utc)
        raw = _encode_progress(ChunkProgress(lo=lo, hi=lo, windows=[]))
        raw["version"] = 999

        with self.assertRaisesRegex(RuntimeError, "Unsupported SIRENE progress version"):
            _decode_progress(raw)

    @patch("pipelines.sirene.incremental_sync.requests.patch")
    @patch("pipelines.sirene.incremental_sync._registry_api_params")
    def test_checkpoint_preserves_unrelated_api_params(self, read_params, patch_request):
        read_params.return_value = {"provider_option": "keep-me"}
        response = Mock()
        response.json.return_value = [{"dataset_code": "insee_sirene_companies"}]
        patch_request.return_value = response
        lo = datetime(2026, 6, 17, tzinfo=timezone.utc)
        progress = ChunkProgress(lo=lo, hi=lo, windows=[], next_index=0)

        _write_progress("https://example.supabase.co", "test-key", progress)

        response.raise_for_status.assert_called_once_with()
        payload = patch_request.call_args.kwargs["json"]["api_params"]
        self.assertEqual(payload["provider_option"], "keep-me")
        self.assertEqual(payload["sirene_incremental_progress"]["version"], 1)


if __name__ == "__main__":
    unittest.main()
