"""Smoke tests for the StashDB → TPDB → FansDB GraphQL fallback chain.

Targets `main.query_with_fallback`, which is the single function every
phash identification path in the app runs through. These tests verify
the chain's contract:

  • A successful StashDB hit short-circuits — TPDB and FansDB are not
    called.
  • A StashDB exception triggers a TPDB attempt; a TPDB exception
    triggers a FansDB attempt; a FansDB exception is fatal.
  • Empty (non-exception) results from one source still fall through
    to the next, and a fully-empty chain returns `([], "none")`.

The chain itself is small but it's wired into every file the app
processes, so a silent regression here would mean no scenes get
matched at all. Hence "smoke" rather than "exhaustive."
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


# Project root needs to be on sys.path so `import main` works when this
# test file is run via `python -m unittest discover tests`.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Point the SQLite DB at a throwaway tempfile *before* importing main —
# `database.py` opens the configured path at import time, and we don't
# want module load to touch the real production DB.
_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMP_DB.close()
os.environ["DB_PATH"] = _TMP_DB.name

import main  # noqa: E402  (after sys.path / env setup)


_FAKE_KEYS = {
    "stashdb": "k-stash",
    "tpdb": "k-tpdb",
    "fansdb": "k-fansdb",
    "javstash": "k-javstash",
}


def _scene(sid: str = "abc-123") -> dict:
    """A minimal scene-shaped dict — only id is read by the chain."""
    return {"id": sid, "title": "Smoke Test Scene"}


class QueryWithFallbackTests(unittest.TestCase):
    """Each test patches `query_stashbox` to script the chain's branches."""

    def setUp(self) -> None:
        # Always pretend api keys are configured so the chain doesn't
        # short-circuit on missing credentials.
        self._keys_patch = patch.object(main, "get_api_keys", return_value=_FAKE_KEYS)
        self._keys_patch.start()
        # Suppress the chain's `emit(...)` warning lines so the test
        # output stays clean.
        self._emit_patch = patch.object(main, "emit", lambda *a, **kw: None)
        self._emit_patch.start()

    def tearDown(self) -> None:
        self._keys_patch.stop()
        self._emit_patch.stop()

    # ── happy paths ────────────────────────────────────────────────

    def test_stashdb_hit_short_circuits(self) -> None:
        """StashDB returns a scene → TPDB and FansDB are never called."""
        scenes = [_scene()]
        with patch.object(main, "query_stashbox", side_effect=[scenes]) as qs:
            result, source = main.query_with_fallback("d34db33f")
        self.assertEqual(source, "StashDB")
        self.assertEqual(result, scenes)
        self.assertEqual(qs.call_count, 1)
        # First positional after phash is the endpoint URL — confirm it
        # was StashDB so we know the right source was actually hit.
        self.assertIn("stashdb.org", qs.call_args_list[0].args[1])

    def test_stashdb_exception_falls_through_to_tpdb(self) -> None:
        scenes = [_scene("tpdb-1")]
        with patch.object(
            main,
            "query_stashbox",
            side_effect=[RuntimeError("stashdb down"), scenes],
        ) as qs:
            result, source = main.query_with_fallback("d34db33f")
        self.assertEqual(source, "TPDB")
        self.assertEqual(result, scenes)
        self.assertEqual(qs.call_count, 2)
        self.assertIn("theporndb.net", qs.call_args_list[1].args[1])

    def test_stashdb_and_tpdb_fail_then_fansdb_succeeds(self) -> None:
        scenes = [_scene("fansdb-1")]
        with patch.object(
            main,
            "query_stashbox",
            side_effect=[
                RuntimeError("stashdb down"),
                RuntimeError("tpdb down"),
                scenes,
            ],
        ) as qs:
            result, source = main.query_with_fallback("d34db33f")
        self.assertEqual(source, "FansDB")
        self.assertEqual(result, scenes)
        self.assertEqual(qs.call_count, 3)
        self.assertIn("fansdb.cc", qs.call_args_list[2].args[1])

    # ── empty-result fall-through (a real-world quirk) ─────────────

    def test_empty_stashdb_falls_through_to_tpdb(self) -> None:
        """An empty list (not an exception) still continues the chain."""
        scenes = [_scene("tpdb-after-empty")]
        with patch.object(main, "query_stashbox", side_effect=[[], scenes]) as qs:
            result, source = main.query_with_fallback("d34db33f")
        self.assertEqual(source, "TPDB")
        self.assertEqual(result, scenes)
        self.assertEqual(qs.call_count, 2)

    def test_all_empty_returns_none_source(self) -> None:
        """Every source returns []; the chain reports 'none' not error."""
        with patch.object(main, "query_stashbox", side_effect=[[], [], [], []]) as qs:
            result, source = main.query_with_fallback("d34db33f")
        self.assertEqual(source, "none")
        self.assertEqual(result, [])
        self.assertEqual(qs.call_count, 4)

    # ── failure paths ──────────────────────────────────────────────

    def test_all_four_exceptions_raises_runtime_error(self) -> None:
        """If even JAVStash blows up the function should raise — silent
        empty returns would mask a total outage."""
        with patch.object(
            main,
            "query_stashbox",
            side_effect=[
                RuntimeError("stashdb down"),
                RuntimeError("tpdb down"),
                RuntimeError("fansdb down"),
                RuntimeError("javstash down"),
            ],
        ):
            with self.assertRaises(RuntimeError) as ctx:
                main.query_with_fallback("d34db33f")
        self.assertIn("JAVStash", str(ctx.exception))


class QueryStashboxParsingTests(unittest.TestCase):
    """Light coverage of `query_stashbox` response parsing — verifies
    that both the `findScenesByFullFingerprints` (StashDB / FansDB) and
    `findScenesBySceneFingerprints` (TPDB) shapes are unpacked correctly.
    """

    def _make_response(self, payload: dict, status: int = 200):
        class _Resp:
            status_code = status
            def raise_for_status(self):
                if status != 200:
                    raise RuntimeError(f"HTTP {status}")
            def json(self):
                return payload
        return _Resp()

    def test_full_fingerprint_shape_unpacks(self) -> None:
        """`findScenesByFullFingerprints` returns a flat list."""
        payload = {"data": {"findScenesByFullFingerprints": [{"id": "A"}, {"id": "B"}]}}
        with patch.object(main.requests, "post", return_value=self._make_response(payload)):
            scenes = main.query_stashbox("d34db33f", "https://stashdb.org/graphql", "k", "Q", "full")
        self.assertEqual([s["id"] for s in scenes], ["A", "B"])

    def test_scene_fingerprint_shape_flattens_groups(self) -> None:
        """`findScenesBySceneFingerprints` returns a list-of-lists; the
        helper flattens groups so callers see one list of scenes."""
        payload = {
            "data": {
                "findScenesBySceneFingerprints": [
                    [{"id": "A"}],
                    [{"id": "B"}, {"id": "C"}],
                ]
            }
        }
        with patch.object(main.requests, "post", return_value=self._make_response(payload)):
            scenes = main.query_stashbox(
                "d34db33f", "https://theporndb.net/graphql", "k", "Q", "scene",
            )
        self.assertEqual([s["id"] for s in scenes], ["A", "B", "C"])

    def test_graphql_errors_become_runtime_error(self) -> None:
        """Stash-box GraphQL errors must surface — the fallback chain
        relies on this to skip a broken source instead of treating an
        error response as a confident empty match."""
        payload = {"errors": [{"message": "bad query"}]}
        with patch.object(main.requests, "post", return_value=self._make_response(payload)):
            with self.assertRaises(RuntimeError):
                main.query_stashbox("d34db33f", "https://stashdb.org/graphql", "k", "Q", "full")


if __name__ == "__main__":
    unittest.main()
