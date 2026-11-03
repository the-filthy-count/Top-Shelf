import ast
import asyncio
import json
from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from app.core.scene_acceptance import hydrate_tpdb_cast


class SceneAcceptanceTests(unittest.TestCase):
    def routing(self, lookup):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        names = {'_performer_objects_in_match_order', '_performer_crosswalk_ids', '_find_performer_dir_via_library_index'}
        env = {'Path': Path, 'db': SimpleNamespace(favourite_find_performer_folders_by_crosswalk=lookup), 'emit': Mock()}
        exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[]), 'routing', 'exec'), env)
        return env

    def test_picker_cast_is_rehydrated_and_canonical_id_resolves_folder(self):
        with tempfile.TemporaryDirectory() as folder:
            lookup = Mock(side_effect=lambda tid, *args: [{'path': folder, 'folder_name': 'Pocket Pleasure'}] if tid == 'canonical' else [])
            fetch = Mock(return_value={'id': 'selected-scene', 'performers': [{'performer': {'id': 'site-credit', 'name': 'PocketPleasure', 'parent': {'id': 'canonical'}}}]})
            scene = hydrate_tpdb_cast({'id': 'selected-scene', 'title': 'Selected title', 'performers': [{'performer': {'id': '', 'name': 'PocketPleasure'}}]}, fetch)
            result = self.routing(lookup)['_find_performer_dir_via_library_index'](scene['performers'], 'TPDB')
            self.assertEqual(result[0][0], Path(folder))
            lookup.assert_called_once_with('canonical', '', '', '')
            fetch.assert_called_once_with({'source_db': 'TPDB', 'source_id': 'selected-scene'})
            self.assertEqual(scene['title'], 'Selected title')

    def test_same_name_does_not_authorize_different_id(self):
        lookup = Mock(return_value=[])
        result = self.routing(lookup)['_find_performer_dir_via_library_index']([{'performer': {'id': 'other', 'name': 'Pocket Pleasure'}}], 'TPDB')
        self.assertEqual(result, [])
        lookup.assert_called_once_with('other', '', '', '')

    def test_accept_endpoint_files_authoritative_selected_cast(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        endpoint = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'api_queue_suggestions_accept')
        async def inline(fn):
            return fn()
        with tempfile.TemporaryDirectory() as folder:
            video = Path(folder) / 'video.mp4'; video.touch()
            filing = Mock(return_value={'status': 'filed'})
            clear = Mock()
            fetch = Mock(return_value={'id': 'child', 'performers': [{'performer': {'id': 'verified'}}]})
            env = {'app': FastAPI(), 'Body': Body, 'JSONResponse': JSONResponse, 'json': json,
                   'run_in_threadpool': inline, '_fetch_grab_scene_for_filing': fetch,
                   '_resolve_queue_file_path': lambda *args: video, 'file_scene_from_match': filing,
                   'db': SimpleNamespace(get_settings=lambda: {}, queue_suggestions_clear=clear,
                       queue_suggestions_for_file=lambda _: [{'source_db': 'TPDB', 'source_id': 'parent',
                           'payload_json': json.dumps({'id': 'parent', 'performers': []})}])}
            exec(compile(ast.Module(body=[endpoint], type_ignores=[]), 'accept-api', 'exec'), env)
            payload = {'filename': 'video.mp4', 'source_db': 'TPDB', 'source_id': 'parent',
                       'scene_override': {'id': 'child', 'performers': [{'performer': {'id': '', 'name': 'Credit'}}]}}
            result = asyncio.run(env['api_queue_suggestions_accept'](payload))
            self.assertTrue(result['ok'])
            self.assertEqual(filing.call_args.args[1]['performers'][0]['performer']['id'], 'verified')
            fetch.assert_called_once_with({'source_db': 'TPDB', 'source_id': 'child'})
            clear.assert_called_once()
            fetch.return_value = None; filing.reset_mock(); clear.reset_mock()
            result = asyncio.run(env['api_queue_suggestions_accept'](payload))
            self.assertFalse(result['ok']); filing.assert_not_called(); clear.assert_not_called()

    def test_parent_is_not_used_for_other_sources(self):
        ids = self.routing(Mock())['_performer_crosswalk_ids']({'id': 'original', 'parent': {'id': 'other'}}, 'StashDB')
        self.assertEqual(ids, ('', 'original', '', ''))

    def test_unavailable_or_wrong_scene_cannot_file_cached_cast(self):
        for fetch in (Mock(return_value=None), Mock(return_value={'id': 'wrong'}), Mock(side_effect=OSError('offline'))):
            with self.subTest(fetch=fetch), self.assertRaises(ValueError):
                hydrate_tpdb_cast({'id': 'selected', 'performers': []}, fetch)

    def test_selected_scene_replaces_movie_cast(self):
        scene = hydrate_tpdb_cast({'id': 'child', 'performers': [{'performer': {'id': 'movie-cast'}}]}, lambda _: {'id': 'child', 'performers': [{'performer': {'id': 'child-cast'}}]})
        self.assertEqual(scene['performers'][0]['performer']['id'], 'child-cast')


if __name__ == '__main__':
    unittest.main()
