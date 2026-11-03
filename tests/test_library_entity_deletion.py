"""Library deletion guards tested without importing live application workers."""
import ast
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.core.library_deletion import delete_folder


class LibraryDeletionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        names = {'_library_delete_folder_on_disk', '_library_delete_favourite_from_disk',
                 '_library_delete_vice_from_disk'}
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        cls.code = compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)
                                          and n.name in names], type_ignores=[]), 'deletion', 'exec')

    def test_roots_and_parents_are_never_deletable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)/'library'; root.mkdir()
            for target in (root, root.parent):
                with self.assertRaises(ValueError):
                    delete_folder(target, [{'path': str(root)}])
                self.assertTrue(root.exists())

    def test_nested_root_is_protected_even_under_another_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); child = root/'nested'; child.mkdir()
            with self.assertRaises(ValueError):
                delete_folder(child, [{'path': str(root)}, {'path': str(child)}])
            self.assertTrue(child.exists())

    def test_part_and_symlink_preflight_leave_everything(self):
        for case in ('part', 'link', 'parent_link'):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); folder = root/'performer'; folder.mkdir()
                video = folder/'video.mp4'; video.write_bytes(b'keep')
                target = folder
                if case == 'part':
                    nested = folder/'nested'; nested.mkdir(); (nested/'video.PART').touch()
                elif case == 'link':
                    (folder/'link').symlink_to(video)
                else:
                    alias = root/'alias'; alias.symlink_to(folder, target_is_directory=True); target = alias
                with self.assertRaises(ValueError):
                    delete_folder(target, [{'path': str(root)}])
                self.assertEqual(video.read_bytes(), b'keep')

    def test_deletes_only_selected_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); selected = root/'one'; selected.mkdir()
            (selected/'video.mp4').write_bytes(b'one')
            sibling = root/'two'; sibling.mkdir(); (sibling/'video.part').write_bytes(b'two')
            delete_folder(selected, [{'path': str(root)}])
            self.assertFalse(selected.exists()); self.assertTrue(sibling.exists())

    def environment(self, root, folder):
        db = SimpleNamespace(favourite_get=Mock(return_value={'id': 1, 'path': str(folder), 'kind': 'performer'}),
                             favourite_delete=Mock())
        env = {'Path': Path, 'db': db, '_health_library_roots': lambda: [{'path': str(root)}],
               '_library_purge_folder_index': Mock(), '_library_purge_metadata_cache': Mock(),
               '_library_vice_by_id': lambda _: {'id': 'vice'}, '_library_vice_folder_path': lambda _: folder,
               '_vices_metadata_root': lambda: root/'metadata', '_library_remove_vice': Mock()}
        exec(self.code, env)
        return env

    def test_vice_dispatches_and_propagates_recovery_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)/'one'
            env = self.environment(Path(tmp), folder)
            with patch('app.core.vice_recovery.archive', side_effect=OSError('archive failed')) as archive:
                with self.assertRaises(OSError): env['_library_delete_vice_from_disk']('vice')
                archive.assert_called_once_with('vice', folder)
            env['_library_remove_vice'].assert_not_called()

    def test_favourite_dispatches_to_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = self.environment(Path(tmp), Path(tmp)/'one')
            with patch('app.core.library_recovery.archive') as archive:
                self.assertTrue(env['_library_delete_favourite_from_disk'](1))
                archive.assert_called_once_with(1)
            env['_library_purge_folder_index'].assert_not_called()
            env['db'].favourite_delete.assert_not_called()


class LibraryDeletionAPITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        cls.code = compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.AsyncFunctionDef)
                         and n.name == 'api_library_entity_delete_from_disk'], type_ignores=[]), 'delete-api', 'exec')

    def test_failed_removal_returns_conflict_without_success_notification(self):
        import asyncio
        import threading
        from fastapi import FastAPI, Body
        from fastapi.responses import JSONResponse
        from starlette.concurrency import run_in_threadpool
        for row_id in (1, -1):
            with self.subTest(row_id=row_id):
                request_thread = threading.get_ident()
                def failed(_):
                    self.assertNotEqual(threading.get_ident(), request_thread)
                    raise PermissionError('disk refused deletion')
                db = SimpleNamespace(favourite_get=lambda _: {'folder_name': 'Example'}, notification_add=Mock())
                env = {'app': FastAPI(), 'Body': Body, 'JSONResponse': JSONResponse, 'db': db,
                       'run_in_threadpool': run_in_threadpool, '_library_vice_by_id': lambda _: {'id': 'vice', 'name': 'Example'},
                       '_library_vice_by_negative_id': lambda _: {'id': 'vice', 'name': 'Example'},
                       '_library_delete_vice_from_disk': failed, '_library_delete_favourite_from_disk': failed}
                exec(self.code, env)
                response = asyncio.run(env['api_library_entity_delete_from_disk']({'id': row_id}))
                self.assertEqual(response.status_code, 409)
                self.assertIn(b'disk refused deletion', response.body)
                db.notification_add.assert_not_called()
