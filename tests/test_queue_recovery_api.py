"""Exercise the real queue recovery endpoints without starting main's workers."""
import ast
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from starlette.concurrency import run_in_threadpool
import database as db
from app.core.queue_recovery import archive


class RecoveryAPITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        names = {'queue_delete_file','queue_recovery_list','queue_recovery_restore','_queue_delete_filename_sync','file_history','queue_recovery_purge'}
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        cls.nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef,ast.AsyncFunctionDef)) and node.name in names]

    def test_stale_name_uses_id_removes_and_restores_exact_scene(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(db, '_settings_cache', None):
            root=Path(tmp);inbox=root/'inbox';inbox.mkdir()
            db.close_thread_connection()
            old = db.DB_PATH
            db.DB_PATH=root/'state.sqlite'
            try:
                db.init_db();db.save_settings({'source_dir':str(inbox)})
                original=inbox/'original.mp4';original.write_bytes(b'original')
                db.upsert_file(original.name);identity=db.file_identity(original.name)
                renamed=inbox/'renamed.mp4';original.rename(renamed);db.rename_file(original.name,renamed.name)
                original.write_bytes(b'unrelated replacement');db.upsert_file(original.name)
                app=FastAPI()
                env={'app':app,'Path':Path,'db':db,'emit':Mock(),'_archive_queue_file':archive,
                     'JSONResponse':JSONResponse,'run_in_threadpool':run_in_threadpool,
                     '_queue_warm':Mock(),'_movies_queue_warm':Mock()}
                exec(compile(ast.Module(body=self.nodes,type_ignores=[]),'recovery-routes','exec'),env)
                with TestClient(app) as client:
                    response=client.post('/api/queue/delete',json={'filename':'original.mp4','file_id':identity['id']})
                    self.assertEqual(response.status_code,200,response.text)
                    result=response.json();self.assertEqual(result['freed_bytes'],0)
                    self.assertEqual(original.read_bytes(),b'unrelated replacement');self.assertFalse(renamed.exists())
                    response=client.get('/api/queue/recovery');self.assertEqual(len(response.json()['files']),1)
                    response=client.post('/api/queue/recovery/'+result['recovery_id']+'/restore')
                    self.assertEqual(response.status_code,200,response.text)
                    self.assertEqual(renamed.read_bytes(),b'original')
                    self.assertEqual(db.file_identity(renamed.name)['id'],identity['id'])
                    response=client.get('/api/files/'+identity['id']+'/history')
                    self.assertEqual(response.status_code,200,response.text)
                    kinds=[event['kind'] for event in response.json()['events']]
                    self.assertIn('file.recovered_removal',kinds);self.assertIn('file.restored',kinds)
                    again=client.post('/api/queue/delete',json={'file_id':identity['id']})
                    self.assertEqual(again.status_code,200,again.text)
                    purged=client.post('/api/queue/recovery/'+again.json()['recovery_id']+'/purge')
                    self.assertEqual(purged.status_code,200,purged.text)
                    self.assertEqual(original.read_bytes(),b'unrelated replacement')
                    self.assertFalse(renamed.exists())
            finally:
                db.close_thread_connection();db.DB_PATH=old
