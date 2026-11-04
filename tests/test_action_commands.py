import json
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
import database as db
from app.core import action_commands as commands, queue_changes as changes
import test_suggestion_rule_versions as fixtures

class ActionCommandTests(unittest.TestCase):
    setUp=fixtures.SuggestionRuleVersionTests.setUp

    def test_separate_worker_reads_saved_arguments_and_records_result(self):
        caller=threading.get_ident()
        @commands.command('test.worker')
        def run(path):
            self.assertNotEqual(threading.get_ident(),caller)
            self.assertIsInstance(path,Path)
            with db.get_conn() as conn:
                row=conn.execute("SELECT state,desired_json FROM queue_changes WHERE action='test.worker'").fetchone()
                self.assertEqual(row['state'],'running')
                self.assertEqual(json.loads(row['desired_json'])['arguments']['path']['__command_path__'],'example')
            return {'ok':True}
        self.assertEqual(run(Path('example')),{'ok':True})
        row=changes.list_changes()[0]
        self.assertEqual(row['state'],'completed')
        self.assertEqual(row['queue_table'],'command')

    def test_settings_are_not_persisted_and_nested_steps_do_not_deadlock(self):
        @commands.command('test.child')
        def child(): return 'done'
        @commands.command('test.parent')
        def parent(settings):
            self.assertNotIn('secret',settings)
            return child()
        self.assertEqual(parent({'secret':'never store me'}),'done')
        with db.get_conn() as conn:
            rows=conn.execute('SELECT desired_json FROM queue_changes').fetchall()
        self.assertEqual(len(rows),1)
        self.assertNotIn('never store me',rows[0][0])

    def test_error_result_stays_failed(self):
        @commands.command('test.failure')
        def fail(): return {'error':'offline'}
        self.assertEqual(fail(),{'error':'offline'})
        self.assertEqual(changes.list_changes()[0]['state'],'failed')

    def test_pending_work_replays_without_original_caller(self):
        called=[]
        @commands.command('test.replay')
        def action(value): called.append(value)
        with db.get_conn() as conn:
            conn.execute("INSERT INTO command_subjects VALUES('action:test','Test')")
            changes.request(conn,'action:test','test.replay',{'arguments':{'value':123}})
        commands.drain()
        self.assertEqual(called,[123])
        commands.drain()
        self.assertEqual(called,[123])

    def test_payload_cannot_inject_codec_markers(self):
        @commands.command('test.markers')
        def action(payload): return payload
        value={'__current_settings__':True}
        self.assertEqual(action(value),value)

    def test_http_error_is_preserved_and_recorded_as_failure(self):
        from starlette.responses import JSONResponse
        @commands.command('test.http')
        def action(): return JSONResponse({'error':'conflict'},status_code=409)
        self.assertEqual(action().status_code,409)
        self.assertEqual(changes.list_changes()[0]['state'],'failed')
