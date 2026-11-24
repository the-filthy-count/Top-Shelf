import ast
from pathlib import Path
import sqlite3
import unittest

class RetryCandidateTests(unittest.TestCase):
    def test_stale_processing_is_retried_without_active_or_manual_jobs(self):
        c=sqlite3.connect(':memory:');self.addCleanup(c.close);c.row_factory=sqlite3.Row
        c.executescript('''CREATE TABLE processed_files(id INTEGER,filename TEXT,status TEXT,processed_at TEXT);
        CREATE TABLE file_assets(id TEXT,queue_table TEXT,queue_id INTEGER);
        CREATE TABLE queue_changes(file_id TEXT,running_revision INTEGER,state TEXT,action TEXT);''')
        for i,status in enumerate(['unmatched','processing','processing','error','error','filed','removed','error','error']):
            c.execute('INSERT INTO processed_files VALUES(?,?,?,?)',(i,str(i),status,'2026-01-01'))
            c.execute('INSERT INTO file_assets VALUES(?,?,?)',(str(i),'processed_files',i))
        c.executemany('INSERT INTO queue_changes VALUES(?,?,?,?)', [('2',1,'running','process'),('3',None,'pending','manual_scene'),('4',None,'cancelled','process'),('7',None,'blocked','manual_scene'),('8',None,'failed','manual_scene')])
        tree=ast.parse((Path(__file__).resolve().parents[1]/'database.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='get_retry_files')
        ns={'get_conn':lambda:c};exec(compile(ast.Module(body=[node],type_ignores=[]),'database.py','exec'),ns)
        self.assertEqual(ns['get_retry_files'](),['0','1'])

    def test_processing_guard_preserves_interrupted_manual_actions(self):
        from unittest.mock import Mock
        tree=ast.parse((Path(__file__).resolve().parents[1]/'app/core/queue_changes.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='schedule_processing')
        request=Mock();ns={'request':request}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'queue_changes.py','exec'),ns)
        c=sqlite3.connect(':memory:');self.addCleanup(c.close)
        c.execute('CREATE TABLE queue_changes(file_id,action,state)')
        for state in ('blocked','failed'):
            c.execute('DELETE FROM queue_changes')
            c.execute('INSERT INTO queue_changes VALUES(?,?,?)',('id','manual_scene',state))
            self.assertFalse(ns['schedule_processing'](c,'id'))
        request.assert_not_called()
