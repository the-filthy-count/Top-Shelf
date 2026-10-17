import ast
import asyncio
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from security_runtime import LoginLimiter, same_origin_write

ROOT = Path(__file__).resolve().parents[1]

class SecurityTests(unittest.TestCase):
    def request(self, **headers):
        return SimpleNamespace(method='POST', url='https://shelf.example/api/settings', headers=headers)

    def test_origins(self):
        self.assertTrue(same_origin_write(self.request(origin='https://shelf.example')))
        self.assertTrue(same_origin_write(self.request(origin='https://shelf.example:443')))
        for origin in ['null', 'https://shelf.example.evil.test', 'http://shelf.example', 'https://shelf.example:8443']:
            self.assertFalse(same_origin_write(self.request(origin=origin)))
        self.assertFalse(same_origin_write(self.request(**{'sec-fetch-site':'cross-site'})))
        self.assertFalse(same_origin_write(self.request(referer='https://evil.test/form')))
        self.assertTrue(same_origin_write(self.request())) # CLI clients

    def test_throttle_expires_and_is_bounded(self):
        limiter = LoginLimiter()
        with patch('security_runtime.monotonic', return_value=1):
            self.assertTrue(all(limiter.allow('peer') for _ in range(10)))
            self.assertFalse(limiter.allow('peer'))
        with patch('security_runtime.monotonic', return_value=302):
            self.assertTrue(limiter.allow('peer'))
        for n in range(5000): limiter.allow(str(n))
        self.assertEqual(len(limiter.entries),4096)

    def database_functions(self):
        conn=sqlite3.connect(':memory:');conn.row_factory=sqlite3.Row
        conn.executescript('CREATE TABLE auth(id INTEGER, password_hash TEXT, session_minutes INTEGER, session_hours INTEGER); INSERT INTO auth VALUES(1,"old",10,1); CREATE TABLE sessions(token TEXT, expires_at TEXT);')
        @contextmanager
        def get_conn(): yield conn
        tree=ast.parse((ROOT/'database.py').read_text())
        nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'set_password','touch_session','set_session_minutes'}]
        namespace={'get_conn':get_conn,'datetime':datetime}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'database.py','exec'),namespace)
        self.addCleanup(conn.close)
        return conn,namespace

    def test_password_change_revokes_all_sessions(self):
        conn,ns=self.database_functions()
        conn.execute('INSERT INTO sessions VALUES (?,?)',('old', (datetime.now()+timedelta(hours=1)).isoformat()))
        ns['set_password']('new')
        self.assertEqual(conn.execute('SELECT count(*) FROM sessions').fetchone()[0],0)
        self.assertEqual(conn.execute('SELECT password_hash FROM auth').fetchone()[0],'new')

    def test_shorter_timeout_caps_existing_sessions_without_reviving_expired(self):
        conn,ns=self.database_functions()
        expired=(datetime.now()-timedelta(minutes=1)).isoformat()
        conn.execute('INSERT INTO sessions VALUES (?,?)',('expired',expired))
        conn.execute('INSERT INTO sessions VALUES (?,?)',('active',(datetime.now()+timedelta(hours=24)).isoformat()))
        ns['set_session_minutes'](2)
        rows=dict(conn.execute('SELECT token,expires_at FROM sessions'))
        self.assertEqual(rows['expired'],expired)
        self.assertLessEqual(datetime.fromisoformat(rows['active']),datetime.now()+timedelta(minutes=2))

    def test_heartbeat_cannot_resurrect_expired_session(self):
        conn,ns=self.database_functions()
        for token,delta in [('expired',-1),('active',1)]:
            conn.execute('INSERT INTO sessions VALUES (?,?)',(token,(datetime.now()+timedelta(minutes=delta)).isoformat()))
        self.assertEqual(ns['touch_session']('expired'),0)
        self.assertEqual(ns['touch_session']('missing'),0)
        self.assertEqual(ns['touch_session']('active'),600)

    def test_password_changes_require_current_password_but_initial_setup_does_not(self):
        tree=ast.parse((ROOT/'main.py').read_text())
        node=next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == '_confirm_current_password')
        async def run(fn,*args): return fn(*args)
        ns={'db':SimpleNamespace(get_password_hash=lambda:'stored'),
            'login_limiter':LoginLimiter(), 'run_in_threadpool':run,
            'bcrypt':SimpleNamespace(checkpw=lambda supplied,stored: supplied == b'correct')}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
        request=SimpleNamespace(client=SimpleNamespace(host='test'))
        self.assertFalse(asyncio.run(ns['_confirm_current_password'](request,{})))
        self.assertFalse(asyncio.run(ns['_confirm_current_password'](request,{'current_password':'wrong'})))
        self.assertTrue(asyncio.run(ns['_confirm_current_password'](request,{'current_password':'correct'})))
        ns['db'].get_password_hash=lambda:None
        self.assertTrue(asyncio.run(ns['_confirm_current_password'](request,{})))

if __name__ == '__main__': unittest.main()
