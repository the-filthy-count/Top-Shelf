"""Download recovery contracts with no live download-client mutations."""
import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
def functions(file, names, namespace):
    tree = ast.parse((ROOT/file).read_text())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), file, 'exec'), namespace)
    return namespace

class RecoveryTests(unittest.TestCase):
    def test_alert_dedup_survives_connection_restart_and_dismissal(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory)/'test.db')
            conn = sqlite3.connect(path)
            conn.execute('CREATE TABLE app_notifications(id INTEGER PRIMARY KEY, created_at TEXT,kind TEXT,message TEXT,expires_at TEXT)')
            ns = functions('database.py', {'download_failure_observe'}, dict(get_conn=lambda:conn, datetime=datetime, timedelta=timedelta, timezone=timezone))
            observe = ns['download_failure_observe']
            self.assertTrue(observe('job','FAILURE','reason'))
            conn.execute('DELETE FROM app_notifications');conn.commit();conn.close()
            conn = sqlite3.connect(path)
            self.assertFalse(observe('job','FAILURE','reason'))
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM app_notifications').fetchone()[0],0)
            self.assertFalse(observe('job','',''))
            self.assertTrue(observe('job','FAILURE','reason'))
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM app_notifications').fetchone()[0],1)
            observe('job','','')
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM app_notifications').fetchone()[0],0)
            self.assertTrue(observe('job','FAILURE','reason'))
            conn.close()

    def test_retry_checks_client_response_and_uses_single_job(self):
        import re
        rpc = Mock(return_value=False)
        config = {'client':'nzbget','host':'localhost','port':'6789','user':'','pass':''}
        ns = functions('main.py', {'_download_retry_by_id'}, dict(db=SimpleNamespace(get_settings=lambda:{}), _dl_resolve_nzb_settings=lambda _:config, _nzbget_rpc=rpc, re=re))
        retry = ns['_download_retry_by_id']
        self.assertIn('error',retry('nzbget-h-42'))
        rpc.return_value = True
        self.assertTrue(retry('nzbget-h-42')['ok'])
        self.assertEqual(rpc.call_args.args[3], ['HistoryRedownload','',[42]])
        self.assertIn('error',retry('qbit-other'))
        config.update(client='sabnzbd',apikey='test')
        requests = SimpleNamespace(get=Mock())
        requests.get.return_value.json.return_value={'status':False,'error':'refused'}
        ns['requests']=requests
        self.assertIn('error',retry('sab-h-SABnzbd_nzo_42'))
        requests.get.return_value.json.return_value={'status':True}
        self.assertTrue(retry('sab-h-SABnzbd_nzo_42')['ok'])
        self.assertEqual(requests.get.call_args.kwargs['params']['value'],'SABnzbd_nzo_42')
        self.assertIn('error',retry('sab-h-all,other'))

    def test_metrics_are_bounded_and_count_errors(self):
        import performance_metrics as metrics
        with metrics._lock: metrics._metrics.clear()
        for _ in range(300): metrics.record('test', .01)
        with self.assertRaises(ValueError):
            with metrics.measure('test'): raise ValueError('test')
        result=metrics.snapshot()['test']
        self.assertEqual(result['count'],301)
        self.assertEqual(result['errors'],1)
        self.assertEqual(len(metrics._metrics['test']['samples']),256)

class RemovalTests(unittest.TestCase):
    def test_torrent_removal_preserves_files_unless_explicit(self):
        config = {'client':'qbittorrent'}
        session = Mock()
        session.post.return_value.status_code = 200
        ns = functions('main.py', {'_download_remove_by_id'}, dict(db=SimpleNamespace(get_settings=lambda:{}), _dl_resolve_torrent_settings=lambda _:config, _qbittorrent_session=lambda _:(session,'http://client')))
        remove = ns['_download_remove_by_id']
        self.assertTrue(remove('qbit-abc', delete_files=False)['ok'])
        self.assertEqual(session.post.call_args.kwargs['data']['deleteFiles'], 'false')
        self.assertTrue(remove('qbit-abc', delete_files=True)['ok'])
        self.assertEqual(session.post.call_args.kwargs['data']['deleteFiles'], 'true')
        self.assertIn('error', remove('nzbget-1', delete_files=False))

    def test_successful_retry_resets_alert_but_refused_retry_does_not(self):
        import threading
        observed = Mock()
        retry = Mock(return_value={'error':'refused'})
        warm = SimpleNamespace(_build_lock=threading.Lock(), invalidate=Mock())
        tree = ast.parse((ROOT/'main.py').read_text())
        node = next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='api_downloads_retry')
        node.decorator_list=[]
        ns=dict(Body=lambda _:None, _downloads_warm=warm, _download_retry_by_id=retry, db=SimpleNamespace(download_failure_observe=observed))
        exec(compile(ast.Module(body=[node],type_ignores=[]),'retry','exec'),ns)
        ns['api_downloads_retry']({'id':'nzbget-h-42'})
        observed.assert_not_called()
        retry.return_value={'ok':True}
        ns['api_downloads_retry']({'id':'nzbget-h-42'})
        observed.assert_called_once_with('nzbget:nzbget-h-42','','')
        warm.invalidate.assert_called_once()
