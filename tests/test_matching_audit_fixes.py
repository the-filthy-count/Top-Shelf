import ast
import asyncio
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from app.integrations import fingerprint_cooldown as cooldown

ROOT=Path(__file__).resolve().parents[1]
def load(file,name,ns):
    node=next(n for n in ast.parse((ROOT/file).read_text()).body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),file,'exec'),ns)
    return ns[name]

class MatchingAuditFixes(unittest.TestCase):
    def setUp(self):
        cooldown._until.clear()
    def tearDown(self):
        cooldown._until.clear()
    def test_cooldown_expiry_and_service_isolation(self):
        with patch.object(cooldown.time,'monotonic',return_value=100):
            with self.assertRaisesRegex(RuntimeError,'HTTP 429'):
                cooldown.rate_limited('https://one.test/graphql','60')
            with self.assertRaisesRegex(RuntimeError,'60 seconds'):
                cooldown.check('https://one.test/graphql')
            cooldown.check('https://two.test/graphql')
        with patch.object(cooldown.time,'monotonic',return_value=161):
            cooldown.check('https://one.test/graphql')
    def test_http_date_and_default(self):
        with patch.object(cooldown.time,'time',return_value=1000),patch.object(cooldown.time,'monotonic',return_value=10):
            header=format_datetime(datetime.fromtimestamp(1120,timezone.utc),usegmt=True)
            with self.assertRaisesRegex(RuntimeError,'120 seconds'):cooldown.rate_limited('https://one.test',header)
            with self.assertRaisesRegex(RuntimeError,'300 seconds'):cooldown.rate_limited('https://two.test','invalid')
    def test_sync_lookup_stops_network_during_cooldown(self):
        response=SimpleNamespace(status_code=429,headers={'Retry-After':'60'})
        post=Mock(return_value=response)
        fn=load('main.py','query_stashbox',{'requests':SimpleNamespace(post=post)})
        for _ in range(2):
            with self.assertRaises(RuntimeError):fn('hash','https://one.test/graphql','key','query','scene')
        self.assertEqual(post.call_count,1)
    def test_async_lookup_shares_sync_cooldown(self):
        tree=ast.parse((ROOT/'app/integrations/stashbox.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='query_stashbox')
        ns={'httpx':SimpleNamespace(AsyncClient=object)}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'stashbox.py','exec'),ns)
        with self.assertRaises(RuntimeError):cooldown.rate_limited('https://one.test/graphql','60')
        client=SimpleNamespace(post=Mock())
        with self.assertRaisesRegex(RuntimeError,'rate-limited'):
            asyncio.run(ns['query_stashbox'](client,'hash','https://one.test/graphql','key','query','scene'))
        client.post.assert_not_called()

    def test_retry_includes_separate_vice_inbox(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);vice=root/'vice';(vice/'Category').mkdir(parents=True);(vice/'Category'/'video.mp4').touch()
            resolve=load('database.py','resolve_source_video_path',{'Path':Path,'_VIDEO_SUFFIXES_TRY':['.mp4']})
            run=Mock();settings={'source_dir':str(root/'scenes'),'vice_download_watch_dir':str(vice)}
            db=SimpleNamespace(get_settings=lambda:settings,get_retry_files=lambda:['Vices/Category/video.mp4','missing.mp4'],resolve_source_video_path=resolve)
            load('main.py','run_retry_pipeline',{'Path':Path,'db':db,'processing_state':{'running':False},'run_pipeline':run})()
            run.assert_called_once_with(['Vices/Category/video.mp4'])
    def test_new_result_clears_old_error(self):
        c=sqlite3.connect(':memory:');self.addCleanup(c.close);c.row_factory=sqlite3.Row
        c.execute('CREATE TABLE processed_files(id INTEGER,filename TEXT,status TEXT,error TEXT,processed_at TEXT)')
        c.execute("INSERT INTO processed_files VALUES(1,'video.mp4','error','old error',NULL)")
        fn=load('database.py','update_file',{'get_conn':lambda:c,'datetime':datetime,'_filename_stem_ext':lambda f:('video','.mp4')})
        for status in ('unmatched','no_dir','filed'):
            fn('video.mp4',status)
            self.assertIsNone(c.execute('SELECT error FROM processed_files').fetchone()[0])
        fn('video.mp4','error',error='current error')
        self.assertEqual(c.execute('SELECT error FROM processed_files').fetchone()[0],'current error')
