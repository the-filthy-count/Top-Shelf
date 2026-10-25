import ast
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[1]
def load(file,name,env):
    fn=next(n for n in ast.parse((ROOT/file).read_text()).body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[fn],type_ignores=[]),file,'exec'),env)
    return env[name]
class DuplicateSafetyTests(unittest.TestCase):
    def test_deletion_requires_successful_replacement(self):
        with tempfile.TemporaryDirectory() as tmp:
            old=Path(tmp)/'old.mp4';new=Path(tmp)/'new.mp4';old.touch();new.touch()
            delete=Mock();env={'Path':Path,'emit':Mock(),'db':SimpleNamespace(media_file_signature=lambda p:'stable',library_files_active_matching_phash=lambda ph:[{'destination':str(old)}]),
                '_phash_for_library_destination':lambda p:'hash','_queue_source_video_dup_compare_payload':lambda p:{'destination':str(p)},'_health_dup_file_payload':lambda row:row,
                '_duplicate_recommend':lambda rows:{'confidence':'high','keeper':rows[-1]},'_duplicate_delete_one_library_video':delete}
            early=load('main.py','_queue_resolve_phash_vs_indexed_library',env)
            self.assertIsNone(early(new,'new.mp4','hash'));delete.assert_not_called()
            cleanup=load('main.py','_cleanup_superseded_library_copies',env)
            for result in ({'status':'no_dir'},{'status':'error'},{'status':'filed','destination':str(new),'deduped':True},{'status':'filed','destination':str(new)+'missing'}):cleanup(result,'hash')
            delete.assert_not_called()
            cleanup({'status':'filed','destination':str(new)},'hash');delete.assert_called_once_with(str(old))
            self.assertTrue(old.exists()) # all deletions mocked

    def test_rename_clears_derived_metadata(self):
        conn=sqlite3.connect(':memory:');conn.row_factory=sqlite3.Row;self.addCleanup(conn.close)
        conn.execute('CREATE TABLE processed_files (id INTEGER, filename TEXT, filename_stem TEXT, filename_ext TEXT, parsed_meta_json TEXT, guess_studio TEXT, guess_performers TEXT, guess_lib_gen TEXT, perf_resolver_json TEXT)')
        conn.execute("INSERT INTO processed_files VALUES(1,'old.mp4','old','.mp4','stale','stale','stale','stale','stale')")
        env={'sqlite3':sqlite3,'get_conn':lambda:conn,'_filename_stem_ext':lambda f:(str(Path(f).with_suffix('')),Path(f).suffix)}
        rename=load('database.py','rename_file',env)
        self.assertTrue(rename('old.mp4','Person/new.mp4'))
        row=conn.execute('SELECT * FROM processed_files').fetchone()
        self.assertEqual(row['filename'],'Person/new.mp4')
        for key in ('parsed_meta_json','guess_studio','guess_performers','guess_lib_gen','perf_resolver_json'):self.assertIsNone(row[key])

    def test_nested_duplicate_keys_remain_distinct(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for folder in ('One','Two'):
                (root/folder).mkdir();(root/folder/'same.mp4').touch()
            env={'Path':Path,'VIDEO_EXTENSIONS':{'.mp4'},'PartialDownloadPending':type('PartialDownloadPending',(Exception,),{}),
                 'require_complete_queue_video':lambda *a:None,'_queue_duplicate_ignored_get':lambda:set(),
                 'db':SimpleNamespace(get_settings=lambda:{'source_dir':tmp},get_history=lambda **k:[],upsert_file=Mock(),validate_media_cache=lambda *a:'sig',get_phash=lambda f:'hash'),
                 '_queue_dup_file_payload':lambda fn,p,**kw:{'current_filename':fn,'destination':str(p)},
                 '_duplicate_recommend':lambda rows:{'keeper':rows[0],'confidence':'high'}}
            scan=load('main.py','_queue_duplicate_groups_payload',env)
            result=scan()
            self.assertEqual(result['file_count'],2)
            self.assertEqual([f['current_filename'] for f in result['groups'][0]['files']],['One/same.mp4','Two/same.mp4'])
