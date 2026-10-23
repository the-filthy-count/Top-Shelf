import ast
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from test_duplicate_filing_safety import load, ROOT

class SuggestionInboxTests(unittest.TestCase):
    def test_uses_configured_inbox_over_legacy_namesake(self):
        tree=ast.parse((ROOT/'main.py').read_text())
        endpoint=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='api_queue_suggestions_accept')
        accept=next(n for n in endpoint.body if isinstance(n,ast.FunctionDef) and n.name=='_accept')
        for scope,setting in [('Vices/Category','vice_download_watch_dir'),('JAV','jav_download_watch_dir')]:
            with self.subTest(scope=scope),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);scenes=root/'scenes';inbox=root/'inbox';scenes.mkdir();inbox.mkdir()
                relative='Category/clip.mp4' if scope.startswith('Vices') else 'clip.mp4'
                actual=inbox/relative;actual.parent.mkdir(exist_ok=True);actual.touch()
                filename=scope+'/clip.mp4';legacy=scenes/filename;legacy.parent.mkdir(parents=True);legacy.touch()
                filing=Mock(return_value={'status':'filed'});clear=Mock()
                settings={'source_dir':str(scenes),setting:str(inbox)}
                resolver=load('database.py','resolve_source_video_path',{'Path':Path,'_VIDEO_SUFFIXES_TRY':('.mp4',)})
                db=SimpleNamespace(get_settings=lambda:settings,resolve_source_video_path=resolver,
                    queue_suggestions_for_file=lambda f:[{'source_db':'StashDB','source_id':'scene','payload_json':'{"id":"scene"}'}],queue_suggestions_clear=clear)
                env={'Path':Path,'json':json,'db':db,'filename':filename,'source_db':'StashDB','source_id':'scene','scene_override':None,
                     '_extract_scene_number_from_filename':lambda f:None,'file_scene_from_match':filing}
                load('main.py','_resolve_queue_file_path',env)
                exec(compile(ast.Module(body=[accept],type_ignores=[]),'<test>','exec'),env)
                self.assertEqual(env['_accept'](),{'status':'filed'})
                self.assertEqual(filing.call_args.args[0],actual);clear.assert_called_once_with(filename)
                self.assertTrue(legacy.exists())
                filing.reset_mock();clear.reset_mock();actual.unlink();legacy.unlink()
                self.assertEqual(env['_accept']()['status'],'error')
                filing.assert_not_called();clear.assert_not_called()
