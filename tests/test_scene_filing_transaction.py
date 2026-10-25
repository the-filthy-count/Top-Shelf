import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from test_duplicate_filing_safety import load
from download_safety import require_complete_queue_video, PartialDownloadPending

class SceneFilingSafetyTests(unittest.TestCase):
    def test_partial_and_nfo_failures_preserve_video(self):
        for case in ('partial','prepare','publish','rollback_failure','resumed_partial'):
            with self.subTest(case=case),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);inbox=root/'inbox';inbox.mkdir();release=inbox/'release';release.mkdir()
                src=release/'clip.mp4';src.write_bytes(b'video');dest=root/'library';dest.mkdir()
                nfo=dest/'Scene.nfo';nfo.write_text('old metadata')
                if case=='partial':(release/'unfinished.part').touch()
                plan={'status':'ok','dest_season':dest,'base_name':'Scene','nfo_title':'Scene','show_title':'Show','studio':'Studio','perf_names':[],'date':'2026-01-01','images':[],'title':'Scene'}
                gate=Mock(side_effect=lambda a,b,**kw:a.rename(b));updates=Mock()
                def build(*a,**kw):
                    if case=='resumed_partial':(release/'unfinished.part').touch()
                    return 'new metadata'
                def restore(a,b):
                    if case=='rollback_failure':raise OSError('restore denied')
                    a.rename(b)
                env={'Path':Path,'os':os,'require_complete_queue_video':require_complete_queue_video,
                    'db':SimpleNamespace(get_settings=lambda:{'source_dir':str(inbox)},queue_file_key=lambda *a:'release/clip.mp4',update_file=updates),
                    '_scene_filing_plan':lambda *a:plan,'_expand_group_performers_for_nfo':lambda p:p,'build_nfo':build,
                    '_quality_gate_move':gate,'safe_move':restore,'emit':Mock()}
                fn=load('main.py','_file_scene_from_match_impl',env)
                scene={'id':'scene','_extra_kind':'','_scope':''}
                if case in ('partial','resumed_partial'):
                    with self.assertRaises(PartialDownloadPending):fn(src,scene,'Manual')
                    gate.assert_not_called()
                    self.assertTrue((release/'unfinished.part').exists())
                elif case=='prepare':
                    with patch.object(Path,'write_text',side_effect=PermissionError('denied')),self.assertRaises(PermissionError):fn(src,scene,'Manual')
                    gate.assert_not_called()
                else:
                    with patch.object(os,'replace',side_effect=PermissionError('publish denied')),self.assertRaises((PermissionError,RuntimeError)):fn(src,scene,'Manual')
                self.assertEqual(nfo.read_text(),'old metadata')
                self.assertEqual(list(dest.glob('.scene-nfo-*')),[])
                if case=='rollback_failure':
                    self.assertEqual(updates.call_args.kwargs['destination'],str(dest/'Scene.mp4'))
                    self.assertTrue((dest/'Scene.mp4').exists())
                else:
                    self.assertEqual(src.read_bytes(),b'video');updates.assert_not_called()
