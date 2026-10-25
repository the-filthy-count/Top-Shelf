import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from test_duplicate_filing_safety import load
from download_safety import require_complete_queue_video

class MovieMetadataTransactionTests(unittest.TestCase):
    def test_metadata_failures_preserve_source_and_incumbent_nfo(self):
        for stage in ('prepare','publish','success','rollback_failure'):
            with self.subTest(stage=stage),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);library=root/'library';library.mkdir();src=root/'movie.mp4';src.write_bytes(b'video')
                folder=library/'Movie (2020)';folder.mkdir();nfo=folder/'Movie (2020).nfo';nfo.write_text('old metadata')
                updates=Mock();gate=Mock(side_effect=lambda a,b,**kw:a.rename(b))
                def restore(a,b):
                    if stage=='rollback_failure':raise OSError('restore denied')
                    a.rename(b)
                env={'Path':Path,'os':os,'require_complete_queue_video':require_complete_queue_video,'db':SimpleNamespace(get_settings=lambda:{'features_dir':str(library)},upsert_movie=Mock(),update_movie=updates,wanted_mark_acquired=Mock()),
                     'emit':Mock(),'_movie_filing_stem':lambda m:'Movie (2020)',
                     '_quality_gate_move':gate,'build_movie_nfo':lambda *a:'new metadata','safe_move':restore,
                     'compute_phash':lambda p:None,'_health_register_filed_movie':Mock(),
                     'trigger_media_scans':Mock(),'_remove_client_jobs_after_import':Mock()}
                fn=load('main.py','_file_movie_impl',env)
                if stage=='prepare':
                    with patch.object(Path,'write_text',side_effect=PermissionError('write denied')),self.assertRaises(PermissionError):
                        fn(src,{'id':'1','title':'Movie','year':'2020'})
                    gate.assert_not_called()
                elif stage in ('publish','rollback_failure'):
                    with patch.object(os,'replace',side_effect=PermissionError('publish denied')),self.assertRaises((PermissionError,RuntimeError)):
                        fn(src,{'id':'1','title':'Movie','year':'2020'})
                else:
                    self.assertEqual(fn(src,{'id':'1','title':'Movie','year':'2020'})['status'],'filed')
                if stage in ('prepare','publish'):
                    self.assertEqual(src.read_bytes(),b'video');self.assertEqual(nfo.read_text(),'old metadata')
                    updates.assert_not_called()
                elif stage=='rollback_failure':
                    self.assertEqual(updates.call_args.kwargs['destination'],str(folder/'Movie (2020).mp4'))
                    self.assertEqual(updates.call_args.kwargs['status'],'error')
                else:
                    self.assertFalse(src.exists());self.assertEqual(nfo.read_text(),'new metadata')
                self.assertEqual(list(folder.glob('.movie-nfo-*')),[])
