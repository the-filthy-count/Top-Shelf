import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from download_safety import require_complete_queue_video, PartialDownloadPending
from test_duplicate_filing_safety import load

class ManualMoviePartialTests(unittest.TestCase):
    def test_partial_protection_in_both_inboxes(self):
        for kind in ('movies_source_dir','jav_download_watch_dir'):
            for case in ('nested','related','unrelated','resumed'):
                with self.subTest(kind=kind,case=case),tempfile.TemporaryDirectory() as tmp:
                    root=Path(tmp);inbox=root/'inbox';inbox.mkdir();folder=inbox/'release' if case=='nested' else inbox;folder.mkdir(exist_ok=True)
                    video=folder/'movie.mp4';video.touch();library=root/'library';library.mkdir()
                    partial=folder/('movie.mp4.part' if case in ('related','resumed') else 'other.part')
                    if case!='resumed':partial.touch()
                    def build(*a):
                        if case=='resumed':partial.touch()
                        return 'nfo'
                    gate=Mock(side_effect=RuntimeError('move reached'));upsert=Mock()
                    env={'Path':Path,'os':os,'require_complete_queue_video':require_complete_queue_video,
                         'db':SimpleNamespace(get_settings=lambda:{kind:str(inbox),'features_dir':str(library)},upsert_movie=upsert),
                         '_movie_filing_stem':lambda m:'Movie (2020)','build_movie_nfo':build,'_quality_gate_move':gate,'emit':Mock()}
                    fn=load('main.py','_file_movie_impl',env)
                    if case=='unrelated':
                        with self.assertRaisesRegex(RuntimeError,'move reached'):fn(video,{'title':'Movie','year':'2020'})
                        gate.assert_called_once()
                    else:
                        with self.assertRaises(PartialDownloadPending):fn(video,{'title':'Movie','year':'2020'})
                        gate.assert_not_called()
                        if case!='resumed':upsert.assert_not_called()
                    self.assertTrue(video.exists());self.assertTrue(partial.exists())
                    self.assertEqual(list(library.rglob('.movie-nfo-*')),[])
