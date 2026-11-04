import ast
import re
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, Mock
import database as db
from app.core import matching

FILENAME = 'Demi Hawks/[Demi Hawks] Demi Hawks - FuckPassVR 2023-10-13 Demi Hawks Summer Fling In Saratoga Springs.mp4'
TITLE = 'Summer Fling In Saratoga Springs'

class FolderTagSearchTests(unittest.TestCase):
    def setUp(self):
        self.guess = patch.object(db, '_library_guess_from_release_name', return_value={'performers':['Demi Hawks'], 'studio':'demi_hawks'})
        self.logos = patch.object(db, 'studio_logo_list_all', return_value=[{'name':'demi_hawks','slug':'demi_hawks'}, {'name':'FuckPassVR','slug':'fuckpassvr'}])
        indexes = patch.object(db, '_build_library_match_indexes', return_value=([], []))
        indexes.start(); self.addCleanup(indexes.stop)
        self.guess.start(); self.logos.start()
        self.addCleanup(self.guess.stop); self.addCleanup(self.logos.stop)
        tree = ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        names = {'parse_filename', '_parse_filename_parts', '_ensure_parsed_filename'}
        self.env = {'Path':Path,'re':re,'datetime':datetime,'timezone':timezone,'_strip_title_noise':lambda text:text}
        exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[]),'parser','exec'),self.env)

    def test_automatic_query_excludes_creator_duplicate_and_real_studio(self):
        _, title, performer, studio, cast = matching._split_query_for_search(FILENAME)
        self.assertEqual((title,performer,studio,cast),(TITLE,'Demi Hawks','FuckPassVR',['Demi Hawks']))
        scene={'title':TITLE,'performers':[{'performer':{'name':'Demi Hawks'}}], 'duration':3720}
        self.assertEqual(matching._score_name_match(title,scene,query_title=title,query_performers=cast,file_meta={'duration_s':3780}),1)

    def test_form_parser_preserves_title_date_and_correct_identity(self):
        result=self.env['parse_filename'](FILENAME,db.DEFAULTS)
        self.assertEqual({k:result[k] for k in ('title','performers','site','date','folder_tag')},{'title':TITLE,'performers':'Demi Hawks','site':'FuckPassVR','date':'2023-10-13','folder_tag':True})

    def test_actual_bracket_studio_format_still_works(self):
        result=self.env['parse_filename']('Other Folder/[Actual Studio] Private Lesson - Jane Doe.mp4',db.DEFAULTS)
        self.assertEqual(result['site'],'Actual Studio')
        self.assertEqual(result['title'],'Private Lesson')
        self.assertNotIn('folder_tag',result)

    def test_old_parse_cache_is_refreshed(self):
        save=Mock()
        self.env.update(db=SimpleNamespace(get_parsed_meta=lambda _: {'title':'Demi Hawks'},set_parsed_meta=save),
                        _resolve_queue_file_path=lambda *args: None,emit=Mock())
        result=self.env['_ensure_parsed_filename'](FILENAME,db.DEFAULTS,Path('/nonexistent'))
        self.assertEqual(result['title'],TITLE); self.assertEqual(result['parser_version'],3)
        save.assert_called_once()

    def test_cache_tracks_settings_and_library_inputs(self):
        saved = {}
        self.env.update(db=SimpleNamespace(
            get_parsed_meta=lambda _: saved.copy() or None,
            set_parsed_meta=lambda _, value: saved.update(value)),
            _resolve_queue_file_path=lambda *args: None, emit=Mock())
        ensure = self.env['_ensure_parsed_filename']
        original = self.env['parse_filename']
        parser = Mock(wraps=original)
        self.env['parse_filename'] = parser
        ensure(FILENAME, db.DEFAULTS, Path('/nonexistent'))
        ensure(FILENAME, db.DEFAULTS, Path('/nonexistent'))
        self.assertEqual(parser.call_count, 1)
        settings = dict(db.DEFAULTS, filename_strip_words='Summer')
        result = ensure(FILENAME, settings, Path('/nonexistent'))
        self.assertNotIn('Summer', result['title'])
        self.assertEqual(parser.call_count, 2)
        with patch.object(db, '_build_library_match_indexes', return_value=([], [('new', 'name', 1)])):
            ensure(FILENAME, settings, Path('/nonexistent'))
        self.assertEqual(parser.call_count, 3)
        with patch.object(db, 'studio_logo_list_all', return_value=[{'name':'New Studio','slug':'new'}]):
            ensure(FILENAME, settings, Path('/nonexistent'))
        self.assertEqual(parser.call_count, 4)
