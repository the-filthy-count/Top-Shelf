import sqlite3
import json
import unittest
from unittest.mock import patch
import database as db

class LinkedAliasesTests(unittest.TestCase):
    def test_merge_linked_only_preserve_manual_and_enable_filename_match(self):
        conn=sqlite3.connect(':memory:');conn.row_factory=sqlite3.Row
        conn.execute('CREATE TABLE favourite_entities(id INTEGER,kind TEXT,aliases_json TEXT,match_tpdb_id TEXT,match_stashdb_id TEXT,match_fansdb_id TEXT,match_javstash_id TEXT,updated_at TEXT)')
        conn.execute("INSERT INTO favourite_entities VALUES(1,'performer','[\"Manual Alias\"]','tp','st','fa','ja','old')");conn.commit()
        with patch.object(db,'get_conn',return_value=conn):
            self.assertTrue(db.favourite_merge_linked_aliases(1,[('TPDB',{'id':'tp','aliases':['Poppy Palmers','manual alias']}),('FansDB',{'id':'wrong','aliases':['Wrong Person']})]))
            saved=json.loads(conn.execute('SELECT aliases_json FROM favourite_entities').fetchone()[0])
            self.assertEqual(saved,['Manual Alias','Poppy Palmers'])
            self.assertFalse(db.favourite_merge_linked_aliases(1,[('TPDB',{'id':'tp','aliases':['Poppy Palmers']})]))
        index=[(tuple(db._tokenize_for_match(a,release=False)),'Strawberry Poppy',1) for a in saved]
        self.assertEqual(db._library_guess_from_release_name('Poppy Palmers - Janitor Arrangement.mp4',[],index)['performers'],['Strawberry Poppy'])
        self.assertEqual(db._library_guess_from_release_name('Palmers Poppy - Janitor Arrangement.mp4',[],index)['performers'],[])
        conn.close()

    def test_all_alias_shapes_are_indexed_and_shared_aliases_are_ambiguous(self):
        c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row
        c.execute('CREATE TABLE studio_logos(name TEXT,slug TEXT,logo_path TEXT)')
        c.execute('CREATE TABLE favourite_entities(id INTEGER,kind TEXT,folder_name TEXT,aliases_json TEXT,match_tpdb_name TEXT,match_stashdb_name TEXT,match_fansdb_name TEXT)')
        c.execute("INSERT INTO favourite_entities VALUES(1,'performer','Strawberry Poppy',?,NULL,NULL,NULL)",(json.dumps(['Poppy','strawberrykittenn','PP','Poppy Palmers']),))
        c.commit()
        with patch.object(db,'get_conn',return_value=c):
            indexes=db._build_library_match_indexes()
            for name in ('Poppy','strawberrykittenn','PP','PoppyPalmers'):
                self.assertEqual(db._library_guess_from_release_name(name+' - Arrangement.mp4',*indexes)['performers'],['Strawberry Poppy'])
            self.assertEqual(db._library_guess_from_release_name('Poppys - Arrangement.mp4',*indexes)['performers'],[])
            c.execute("INSERT INTO favourite_entities VALUES(2,'performer','Another Person','[\"Poppy\"]',NULL,NULL,NULL)");c.commit()
            indexes=db._build_library_match_indexes()
            self.assertEqual(db._library_guess_from_release_name('Poppy - Arrangement.mp4',*indexes)['performers'],[])
            self.assertEqual(db._library_guess_from_release_name('Poppy Palmers - Arrangement.mp4',*indexes)['performers'],['Strawberry Poppy'])
        c.close()

    def test_stashbox_detail_query_supplies_identity_for_alias_sync(self):
        import ast
        from pathlib import Path
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        query=next(n.value.value for n in tree.body if isinstance(n,ast.Assign)
                   and any(isinstance(t,ast.Name) and t.id=='FANSDB_PERFORMER_DETAIL_GQL' for t in n.targets))
        fields=query.split('findPerformer(id: $id) {',1)[1].split('images',1)[0].split()
        self.assertIn('id',fields)
        self.assertIn('aliases',fields)

    def test_locked_refresh_syncs_aliases_before_returning(self):
        import ast
        from pathlib import Path
        from unittest.mock import Mock
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='favourites_refresh_entity_row')
        row={'id':1,'kind':'performer','matches_locked':1}
        fake_db=Mock();fake_db.favourite_get.return_value=row
        sync=Mock()
        ns={'db':fake_db,'_sync_linked_performer_aliases':sync,'Path':Path}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'main.py','exec'),ns)
        self.assertTrue(ns['favourites_refresh_entity_row'](1)['skipped'])
        sync.assert_called_once_with(row)

    def test_manual_and_bulk_refresh_reach_locked_alias_refresh(self):
        import ast
        import asyncio
        from pathlib import Path
        from unittest.mock import Mock
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        names = {'api_favourites_refresh', '_favourites_refresh_all_loop'}
        functions = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
        for fn in functions:
            fn.decorator_list = []
        refresh = Mock(return_value={'ok': True, 'skipped': True})
        clear = Mock()
        ns = {'Body': lambda *args, **kw: None, '_favourite_images_locked': lambda rid: True,
              'favourites_clear_image_disk_cache': clear,
              'favourites_refresh_entity_row': refresh,
              '_favourites_progress_update': Mock(), 'emit': Mock()}
        exec(compile(ast.Module(body=functions, type_ignores=[]), 'main.py', 'exec'), ns)
        asyncio.run(ns['api_favourites_refresh']({'id': 1}))
        refresh.assert_called_once_with(1, scrape_aliases=True, only_missing=False)
        clear.assert_not_called()
        refresh.reset_mock()
        ns['_favourites_refresh_all_loop']([{'id': 1, 'folder_name': 'Strawberry Poppy'}])
        refresh.assert_called_once_with(1, scrape_aliases=True, only_missing=False)
        clear.assert_not_called()

    def test_link_confirmation_syncs_selected_source_before_return(self):
        import ast
        import asyncio
        from pathlib import Path
        from unittest.mock import Mock
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        names = {'api_favourites_set_match', 'api_favourites_match'}
        functions = [n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name in names]
        for fn in functions:
            fn.decorator_list = []
        row = {'id': 1, 'kind': 'performer', 'matches_locked': 1}
        fake_db = Mock()
        fake_db.favourite_get.return_value = row
        events = []
        def save(rid, **fields):
            row.update(fields)
            events.append('save')
        fake_db.favourite_update_matches.side_effect = save
        def sync(saved, source):
            self.assertEqual(saved['match_stashdb_id'], 'linked-id')
            self.assertEqual(source.casefold(), 'stashdb')
            events.append('sync')
        async def run(fn, *args):
            return fn(*args)
        ns = {'Body': lambda *a, **kw: None, 'db': fake_db, 'run_in_threadpool': run,
              '_sync_linked_performer_aliases': sync,
              '_favourite_find_local_poster_file': lambda p: False,
              '_favourite_performer_sort_birth_date': Mock(),
              '_submit_enrichment': Mock(), '_enrich_performer_ext_links_bg': Mock(),
              '_performer_popup_cache_invalidate_row': Mock()}
        exec(compile(ast.Module(body=functions, type_ignores=[]), 'main.py', 'exec'), ns)
        for endpoint in names:
            events.clear()
            result = asyncio.run(ns[endpoint]({'row_id': 1, 'source': 'StashDB',
                'id': 'linked-id', 'external_id': 'linked-id', 'name': 'Strawberry Poppy'}))
            self.assertTrue(result['ok'])
            self.assertEqual(events, ['save', 'sync'])

    def test_selected_source_sync_does_not_query_other_linked_databases(self):
        import ast
        from pathlib import Path
        from unittest.mock import Mock
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_sync_linked_performer_aliases')
        profile = {'id': 'linked-id', '_source': 'StashDB', 'aliases': ['Poppy Palmers']}
        fetch = Mock(return_value=profile)
        fake_db = Mock()
        fake_db.favourite_merge_linked_aliases.return_value = True
        ns = {'db': fake_db, '_performer_popup_stashbox_bio': fetch,
              'fetch_performer_detail': Mock(), '_log': Mock(),
              '_favourites_name_index_bump': Mock(), '_favourites_warm': Mock(),
              '_performer_popup_cache_invalidate_row': Mock()}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'main.py', 'exec'), ns)
        row = {'id': 1, 'kind': 'performer', 'match_stashdb_id': 'linked-id',
               'match_tpdb_id': 'another-id', 'match_fansdb_id': 'other-id'}
        self.assertTrue(ns['_sync_linked_performer_aliases'](row, 'STASHDB'))
        fetch.assert_called_once_with('linked-id', False)
        ns['fetch_performer_detail'].assert_not_called()
        fake_db.favourite_merge_linked_aliases.assert_called_once_with(1, [('StashDB', profile)])
        ns['_favourites_name_index_bump'].assert_called_once()
