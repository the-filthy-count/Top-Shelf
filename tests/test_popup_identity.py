"""Exercise popup library resolution without external services or app startup."""
import ast
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

class PopupIdentityTests(unittest.IsolatedAsyncioTestCase):
    def resolver(self, crosswalk=None):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='api_performer_popup')
        # Stop after library identity resolution, before external bio retrieval.
        end=next(i for i,n in enumerate(fn.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='row_stashdb_id' for t in n.targets))
        fn.body=fn.body[:end]+[ast.Return(value=ast.Name(id='row',ctx=ast.Load()))]
        fn.decorator_list=[]
        wrong={'id':10,'kind':'performer','folder_name':'Jennifer White'}
        db=SimpleNamespace(favourite_find_performer_folder_by_crosswalk=Mock(return_value=crosswalk),
             favourite_get=Mock(return_value={'id':20,'kind':'performer','folder_name':'Ember'}),
             favourite_lookup_performer_by_name=Mock(return_value=None),
             favourite_lookup_performer_by_alias=Mock(return_value=wrong))
        env={'db':db,'_performer_popup_safe_key':lambda x:x}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[fn],type_ignores=[])),'main.py','exec'),env)
        return env['api_performer_popup'],db

    async def test_external_ids_never_fall_back_to_alias(self):
        for key in ['stash_id','tpdb_id','fansdb_id','javstash_id']:
            with self.subTest(source=key):
                resolve,db=self.resolver()
                self.assertIsNone(await resolve(**{key:'ember-source-id'},name='Ember',refresh=1))
                db.favourite_lookup_performer_by_name.assert_not_called()
                db.favourite_lookup_performer_by_alias.assert_not_called()
    async def test_matching_id_resolves_library_row(self):
        resolve,db=self.resolver({'id':20})
        row=await resolve(stash_id='ember-source-id',name='Ember',refresh=1)
        self.assertEqual(row['folder_name'],'Ember')
        db.favourite_lookup_performer_by_alias.assert_not_called()
    async def test_name_only_lookup_still_supported(self):
        resolve,db=self.resolver()
        row=await resolve(name='Ember',refresh=1)
        self.assertEqual(row['id'],10)
