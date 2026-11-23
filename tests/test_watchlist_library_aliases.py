"""Library badge matching without the web-server dependency stack."""
import ast
import json
from pathlib import Path
import re
import sqlite3
from types import SimpleNamespace
import unicodedata
import unittest

class LibraryAliasTests(unittest.TestCase):
    def test_library_names_aliases_and_groups(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'app/core/performer_watchlist.py').read_text())
        conn = sqlite3.connect(':memory:')
        self.addCleanup(conn.close)
        conn.row_factory = sqlite3.Row
        ns = dict(json=json, re=re, unicodedata=unicodedata,
                  db=SimpleNamespace(get_conn=lambda:conn, _entry_id=lambda e:e.get('id') if isinstance(e,dict) else e),
                  SOURCES={'TPDB':None,'StashDB':None,'FansDB':None,'JAVStash':None}, EXTERNAL_SOURCES=('Coomer','IAFD'),
                  profile_image=lambda row:'')
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in {'install','name_key','matches_names','library_identity','rows'}:
                exec(compile(ast.Module(body=[node],type_ignores=[]),'watchlist.py','exec'),ns)
        ns['install'](conn)
        conn.execute('CREATE TABLE favourite_entities(kind TEXT, folder_name TEXT, aliases_json TEXT, match_tpdb_id TEXT, match_stashdb_id TEXT, match_fansdb_id TEXT, match_javstash_id TEXT, is_group INTEGER, group_ids_json TEXT)')
        conn.execute("INSERT INTO favourite_entities(kind,folder_name,aliases_json) VALUES('performer','Canonical','[\"Stage Alias\"]')")
        conn.execute("INSERT INTO favourite_entities(kind,folder_name,is_group,group_ids_json) VALUES('performer','Group',1,?)", (json.dumps({'stashdb':[{'id':'member','name':'Group Member'}]}),))
        conn.execute("INSERT INTO favourite_entities(kind,folder_name,aliases_json) VALUES('studio','Studio Only','broken')")
        for name, aliases in [('STAGE_ALIAS',[]),('Different',['Canonical']),('Stage',[]),('Group Member',[]),('Studio Only',[])]:
            conn.execute('INSERT INTO performer_watchlist(name,aliases) VALUES(?,?)',(name,json.dumps(aliases)))
        result = {r['name']:r for r in ns['rows']()}
        for name in ('STAGE_ALIAS','Different','Group Member'):
            self.assertTrue(result[name]['in_library'],name)
            self.assertIsNone(result[name]['confirmed'])
        for name in ('Stage','Studio Only'):
            self.assertFalse(result[name]['in_library'],name)
