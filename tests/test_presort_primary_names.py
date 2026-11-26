import json
import re
import sqlite3
import threading
import time
import unittest
from types import SimpleNamespace
from test_filename_identity import load_function


class PrimaryNameTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.addCleanup(self.conn.close)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('CREATE TABLE studio_logos(name, slug, logo_path)')
        self.conn.execute('CREATE TABLE favourite_entities(id, kind, folder_name, aliases_json, match_tpdb_name, match_stashdb_name, match_fansdb_name)')
        self.rows = [dict(id=1, kind='performer', folder_name='Lily Jordan', aliases_json='["Liz Jordan", "Shared Alias"]'),
                     dict(id=2, kind='performer', folder_name='Liz Jordan', aliases_json='["Lily Jordan", "Shared Alias"]')]
        for row in self.rows:
            self.conn.execute('INSERT INTO favourite_entities VALUES (?, ?, ?, ?, NULL, NULL, NULL)', tuple(row.values()))
        self.env = {'re': re, 'json': json, 'get_conn': lambda: self.conn,
                    '_tokenize_for_match': lambda s, **kw: re.findall('[a-z0-9]+', s.lower()),
                    '_normalize_release_for_match': lambda s: re.sub('[^a-z0-9]', '', s.lower())}
        self.build = load_function('database.py', '_build_library_match_indexes', self.env)
        self.guess = load_function('database.py', '_library_guess_from_release_name', self.env)

    def test_primary_beats_other_rows_alias_and_shared_alias_stays_blocked(self):
        indexes = self.build()
        for name in ('Liz Jordan', 'Lily Jordan', 'LizJordan'):
            expected = 'Liz Jordan' if name == 'LizJordan' else name
            self.assertEqual(self.guess(name + ' - bg rockstar.mp4', *indexes)['performers'], [expected])
        self.assertEqual(self.guess('Shared Alias scene.mp4', *indexes)['performers'], [])

    def test_duplicate_primary_stays_ambiguous(self):
        self.conn.execute("INSERT INTO favourite_entities VALUES (3, 'performer', 'Liz Jordan', NULL, NULL, NULL, NULL)")
        self.assertEqual(self.guess('Liz Jordan - bg rockstar.mp4', *self.build())['performers'], [])

    def test_destination_primary_wins_regardless_of_row_order(self):
        env = {'time': time, 'threading': threading, 'json': json,
               '_NAME_INDEX_LOCK': threading.Lock(), '_NAME_INDEX_CACHE': {}, '_NAME_INDEX_TTL_S': 60,
               'normalise': lambda s: s.lower(), 'db': SimpleNamespace(favourite_list=lambda: self.rows)}
        build = load_function('main.py', '_favourites_name_index', env)
        self.assertEqual(build('performer')['liz jordan']['id'], 2)
        self.assertEqual(build('performer')['lily jordan']['id'], 1)
