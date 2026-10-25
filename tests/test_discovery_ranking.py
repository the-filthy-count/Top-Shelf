import ast
import json
import unittest
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

class DiscoveryRankingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / 'main.py'
        tree = ast.parse(path.read_text())
        names = {'_discovery_recency_bonus', '_home_discovered_performers'}
        cls.code = compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[]), str(path), 'exec')

    def setUp(self):
        class Connection:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, *args): return self
            def fetchall(self): return []
            def __iter__(self): return iter([{'kind':'studio','folder_name':'Favourite Studio','aliases_json':'[]'}])
        self.scenes = []
        self.ns = {'json': json, 'db': SimpleNamespace(get_conn=Connection, get_settings=lambda: {}), '_home_recent_scenes': lambda limit: self.scenes}
        exec(self.code, self.ns)

    def scene(self, source, pid, name, aliases=(), studio=''):
        return {'source':source, 'date':date.today().isoformat(), 'studio':studio, 'display_performers':[{'id':pid,'name':name,'gender':'female','aliases':list(aliases)}]}

    def results(self): return self.ns['_home_discovered_performers']()

    def test_aliases_merge_across_sources(self):
        self.scenes = [self.scene('stashdb','one','Jane Alpha',['Jane Beta']), self.scene('fansdb','two','Jane Beta')]
        result = self.results()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['appearances'], 2)
        self.assertEqual(result[0]['stash_id'], 'one')
        self.assertEqual(result[0]['fansdb_id'], 'two')
        self.assertEqual(sum(result[0]['recency_sources'].values()), 6)

    def test_unrelated_source_ids_remain_separate(self):
        self.scenes = [self.scene('stashdb','same','Jane Alpha'), self.scene('fansdb','same','Mary Other')]
        self.assertEqual(len(self.results()), 2)

    def test_repeated_credit_counts_once(self):
        scene = self.scene('javstash','one','Jane Alpha')
        scene['display_performers'].append(dict(scene['display_performers'][0], name='Alternate Credit'))
        self.scenes = [scene]
        self.assertEqual(self.results()[0]['appearances'], 1)

    def test_favourites_do_not_multiply_recency(self):
        self.scenes = [self.scene('stashdb','one','Jane Alpha',studio='Favourite Studio'), self.scene('stashdb','two','Mary Other')]
        result = {r['name']:r for r in self.results()}
        self.assertEqual(result['Jane Alpha']['recency_sources'], result['Mary Other']['recency_sources'])
        self.assertGreater(sum(result['Jane Alpha']['affinity_sources'].values()), sum(result['Mary Other']['affinity_sources'].values()))

    def test_smooth_decay_and_bad_dates(self):
        f = self.ns['_discovery_recency_bonus']
        today = date.today()
        for age, expected in [(0,3),(30,1.5),(60,.75),(90,.375)]:
            self.assertAlmostEqual(f(str(today-timedelta(days=age)), today), expected)
        for boundary in (30,60):
            self.assertAlmostEqual(f(str(today-timedelta(days=boundary+1)),today)/f(str(today-timedelta(days=boundary)),today),2**(-1/30))
        self.assertEqual(f('invalid',today),0)
        self.assertEqual(f('',today),0)
        self.assertEqual(f(str(today+timedelta(days=20)),today),3)

if __name__ == '__main__': unittest.main()
