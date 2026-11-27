import ast
import asyncio
from pathlib import Path
import unittest
from unittest.mock import Mock


class HomeSectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'api_home_data')
        fn.decorator_list = []
        cls.code = compile(ast.Module(body=[fn], type_ignores=[]), 'main.py', 'exec')

    def setUp(self):
        async def run(fn, *args):
            return fn(*args)
        class HTTPException(Exception):
            def __init__(self, status_code, detail): self.status_code = status_code
        self.ns = {'run_in_threadpool': run, 'HTTPException': HTTPException,
                   '_HOME_LAST_REFRESH': {'in_flight': False, 'ts': 1}}
        values = {
            '_home_latest_performers': [{'id': 1}], '_home_favourites_only': [],
            '_home_latest_movies': [{'id': 2}], '_home_latest_jav': [{'id': 3}],
            '_home_movies_favourites': [], '_home_favourite_name_sets': (set(), set()),
            '_home_downloaded_scenes_by_kind': {'all': [{'history_id': 4}]},
            '_home_scenes_favourite_filter': [], '_home_discovered_performers': [{'id': 5}],
            '_home_background_refresh': None,
        }
        for name, value in values.items(): self.ns[name] = Mock(return_value=value)
        exec(self.code, self.ns)

    def test_each_section_is_independent(self):
        for section in ('performers', 'movies', 'jav', 'scenes', 'discovered'):
            with self.subTest(section=section):
                result = asyncio.run(self.ns['api_home_data'](section=section))
                self.assertEqual(len(result[section]), 1)
        self.assertEqual(self.ns['_home_background_refresh'].call_count, 1)

    def test_stars_do_not_wait_for_movie_or_discovery_work(self):
        self.ns['_home_movies_favourites'].side_effect = RuntimeError('unavailable mount')
        self.ns['_home_discovered_performers'].side_effect = RuntimeError('bad pool')
        result = asyncio.run(self.ns['api_home_data'](section='performers'))
        self.assertEqual(result['performers'], [{'id': 1}])
        self.ns['_home_movies_favourites'].assert_not_called()
        self.ns['_home_discovered_performers'].assert_not_called()

    def test_unknown_section_is_rejected(self):
        with self.assertRaises(self.ns['HTTPException']) as error:
            asyncio.run(self.ns['api_home_data'](section='unknown'))
        self.assertEqual(error.exception.status_code, 400)


if __name__ == '__main__': unittest.main()
