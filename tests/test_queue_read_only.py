import ast
import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from fastapi.responses import JSONResponse


class ReadOnlyQueueTests(unittest.TestCase):
    def test_gets_only_read_materialized_snapshots(self):
        tree = ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                 and n.name in {'get_queue', 'movies_queue', 'movies_stats'}]
        for node in nodes: node.decorator_list = []
        cache = SimpleNamespace(peek=Mock(return_value=None))
        db = SimpleNamespace(get_movie_stats=Mock(return_value={'total': 2}))
        env = {'_queue_warm': cache, '_movies_queue_warm': cache, 'JSONResponse': JSONResponse, 'db': db}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'queue-get', 'exec'), env)
        self.assertEqual(asyncio.run(env['get_queue']()).status_code, 202)
        self.assertEqual(env['movies_queue']().status_code, 202)
        self.assertEqual(asyncio.run(env['movies_stats']()), {'total': 2})
        payload = {'files': [{'filename': 'example'}]}; cache.peek.return_value = payload
        self.assertIs(asyncio.run(env['get_queue']()), payload)
        self.assertIs(env['movies_queue'](), payload)
