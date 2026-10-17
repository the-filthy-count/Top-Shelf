import ast
import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest

source = ast.parse((Path(__file__).resolve().parents[1] / "main.py").read_text())
class PerformerLoadingTests(unittest.TestCase):
    def test_stub_cache_is_ignored_and_not_written(self):
        names = {"_performer_popup_cache_get", "_performer_popup_cache_set"}
        with tempfile.TemporaryDirectory() as directory:
            ns = {"json": json, "time": time, "_PERFORMER_POPUP_TTL_S": 3600, "_performer_popup_cache_dir": lambda: Path(directory)}
            exec(compile(ast.Module(body=[n for n in source.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[]), "main.py", "exec"), ns)
            path = Path(directory) / "old.json"
            path.write_text(json.dumps({"is_stub": True}))
            self.assertIsNone(ns["_performer_popup_cache_get"]("old"))
            ns["_performer_popup_cache_set"]("new", {"is_stub": True})
            self.assertFalse((Path(directory) / "new.json").exists())
            ns["_performer_popup_cache_set"]("good", {"is_stub": False})
            self.assertEqual(ns["_performer_popup_cache_get"]("good"), {"is_stub": False})

    def test_library_tpdb_identity_is_fetched(self):
        endpoint = next(n for n in source.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "api_performer_popup")
        gather = next(n for n in endpoint.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "_gather_tpdb_by_id")
        async def run(fn, value):
            return fn(value)
        ns = {"run_in_threadpool": run, "_tpdb_performer_fetch": lambda value: {"id": value}}
        exec(compile(ast.Module(body=[gather], type_ignores=[]), "main.py", "exec"), ns)
        for row, clicked, expected in [(None, "remote", "remote"), ({"match_tpdb_id": "saved"}, "remote", "saved"), ({}, "remote", "remote")]:
            ns.update(row=row, tid_arg=clicked)
            self.assertEqual(asyncio.run(ns["_gather_tpdb_by_id"]()), {"id": expected})

if __name__ == "__main__":
    unittest.main()
