import ast
import unittest
from pathlib import Path

class PipelineActivityTests(unittest.TestCase):
    def test_discovery_and_jobs_run_off_the_event_loop(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'app/core/pipeline.py').read_text())
        calls=[ast.unparse(n) for n in ast.walk(tree) if isinstance(n,ast.Await)]
        self.assertIn('await asyncio.to_thread(register_work)',calls)
        self.assertIn('await asyncio.to_thread(db.processing_work_ready)',calls)
        self.assertTrue(any('asyncio.to_thread(read,' in c for c in calls))
