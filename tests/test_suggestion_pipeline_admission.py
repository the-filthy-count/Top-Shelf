import ast
import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

class SuggestionAdmissionTests(unittest.TestCase):
    def test_request_registers_and_returns_without_waiting_for_backlog(self):
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='api_queue_suggestions_run')
        fn.decorator_list=[]
        async def inline(fn,*a,**kw):return fn(*a,**kw)
        worker=Mock(side_effect=AssertionError('HTTP request must not drain backlog'))
        env={'BackgroundTasks':object,'Body':lambda **kw:{},'run_in_threadpool':inline,'run_pipeline':worker}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'main.py','exec'),env)
        background=Mock()
        with patch('app.core.queue_changes.schedule_suggestions',return_value=True) as schedule:
            result=asyncio.run(env['api_queue_suggestions_run'](background,{'filename':'video.mp4'}))
        self.assertEqual(result['queued'],1)
        schedule.assert_called_once_with('video.mp4',include_iafd=True)
        background.add_task.assert_called_once_with(worker,[])
        worker.assert_not_called()

    def test_queued_row_does_not_report_a_completed_zero_result_search(self):
        script=(Path(__file__).resolve().parents[1]/'static/queue.js').read_text()
        start=script.index('async function runRowSuggestions')
        self.assertLess(script.index('if ((d.queued || d.pending) && !hasCached)',start),
                        script.index('if (newlyAdded === 0 && !hasCached)',start))
