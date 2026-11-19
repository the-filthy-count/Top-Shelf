"""Recurring maintenance must not inherit blocked one-shot command state."""
import ast
from pathlib import Path
import unittest
from unittest.mock import Mock


class QueueMaintenanceScheduleTests(unittest.TestCase):
    def test_sweep_is_repeatable_but_individual_moves_remain_journalled(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text())
        loop = next(n for n in tree.body if isinstance(n, ast.For)
                    and isinstance(n.target, ast.Name) and n.target.id == '_action_name')
        sweep = Mock()
        names = ast.literal_eval(loop.iter)
        namespace = {name: Mock() for name in names}
        namespace['_maintain_queue_paths'] = sweep
        def durable(name):
            def wrap(fn):
                def blocked(*args, **kwargs):
                    raise ValueError('Interrupted action requires review before retrying')
                return blocked
            return wrap
        namespace['_durable_command'] = durable
        exec(compile(ast.Module(body=[loop], type_ignores=[]), 'main.py', 'exec'), namespace)
        namespace['_maintain_queue_paths']()
        namespace['_maintain_queue_paths']()
        self.assertEqual(sweep.call_count, 2)
        group = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                     and n.name == '_group_queue_root_files_by_hint_locked')
        calls = {n.func.id for n in ast.walk(group) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        self.assertIn('_apply_queue_relocation', calls)
        self.assertIn('require_complete_download', calls)
