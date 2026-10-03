import ast
import math
import subprocess
import unittest
from pathlib import Path
from unittest.mock import Mock


class DurationProbeTests(unittest.TestCase):
    def load_probe(self, outcomes):
        tree = ast.parse(Path("app/core/phash.py").read_text())
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "get_video_duration")
        runner = Mock(side_effect=outcomes)
        namespace = {"Path": Path, "math": math, "subprocess": Mock(
            run=runner, CalledProcessError=subprocess.CalledProcessError,
            TimeoutExpired=subprocess.TimeoutExpired)}
        exec(compile(ast.Module(body=[function], type_ignores=[]), "probe", "exec"), namespace)
        return namespace["get_video_duration"], runner

    def test_stream_header_precedes_packet_scan(self):
        probe, runner = self.load_probe([Mock(stdout="N/A"), Mock(stdout="120")])
        self.assertEqual(probe(Path("video.mp4")), 120)
        self.assertEqual(runner.call_count, 2)
        self.assertIn("stream=duration", runner.call_args.args[0])
        self.assertTrue(all(c.kwargs["timeout"] > 0 for c in runner.call_args_list))

    def test_all_stalled_probes_stop(self):
        probe, runner = self.load_probe([subprocess.TimeoutExpired("ffprobe", 20)] * 3)
        with self.assertRaises(ValueError):
            probe(Path("video.mp4"))
        self.assertEqual(runner.call_count, 3)
        self.assertLessEqual(sum(c.kwargs["timeout"] for c in runner.call_args_list), 70)

    def test_invalid_duration_falls_back(self):
        probe, runner = self.load_probe([Mock(stdout="nan"), Mock(stdout="inf"), Mock(stdout="123")])
        self.assertEqual(probe(Path("video.mp4")), 123)


if __name__ == "__main__":
    unittest.main()
