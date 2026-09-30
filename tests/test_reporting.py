"""Completed-round evidence survives cancellation without replacing prior results."""
import asyncio
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import select
import subprocess
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from serving_lab.cli import experiment
from serving_lab.reporting import Output


def arguments(output):
    return SimpleNamespace(workload=None, profile="agent-mix", seed=42, count=1,
        model="fake-only", api_key_env="SERVING_LAB_TEST_UNUSED", timeout=60,
        logprobs=False, tokenize=False, concurrency=1, aging=.2, repeats=2,
        policy="fcfs", starvation=.75, output=output)


def completed(status="ok"):
    return [{"id": "r0", "kind": "short", "status": status,
        "released_s": 0, "admitted_s": .1, "end_s": 1,
        "chunk_times_s": [.1], "chunk_token_counts": [1],
        "usage": {"completion_tokens": 1},
        "error": "http_429" if status == "failed" else None}], 1


class ReportingTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_or_exception_keeps_first_round_and_cleans_backend(self):
        for error, status in ((asyncio.CancelledError(), "interrupted"),
                              (RuntimeError("secret-url-and-key"), "error")):
            with self.subTest(status=status), TemporaryDirectory() as directory:
                backend = SimpleNamespace(close=AsyncMock())
                steps = AsyncMock(side_effect=[completed(), error])
                with patch("serving_lab.cli.OpenAIBackend", return_value=backend), \
                        patch("serving_lab.cli.run", steps), redirect_stdout(io.StringIO()):
                    with self.assertRaises(type(error)):
                        await experiment(arguments(directory), "unused", "fake_test")
                raw = (Path(directory) / "report.json").read_text()
                report = json.loads(raw)
                self.assertEqual(report["batch_status"], status)
                self.assertEqual((report["completed_runs"], report["planned_runs"]), (1, 2))
                self.assertEqual(report["runs"][0]["requests"], completed()[0])
                self.assertEqual(report["config"]["starvation_s"], .75)
                self.assertEqual(report["run_plan"], [{"policy": "fcfs", "repeat": 0}, {"policy": "fcfs", "repeat": 1}])
                self.assertIn(f"**{status}**", (Path(directory) / "report.md").read_text())
                self.assertNotIn("secret-url-and-key", raw)
                if status == "error":
                    self.assertEqual((report["error_type"], report["error_stage"]), ("RuntimeError", "run"))
                backend.close.assert_awaited_once()

    async def test_round_is_saved_before_next_begins_and_failures_complete(self):
        with TemporaryDirectory() as directory:
            backend = SimpleNamespace(close=AsyncMock())
            calls = []

            async def run(*args):
                snapshot = json.loads((Path(directory) / "report.json").read_text())
                self.assertEqual(snapshot["completed_runs"], len(calls))
                self.assertEqual(snapshot["batch_status"], "running")
                calls.append(True)
                return completed("failed" if len(calls) == 2 else "ok")

            with patch("serving_lab.cli.OpenAIBackend", return_value=backend), \
                    patch("serving_lab.cli.run", side_effect=run), redirect_stdout(io.StringIO()):
                code = await experiment(arguments(directory), "unused", "fake_test")
            report = json.loads((Path(directory) / "report.json").read_text())
            self.assertEqual(code, 1)
            self.assertEqual(report["batch_status"], "completed")
            self.assertEqual(report["completed_runs"], 2)
            self.assertEqual(report["runs"][0]["metrics"]["e2e_s"]["mean"], 1)
            self.assertEqual(report["runs"][1]["metrics"]["failed"], 1)
            backend.close.assert_awaited_once()

    async def test_existing_results_rejected_before_any_backend_work(self):
        for name in ("workload.json", "report.json", "report.md"):
            with self.subTest(name=name), TemporaryDirectory() as directory:
                target = Path(directory) / name
                target.write_bytes(b"user-owned evidence\n")
                other = Path(directory) / "notes.txt"
                other.write_bytes(b"unrelated")
                args = arguments(directory)
                args.tokenize = True
                with patch("serving_lab.cli.OpenAIBackend") as backend, patch("serving_lab.cli.run") as run:
                    with self.assertRaisesRegex(FileExistsError, "new --output"):
                        await experiment(args, "unused", "fake_test")
                backend.assert_not_called()
                run.assert_not_called()
                self.assertEqual(target.read_bytes(), b"user-owned evidence\n")
                self.assertEqual(other.read_bytes(), b"unrelated")
                self.assertEqual({p.name for p in Path(directory).iterdir()}, {name, "notes.txt"})

    async def test_setup_failure_is_recorded_and_no_request_is_fabricated(self):
        with TemporaryDirectory() as directory:
            with patch("serving_lab.cli.OpenAIBackend", side_effect=ValueError("private")), redirect_stdout(io.StringIO()):
                with self.assertRaises(ValueError):
                    await experiment(arguments(directory), "unused", "fake_test")
            report = json.loads((Path(directory) / "report.json").read_text())
            self.assertEqual(report["batch_status"], "error")
            self.assertEqual(report["error_stage"], "backend_setup")
            self.assertEqual(report["runs"], [])

    def test_unique_defaults_and_unrelated_files(self):
        with TemporaryDirectory() as directory:
            before = Path.cwd()
            os.chdir(directory)
            try:
                first, second = Output(), Output()
                self.assertNotEqual(first.path, second.path)
                self.assertRegex(first.path.name, r"^run-\d{8}T\d{6}Z-\w+$")
                self.assertTrue(second.path.is_dir())
                other = Path("explicit")
                other.mkdir()
                (other / "notes.txt").write_text("keep")
                Output(other)
                self.assertEqual((other / "notes.txt").read_text(), "keep")
            finally:
                os.chdir(before)

    def test_failed_atomic_replace_keeps_last_readable_json(self):
        with TemporaryDirectory() as directory:
            output = Output(directory)
            output.write("report.json", '{"completed_runs":1}\n')
            with patch("serving_lab.reporting.os.replace", side_effect=OSError("injected")):
                with self.assertRaises(OSError):
                    output.write("report.json", '{"completed_runs":2}\n')
            self.assertEqual(json.loads((Path(directory) / "report.json").read_text()), {"completed_runs": 1})
            self.assertFalse(list(Path(directory).glob(".report.json-*")))

    def test_sigkill_preserves_completed_round_as_running(self):
        script = r'''
import asyncio, sys
from unittest.mock import patch
from serving_lab.cli import experiment
from tests.test_reporting import arguments, completed
calls = 0
class Backend:
    def __init__(self, *args, **kwargs): pass
    async def close(self): pass
async def run(*args):
    global calls
    calls += 1
    if calls == 2:
        print("SECOND_ROUND", flush=True)
        await asyncio.Event().wait()
    return completed()
async def main():
    with patch("serving_lab.cli.OpenAIBackend", Backend), patch("serving_lab.cli.run", run):
        await experiment(arguments(sys.argv[1]), "unused", "fake_test")
asyncio.run(main())
'''
        with TemporaryDirectory() as directory:
            # Import the local test helper without installing or invoking a model.
            script = script.replace("from tests.test_reporting", "from test_reporting")
            env = {**os.environ, "PYTHONPATH": str(Path(__file__).parent.resolve()) + os.pathsep + str(Path(__file__).parents[1].resolve())}
            process = subprocess.Popen([sys.executable, "-u", "-c", script, directory],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, bufsize=0)
            try:
                lines = []
                while b"SECOND_ROUND\n" not in lines:
                    ready, _, _ = select.select([process.stdout], [], [], 5)
                    self.assertTrue(ready, "fake worker did not reach second round")
                    line = process.stdout.readline()
                    self.assertTrue(line, "fake worker exited before second round")
                    lines.append(line)
                process.kill()
                process.wait(timeout=5)
                report = json.loads((Path(directory) / "report.json").read_text())
                self.assertEqual(report["batch_status"], "running")
                self.assertEqual(report["completed_runs"], 1)
                self.assertEqual(report["runs"][0]["requests"], completed()[0])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                process.stdout.close()
                process.stderr.close()
