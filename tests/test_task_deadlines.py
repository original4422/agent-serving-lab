"""Whole-task budgets tested with controlled clocks and counted HTTP requests."""
import asyncio
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, patch

from serving_lab.backend import OpenAIBackend
from serving_lab.cli import experiment
from serving_lab.metrics import summarize, summarize_tasks
from serving_lab.scheduler import run
from serving_lab.tasks import outcomes
from serving_lab.workload import generate, validate
from test_deadlines import counted_server
from test_reporting import arguments


def chain(length=2, budget=1):
    work = generate(count=length)
    for i, request in enumerate(work["requests"]):
        request.update(arrival_s=0, tool_delay_s=0, messages=[{"role": "user", "content": request["id"]}])
        request.pop("after", None)
        if i:
            request["after"] = f"r{i - 1}"
    work["tasks"] = [{"id": "answer", "request_ids": [r["id"] for r in work["requests"]], "deadline_s": budget}]
    return work


def result(error=None):
    return {"error": error, "chunk_times_s": [.01], "chunk_token_counts": [1],
            "usage": {"completion_tokens": 1}, "duration_s": .01}


async def controlled(work, durations, *, honor_deadline=True, error=None, wake_late=0):
    loop = asyncio.get_running_loop()
    clock, calls = [1000.0], []
    original_sleep = asyncio.sleep

    class Backend:
        async def stream(self, request, **options):
            begin = clock[0]
            duration = durations[request["id"]]
            limit = options.get("deadline_at")
            calls.append({"id": request["id"], "start": begin - 1000,
                          "deadline": limit - 1000 if limit is not None else None})
            if honor_deadline and limit is not None and begin + duration >= limit:
                clock[0] = limit
                return result("deadline_during_stream")
            clock[0] += duration
            return result(error)

    async def sleep(delay):
        clock[0] += delay + wake_late
        await original_sleep(0)

    with patch.object(loop, "time", side_effect=lambda: clock[0]), \
            patch("serving_lab.scheduler.asyncio.sleep", side_effect=sleep):
        records, elapsed = await run(work, Backend(), concurrency=1)
    return records, elapsed, calls


class TaskDeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        asyncio.get_running_loop().set_debug(False)  # Synthetic clock jumps are not slow callbacks.

    async def test_shared_budget_is_remaining_time_not_equal_slices(self):
        for second, expected in ((.75, "expired"), (.1, "ok")):
            with self.subTest(second=second):
                work = chain()
                for request in work["requests"]:
                    request["deadline_s"] = 1
                records, elapsed, calls = await controlled(work, {"r0": .75, "r1": second})
                self.assertEqual([r["status"] for r in records], ["ok", expected])
                self.assertEqual([call["deadline"] for call in calls], [1, 1])
                self.assertEqual(calls[1]["start"], .75)
                task = outcomes(work, records)[0]
                self.assertEqual(task["status"], expected)
                if expected == "expired":
                    self.assertEqual(task["reason"], "task_deadline_during_stream")
                    self.assertEqual(task["deciding_request_id"], "r1")
                else:
                    self.assertEqual(summarize_tasks([task])["succeeded"], 1)
                    self.assertEqual(summarize(records, elapsed, 1)["succeeded"], 2)

    async def test_tool_wait_and_future_arrival_expire_without_released_or_admitted_time(self):
        for delay, arrival in ((2, 0), (0, 2)):
            with self.subTest(delay=delay, arrival=arrival):
                work = chain(3)
                work["requests"][1].update(tool_delay_s=delay, arrival_s=arrival)
                # Descendant before parent exercises complete blocking propagation.
                work["requests"][1:] = reversed(work["requests"][1:])
                records, elapsed, calls = await controlled(work, {"r0": .2})
                by_id = {r["id"]: r for r in records}
                self.assertEqual([c["id"] for c in calls], ["r0"])
                self.assertEqual(by_id["r1"]["error"], "task_deadline_before_release")
                self.assertNotIn("released_s", by_id["r1"])
                self.assertNotIn("admitted_s", by_id["r1"])
                self.assertAlmostEqual(by_id["r1"]["end_s"], 1)
                self.assertEqual(by_id["r2"]["status"], "blocked")
                self.assertEqual(by_id["r2"]["end_s"], by_id["r1"]["end_s"])
                metrics = summarize(records, elapsed, 1)
                self.assertEqual(metrics["queue_s"]["n"], 1)
                self.assertEqual(metrics["expired_queue_s"]["n"], 0)
                self.assertEqual(outcomes(work, records)[0]["status"], "expired")

    async def test_late_wakeup_does_not_release_a_step_after_its_task_budget(self):
        for delay, late in ((2, 2), (.75, 0)):
            with self.subTest(delay=delay, late=late):
                work = chain(3)
                work["requests"][1]["tool_delay_s"] = delay
                records, elapsed, calls = await controlled(work, {"r0": .25}, wake_late=late)
                by_id = {r["id"]: r for r in records}
                self.assertEqual([call["id"] for call in calls], ["r0"])
                self.assertEqual(by_id["r1"]["error"], "task_deadline_before_release")
                self.assertNotIn("released_s", by_id["r1"])
                self.assertNotIn("admitted_s", by_id["r1"])
                self.assertEqual(by_id["r1"]["end_s"], 1 + late)
                self.assertEqual(by_id["r2"]["status"], "blocked")
                self.assertEqual(summarize(records, elapsed, 1)["expired_queue_s"]["n"], 0)

    async def test_queued_root_nonzero_arrival_expires_and_independent_task_finishes(self):
        work = chain(3)
        hold, root, child = work["requests"]
        root.pop("after")
        root["arrival_s"] = .25
        work["tasks"] = [
            {"id": "held", "request_ids": ["r0"], "deadline_s": 2},
            {"id": "queued", "request_ids": ["r1", "r2"], "deadline_s": .125}]
        loop = asyncio.get_running_loop()
        clock, calls = [1000.0], []
        release = asyncio.Event()
        original_wait, original_sleep = asyncio.wait, asyncio.sleep

        class Backend:
            async def stream(self, request, **options):
                calls.append(request["id"])
                await release.wait()
                return result()

        async def wait(active, *, timeout, return_when):
            await original_sleep(0)
            if timeout is not None:
                clock[0] += timeout
                return set(), set(active)
            clock[0] = 1000.5
            release.set()
            return await original_wait(active, return_when=return_when)

        with patch.object(loop, "time", side_effect=lambda: clock[0]), \
                patch("serving_lab.scheduler.asyncio.wait", side_effect=wait):
            records, _ = await run(work, Backend(), concurrency=1)
        by_id = {r["id"]: r for r in records}
        self.assertEqual(calls, ["r0"])
        self.assertEqual(by_id["r1"]["error"], "task_deadline_before_admission")
        self.assertAlmostEqual(by_id["r1"]["task_deadline_at_s"], .375)
        self.assertAlmostEqual(by_id["r1"]["end_s"], .375)
        self.assertEqual(by_id["r1"]["released_s"], .25)
        self.assertNotIn("admitted_s", by_id["r1"])
        self.assertLess(by_id["r2"]["end_s"], by_id["r0"]["end_s"])
        self.assertEqual([r["status"] for r in outcomes(work, records)], ["ok", "expired"])

    async def test_task_request_budget_order_and_tie(self):
        for task_limit, request_limit, source in ((.3, .8, "task"), (.8, .3, "request"), (.3, .3, "task")):
            with self.subTest(source=source, task=task_limit):
                work = chain(1, task_limit)
                work["requests"][0]["deadline_s"] = request_limit
                records, _, calls = await controlled(work, {"r0": 1})
                self.assertAlmostEqual(calls[0]["deadline"], min(task_limit, request_limit))
                self.assertEqual(records[0]["deadline_source"], source)
                expected = "task_deadline_during_stream" if source == "task" else "deadline_during_stream"
                self.assertEqual(records[0]["error"], expected)
                self.assertEqual(outcomes(work, records)[0]["reason"], expected)

    async def test_observed_late_or_equal_completion_is_not_timely_even_if_requests_ok(self):
        for total in (1, 1.5):
            with self.subTest(total=total):
                work = chain()
                records, _, _ = await controlled(work, {"r0": .75, "r1": total - .75}, honor_deadline=False)
                self.assertEqual([r["status"] for r in records], ["ok", "ok"])
                task = outcomes(work, records)[0]
                self.assertEqual(task["status"], "expired")
                self.assertEqual(task["reason"], "task_deadline_at_completion")
                self.assertEqual(summarize_tasks([task])["e2e_s"]["n"], 0)

    async def test_task_failure_and_missing_records_never_count_success(self):
        work = chain(3)
        records, _, calls = await controlled(work, {"r0": .1}, error="http_429")
        self.assertEqual([r["status"] for r in records], ["failed", "blocked", "blocked"])
        self.assertEqual(len(calls), 1)
        task = outcomes(work, records)[0]
        self.assertEqual((task["status"], task["reason"], task["deciding_request_id"]), ("failed", "http_429", "r0"))
        with self.assertRaisesRegex(ValueError, "every member"):
            outcomes(work, records[:-1])

    async def test_cli_late_task_nonzero_even_when_every_request_ok(self):
        work = chain()
        records, elapsed, _ = await controlled(work, {"r0": .75, "r1": .75}, honor_deadline=False)
        with TemporaryDirectory() as directory:
            source = Path(directory) / "input.json"
            source.write_text(json.dumps(work))
            args = arguments(str(Path(directory) / "output"))
            args.workload, args.repeats = str(source), 1
            backend = type("Backend", (), {"close": AsyncMock()})()
            with patch("serving_lab.cli.OpenAIBackend", return_value=backend), \
                    patch("serving_lab.cli.run", AsyncMock(return_value=(records, elapsed))), redirect_stdout(io.StringIO()):
                code = await experiment(args, "unused", "fake_test")
            report = json.loads((Path(args.output) / "report.json").read_text())
            self.assertEqual(code, 1)
            self.assertEqual(report["runs"][0]["metrics"]["succeeded"], 2)
            self.assertEqual(report["runs"][0]["task_metrics"]["expired"], 1)
            self.assertEqual(report["batch_status"], "completed")

    async def test_counted_http_cancels_task_stream_and_http_timeout_remains_failure(self):
        # One counted local SSE fixture; no performance assertion or model calls.
        async with counted_server({"r0": .2}, usage=True) as (url, received):
            for task_budget, timeout, expected in ((.05, 1, "expired"), (1, .05, "failed")):
                work = chain(2, task_budget)
                backend = OpenAIBackend(url, "fixture", timeout=timeout)
                try:
                    records, _ = await run(work, backend, concurrency=1)
                finally:
                    await backend.close()
                self.assertEqual([r["status"] for r in records], [expected, "blocked"])
                self.assertEqual(outcomes(work, records)[0]["status"], expected)
                if expected == "expired":
                    self.assertEqual(records[0]["error"], "task_deadline_during_stream")
                    self.assertEqual(records[0]["usage"], {"completion_tokens": 1})
            self.assertEqual(received, ["r0", "r0"])

    def test_linear_task_validation(self):
        good = chain(3)
        validate(good)
        validate(generate(count=4))
        bad = []
        for ids in (["r0", "r1", "r1"], ["r0", "missing"], ["r0", "r2", "r1"], ["r1", "r2"], ["r0", "r1"]):
            work = deepcopy(good)
            work["tasks"][0]["request_ids"] = ids
            bad.append(work)
        overlap = deepcopy(good)
        overlap["tasks"].append({"id": "other", "request_ids": ["r0"], "deadline_s": 1})
        bad.append(overlap)
        branch = deepcopy(good)
        branch["requests"][2]["after"] = "r0"
        bad.append(branch)
        external = deepcopy(good)
        external["tasks"][0]["request_ids"] = ["r0"]
        bad.append(external)
        duplicate_id = chain(1)
        duplicate_id["tasks"] *= 2
        bad.append(duplicate_id)
        for budget in (0, -1, float("nan"), float("inf")):
            work = deepcopy(good)
            work["tasks"][0]["deadline_s"] = budget
            bad.append(work)
        for work in bad:
            with self.subTest(work=work), self.assertRaises(ValueError):
                validate(work)
