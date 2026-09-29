"""Deadline behavior against counted, controlled HTTP/SSE requests."""
import asyncio
from contextlib import asynccontextmanager, redirect_stdout
import io
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import json
import unittest
from unittest.mock import patch

from serving_lab import scheduler

from serving_lab.backend import OpenAIBackend
from serving_lab.cli import experiment
from serving_lab.metrics import summarize
from serving_lab.scheduler import run
from serving_lab.workload import generate, validate


@asynccontextmanager
async def counted_server(delays, usage=False):
    received, handlers = [], set()

    async def handle(reader, writer):
        handlers.add(asyncio.current_task())
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode()
            size = int(next(line.split(":", 1)[1] for line in head.split("\r\n")
                            if line.lower().startswith("content-length:")))
            payload = json.loads(await reader.readexactly(size))
            name = payload["messages"][0]["content"]
            received.append(name)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
            event = {"choices": [{"delta": {"content": "x"}}]}
            if usage:
                event["usage"] = {"completion_tokens": 1}
            writer.write(("data: " + json.dumps(event) + "\n\n").encode())
            await writer.drain()
            await asyncio.sleep(delays[name])
            writer.write(b"data: [DONE]\n\n")
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(asyncio.current_task())

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    async with server:
        try:
            yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1", received
        finally:
            for task in list(handlers):
                task.cancel()
            await asyncio.gather(*handlers, return_exceptions=True)


def workload(count):
    work = generate(count=count)
    for r in work["requests"]:
        r.pop("after", None)
        r["messages"] = [{"role": "user", "content": r["id"]}]
    return work


class DeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def measure(self, work, delays, *, timeout=2, usage=False):
        async with counted_server(delays, usage) as (url, received):
            backend = OpenAIBackend(url, "test", timeout=timeout)
            try:
                records, elapsed = await run(work, backend, concurrency=1)
                return {r["id"]: r for r in records}, received, summarize(records, elapsed, 1)
            finally:
                await backend.close()

    async def test_deadline_crossed_during_selection_never_enters_backend(self):
        work = workload(1)
        work["requests"][0]["deadline_s"] = .005
        loop = asyncio.get_running_loop()
        clock = [loop.time()]
        original_choose = scheduler.choose
        called = []

        def select(*args):
            entry = original_choose(*args)
            clock[0] += .01  # Selection crosses the deadline after the ready scan.
            return entry

        class Backend:
            async def stream(self, request, **options):
                called.append(request["id"])
                return {"error": None}

        with patch.object(loop, "time", side_effect=lambda: clock[0]), \
                patch.object(scheduler, "choose", side_effect=select):
            records, elapsed = await run(work, Backend(), concurrency=1)
        self.assertEqual(called, [])
        self.assertEqual(records[0]["error"], "deadline_before_admission")
        self.assertNotIn("admitted_s", records[0])
        metrics = summarize(records, elapsed, 1)
        self.assertEqual(metrics["queue_s"]["n"], 0)
        self.assertEqual(metrics["expired_queue_s"]["n"], 1)

    async def test_reverse_descendants_block_before_waiting_for_unrelated_stream(self):
        work = workload(4)
        r0, r1, r2, r3 = work["requests"]
        r1["deadline_s"] = .005
        r2["after"], r3["after"] = "r1", "r2"
        work["requests"] = [r0, r1, r3, r2]
        loop = asyncio.get_running_loop()
        clock = [loop.time()]
        original_choose, original_wait = scheduler.choose, asyncio.wait
        release = asyncio.Event()
        waits, called = [], []

        def select(*args):
            entry = original_choose(*args)
            clock[0] += .01  # r1 expires while the slot is assigned to r0.
            return entry

        async def wait(*args, **kwargs):
            waits.append(True)
            if len(waits) == 2:
                # End the unrelated stream only after the scheduler has had its
                # queue-expiry wakeup and next opportunity to propagate blocks.
                clock[0] += .02
                release.set()
            return await original_wait(*args, **kwargs)

        class Backend:
            async def stream(self, request, **options):
                called.append(request["id"])
                await release.wait()
                return {"error": None}

        with patch.object(loop, "time", side_effect=lambda: clock[0]), \
                patch.object(scheduler, "choose", side_effect=select), \
                patch.object(asyncio, "wait", side_effect=wait):
            records, _ = await run(work, Backend(), concurrency=1)
        by_id = {r["id"]: r for r in records}
        self.assertEqual(called, ["r0"])
        self.assertEqual(by_id["r1"]["error"], "deadline_before_admission")
        for child in ("r2", "r3"):
            self.assertEqual(by_id[child]["status"], "blocked")
            self.assertEqual(by_id[child]["end_s"], by_id["r1"]["end_s"])
            self.assertLess(by_id[child]["end_s"], by_id["r0"]["end_s"])

    async def test_queue_expiry_wakes_before_occupied_slot_finishes(self):
        work = workload(3)
        work["requests"][1].update(arrival_s=.04, deadline_s=.1)
        work["requests"][2]["after"] = "r1"
        records, received, metrics = await self.measure(work, {"r0": .45})
        self.assertEqual(received, ["r0"])
        expired = records["r1"]
        self.assertEqual(expired["status"], "expired")
        self.assertEqual(expired["error"], "deadline_before_admission")
        self.assertNotIn("admitted_s", expired)
        self.assertGreaterEqual(expired["end_s"], .14)
        self.assertLess(expired["end_s"], records["r0"]["end_s"] - .15)
        self.assertEqual(records["r2"]["status"], "blocked")
        self.assertLess(records["r2"]["end_s"], records["r0"]["end_s"])
        self.assertEqual(metrics["expired"], 1)
        self.assertEqual(metrics["expired_queue_s"]["n"], 1)
        self.assertEqual(metrics["queue_s"]["n"], 1)
        self.assertEqual(metrics["expiration_rate"], 1 / 3)
        self.assertIsNone(metrics["output_tokens_per_s"])

    async def test_stream_gets_only_remaining_budget_and_keeps_telemetry(self):
        work = workload(3)
        work["requests"][1]["deadline_s"] = .4
        work["requests"][2]["after"] = "r1"
        records, received, metrics = await self.measure(work, {"r0": .2, "r1": .6}, usage=True)
        self.assertEqual(received, ["r0", "r1"])
        expired = records["r1"]
        self.assertEqual(expired["status"], "expired")
        self.assertEqual(expired["error"], "deadline_during_stream")
        self.assertGreaterEqual(expired["end_s"], .4)
        self.assertLess(expired["end_s"], .55)
        self.assertLess(expired["duration_s"], .3)
        self.assertEqual(len(expired["chunk_times_s"]), 1)
        self.assertEqual(expired["usage"], {"completion_tokens": 1})
        self.assertEqual(records["r2"]["status"], "blocked")
        self.assertEqual(metrics["queue_s"]["n"], 2)
        self.assertEqual(metrics["e2e_s"]["n"], 1)
        self.assertEqual(metrics["ttft_s"]["n"], 1)
        self.assertEqual(metrics["failed"], 0)

    async def test_http_timeout_before_deadline_stays_failed(self):
        work = workload(1)
        work["requests"][0]["deadline_s"] = 1
        records, received, metrics = await self.measure(work, {"r0": .5}, timeout=.1)
        self.assertEqual(received, ["r0"])
        self.assertEqual(records["r0"]["status"], "failed")
        self.assertIn(records["r0"]["error"], ("TimeoutError", "ReadTimeout"))
        self.assertEqual(metrics["failed"], 1)
        self.assertEqual(metrics["expired"], 0)

    async def test_dependency_budget_starts_at_eligible_release(self):
        work = workload(2)
        work["requests"][1].update(after="r0", tool_delay_s=.03, deadline_s=.12)
        records, received, _ = await self.measure(work, {"r0": .25, "r1": .01})
        self.assertEqual(received, ["r0", "r1"])
        self.assertEqual([r["status"] for r in records.values()], ["ok", "ok"])
        self.assertGreaterEqual(records["r1"]["released_s"], records["r0"]["end_s"] + .03)

    async def test_no_deadline_keeps_all_requests_and_missing_usage(self):
        records, received, metrics = await self.measure(workload(2), {"r0": .15, "r1": .01})
        self.assertEqual(received, ["r0", "r1"])
        self.assertEqual([r["status"] for r in records.values()], ["ok", "ok"])
        self.assertGreaterEqual(records["r1"]["admitted_s"], records["r0"]["end_s"])
        self.assertEqual(metrics["expired"], 0)
        self.assertEqual(metrics["usage_coverage"], 0)
        self.assertIsNone(metrics["output_tokens_per_s"])

    async def test_caller_cancellation_cleans_active_requests(self):
        started, stopped = asyncio.Event(), asyncio.Event()

        class Backend:
            async def stream(self, request):
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    stopped.set()

        task = asyncio.create_task(run(workload(1), Backend()))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(stopped.is_set())

    async def test_cli_expired_only_run_is_nonzero(self):
        with TemporaryDirectory() as directory:
            work = workload(1)
            work["requests"][0]["deadline_s"] = 1e-9
            path = Path(directory) / "workload.json"
            path.write_text(json.dumps(work))
            args = SimpleNamespace(workload=str(path), model="test", api_key_env="SERVING_LAB_TEST_UNUSED",
                                   timeout=2, logprobs=False, tokenize=False, concurrency=1, aging=.2,
                                   repeats=1, seed=42, policy="fcfs", starvation=1, output=directory)
            async with counted_server({}) as (url, received):
                with redirect_stdout(io.StringIO()):
                    result = await experiment(args, url, "scripted_http_not_llm")
            self.assertEqual(result, 1)
            self.assertEqual(received, [])
            report = json.loads((Path(directory) / "report.json").read_text())
            metrics = report["runs"][0]["metrics"]
            self.assertEqual((metrics["succeeded"], metrics["failed"], metrics["expired"], metrics["blocked"]),
                             (0, 0, 1, 0))
            self.assertIsNone(metrics["output_tokens_per_s"])

    def test_deadline_must_be_positive_and_finite(self):
        for value in (0, -1, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                work = workload(1)
                work["requests"][0]["deadline_s"] = value
                validate(work)
