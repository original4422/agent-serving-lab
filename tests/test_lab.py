import asyncio
import unittest
from serving_lab.backend import OpenAIBackend
from serving_lab.metrics import markdown, summarize
from serving_lab.mock_server import serve
from serving_lab.scheduler import choose, run
from serving_lab.workload import admission_profile, generate, validate


class SchedulingTests(unittest.TestCase):
    def test_admission_profiles_freeze_arrivals_and_generation_limits(self):
        for name in ("mixed-burst", "short-stream"):
            work = admission_profile(name)
            validate(work)
            self.assertEqual(work, admission_profile(name))
            self.assertNotEqual(work, admission_profile(name, seed=7))
            self.assertEqual(len(work["requests"]), 16)
            self.assertEqual({r["max_tokens"] for r in work["requests"]}, {64})
            self.assertEqual({r["input_tokens_source"] for r in work["requests"]}, {"word_estimate"})
        burst = admission_profile("mixed-burst")["requests"]
        self.assertEqual(sum(r["kind"] == "long" for r in burst), 4)
        self.assertEqual({r["arrival_s"] for r in burst}, {0})
        stream = admission_profile("short-stream")["requests"]
        self.assertEqual([r["kind"] for r in stream[:4]], ["short", "short", "long", "long"])
        self.assertEqual([r["arrival_s"] for r in stream[:5]], [0, 0, .05, .05, .12])
        self.assertEqual(stream[-1]["arrival_s"], 1.44)
        with self.assertRaises(ValueError):
            admission_profile("short-stream", count=4)

    def test_seed_and_validation(self):
        self.assertEqual(generate(), generate())
        self.assertNotEqual(generate(1), generate(2))
        bad = generate(count=2)
        bad["requests"][0]["after"] = "r1"
        bad["requests"][1]["after"] = "r0"
        with self.assertRaises(ValueError):
            validate(bad)

    def test_policy_and_aging(self):
        long = {"request": {"input_tokens": 1000}, "index": 0, "released_s": 0}
        short = {"request": {"input_tokens": 10}, "index": 1, "released_s": 0.9}
        self.assertIs(choose([long, short], 1, "fcfs", .5), long)
        self.assertIs(choose([long, short], 1, "shortest-input", .5), short)
        self.assertIs(choose([long, short], 1, "aging", .5), long)
        # Unknown actual output lengths are never used by any policy.
        short["request"]["output_tokens"] = 100000
        self.assertIs(choose([long, short], 1, "shortest-input", .5), short)

    def test_metrics_exact_values_and_missing_usage(self):
        r = {"status": "ok", "released_s": 1, "admitted_s": 2, "end_s": 5,
             "chunk_times_s": [.5, 1., 2.], "chunk_token_counts": [1, 1, 1],
             "usage": {"completion_tokens": 3}}
        m = summarize([r], 6, .9)
        self.assertEqual(m["queue_s"]["p95"], 1)
        self.assertEqual(m["ttft_s"]["p95"], 1.5)
        self.assertEqual(m["e2e_s"]["p95"], 4)
        self.assertEqual(m["e2e_s"]["mean"], 4)
        self.assertEqual(m["failure_rate"], 0)
        self.assertEqual(m["itl_s"]["p95"], 1)
        self.assertEqual(m["output_tokens_per_s"], .5)
        self.assertEqual(m["queue_threshold_exceeded"], 1)
        r["usage"] = {}
        r["chunk_token_counts"] = [None, None, None]
        m = summarize([r], 6, .9)
        self.assertIsNone(m["output_tokens_per_s"])
        self.assertEqual(m["itl_s"]["n"], 0)
        self.assertEqual(m["inter_chunk_s"]["n"], 2)
        report = markdown({"evidence": "test", "runs": [{"policy": "fcfs", "repeat": 1,
                            "metrics": m, "by_kind": {"long": m}}]})
        self.assertIn("| 2 | fcfs | long | 1 / 0 / 0 |", report)
        failed = {**r, "status": "failed"}
        self.assertEqual(summarize([r, failed], 6, 1)["failure_rate"], .5)


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_http_sse_tokenize_and_dependency(self):
        async with serve() as url:
            backend = OpenAIBackend(url, "test", logprobs=True)
            try:
                workload = generate(count=4)
                data = await backend.tokenize(workload["requests"][0]["messages"])
                self.assertGreater(data["count"], 0)
                records, elapsed = await run(workload, backend, "aging", concurrency=2)
                by_id = {r["id"]: r for r in records}
                self.assertTrue(all(r["status"] == "ok" for r in records))
                self.assertGreaterEqual(by_id["r2"]["released_s"], by_id["r1"]["end_s"] + .03)
                self.assertEqual(len(by_id["r0"]["chunk_times_s"]), 12)
                self.assertGreater(summarize(records, elapsed, 1)["itl_s"]["n"], 0)
            finally:
                await backend.close()

    async def test_capacity_and_failure_blocks_descendants(self):
        class Backend:
            active = 0
            peak = 0
            async def stream(self, req):
                self.active += 1
                self.peak = max(self.peak, self.active)
                await asyncio.sleep(.002)
                self.active -= 1
                return {"error": "injected" if req["id"] == "r1" else None,
                        "chunk_times_s": [], "chunk_token_counts": [], "usage": {}, "duration_s": .002}
        backend = Backend()
        records, _ = await run(generate(count=8), backend, concurrency=2)
        self.assertLessEqual(backend.peak, 2)
        self.assertEqual(next(r for r in records if r["id"] == "r2")["status"], "blocked")

    async def test_deadline_and_truncated_stream(self):
        async def handler(reader, writer):
            await reader.read(65536)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\ndata: {\"choices\": []}\n\n")
            await writer.drain()
            await asyncio.sleep(.1)
            writer.close()
            await writer.wait_closed()
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        async with server:
            url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1"
            for timeout in (.02, 1):
                backend = OpenAIBackend(url, "test", timeout=timeout)
                try:
                    result = await backend.stream(generate(count=1)["requests"][0])
                    self.assertIsNotNone(result["error"])
                    self.assertLess(result["duration_s"], .5)
                finally:
                    await backend.close()

    async def test_reverse_order_failed_dependency_chain(self):
        class Failed:
            async def stream(self, req):
                return {"error": "injected"}
        work = generate(count=3)
        work["requests"][1]["after"] = "r0"
        work["requests"][2]["after"] = "r1"
        work["requests"].reverse()
        records, _ = await asyncio.wait_for(run(work, Failed()), 1)
        self.assertEqual([r["status"] for r in records], ["failed", "blocked", "blocked"])

    async def test_chunked_fragmented_unicode(self):
        async def handler(reader, writer):
            await reader.read(65536)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n")
            body = ('data: {"choices": [{"delta": {"content": "中"}}]}\n\n'
                    'data: {"choices": [], "usage": {"completion_tokens": 1}}\n\n'
                    'data: [DONE]\n\n').encode()
            for byte in body:
                writer.write(b"1\r\n" + bytes([byte]) + b"\r\n")
            writer.write(b"0\r\n\r\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        async with server:
            backend = OpenAIBackend(f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1", "test")
            try:
                result = await backend.stream(generate(count=1)["requests"][0])
                self.assertIsNone(result["error"])
                self.assertEqual(len(result["chunk_times_s"]), 1)
                self.assertEqual(result["usage"]["completion_tokens"], 1)
            finally:
                await backend.close()

    async def test_http_failure_does_not_persist_response_or_key(self):
        async def handler(reader, writer):
            await reader.read(65536)
            body = b"private server detail"
            writer.write(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        async with server:
            backend = OpenAIBackend(f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1", "test", api_key="test-key")
            try:
                result = await backend.stream(generate(count=1)["requests"][0])
                self.assertEqual(result["error"], "http_429")
                self.assertNotIn("private", str(result))
                self.assertNotIn("test-key", str(result))
            finally:
                await backend.close()
