"""Count held HTTP streams; no throughput or latency comparisons."""
import asyncio
from contextlib import asynccontextmanager, redirect_stdout
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from serving_lab.backend import OpenAIBackend
from serving_lab.cli import experiment
from serving_lab.scheduler import run
from serving_lab.workload import generate
from test_reporting import arguments


def independent_requests(count):
    workload = generate(count=count)
    for request in workload["requests"]:
        request.pop("after", None)
        request.update(arrival_s=0, tool_delay_s=0, input_tokens=1,
                       messages=[{"role": "user", "content": request["id"]}])
    return workload


@asynccontextmanager
async def held_server():
    class State:
        def __init__(self):
            self.received = []
            self.active = self.peak = self.connections = 0
            self.release = asyncio.Event()
            self.changed = asyncio.Condition()

        async def until(self, condition):
            async with self.changed:
                await self.changed.wait_for(condition)

        async def signal(self):
            async with self.changed:
                self.changed.notify_all()

    state = State()
    handlers = set()

    async def handle(reader, writer):
        handlers.add(asyncio.current_task())
        state.connections += 1
        try:
            while True:
                headers = await reader.readuntil(b"\r\n\r\n")
                size = int(next(line.split(b":", 1)[1] for line in headers.split(b"\r\n")
                                if line.lower().startswith(b"content-length:")))
                payload = json.loads(await reader.readexactly(size))
                if headers.startswith(b"POST /tokenize "):
                    body = b'{"count":1}'
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                                 + str(len(body)).encode() + b"\r\n\r\n" + body)
                    await writer.drain()
                    continue
                state.received.append(payload["messages"][0]["content"])
                state.active += 1
                state.peak = max(state.peak, state.active)
                await state.signal()
                waiters = []
                try:
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                                 b"Transfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n")

                    def event(data):
                        body = b"data: " + data + b"\n\n"
                        writer.write(f"{len(body):x}\r\n".encode() + body + b"\r\n")

                    event(b'{"choices":[{"delta":{"content":"x"}}]}')
                    await writer.drain()
                    released = asyncio.create_task(state.release.wait())
                    disconnected = asyncio.create_task(reader.read(1))
                    waiters = [released, disconnected]
                    done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
                    if disconnected in done:
                        return  # The client closed a still-held stream.
                    event(b'{"choices":[],"usage":{"completion_tokens":1}}')
                    event(b"[DONE]")
                    writer.write(b"0\r\n\r\n")
                    await writer.drain()
                finally:
                    for task in waiters:
                        task.cancel()
                    await asyncio.gather(*waiters, return_exceptions=True)
                    state.active -= 1
                    await state.signal()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(asyncio.current_task())

    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=256)
    async with server:
        try:
            yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1", state
        finally:
            state.release.set()
            for task in list(handlers):
                task.cancel()
            await asyncio.gather(*handlers, return_exceptions=True)


class ConnectionCapacityTests(unittest.IsolatedAsyncioTestCase):
    async def test_cli_capacity_128_reaches_server_before_any_stream_is_released(self):
        with TemporaryDirectory() as directory:
            source = Path(directory) / "input.json"
            source.write_text(json.dumps(independent_requests(128)))
            args = arguments(str(Path(directory) / "output"))
            args.workload, args.concurrency, args.repeats = str(source), 128, 1
            with redirect_stdout(io.StringIO()):
                async with held_server() as (url, state):
                    task = asyncio.create_task(experiment(args, url, "scripted_http_not_llm"))
                    try:
                        try:
                            # The timeout bounds a deadlock; arrival count is the assertion.
                            await asyncio.wait_for(state.until(lambda: len(state.received) == 128), 10)
                        except TimeoutError:
                            self.fail(f"Only {len(state.received)} of 128 admitted requests reached the held server")
                        self.assertFalse(state.release.is_set())
                        self.assertEqual(state.active, 128)
                        state.release.set()
                        code = await asyncio.wait_for(task, 10)
                    finally:
                        if not task.done():
                            task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
            report = json.loads((Path(args.output) / "report.json").read_text())
            self.assertEqual(code, 0)
            self.assertEqual(report["runs"][0]["metrics"]["succeeded"], 128)
            self.assertEqual(sum("admitted_s" in r for r in report["runs"][0]["requests"]), 128)

    async def test_capacity_two_cancels_then_serves_followup_and_reuses_tokenize_connection(self):
        async with held_server() as (url, state):
            backend = OpenAIBackend(url, "fixture", timeout=60, max_connections=2)
            task = asyncio.create_task(run(independent_requests(3), backend, concurrency=2))
            try:
                await asyncio.wait_for(state.until(lambda: len(state.received) == 2), 10)
                self.assertEqual(state.active, 2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                await asyncio.wait_for(state.until(lambda: state.active == 0), 10)
                self.assertFalse(state.release.is_set())
                self.assertEqual(len(state.received), 2)
                self.assertEqual(state.peak, 2)
                state.release.set()
                first, _ = await asyncio.wait_for(run(independent_requests(1), backend, concurrency=2), 10)
                self.assertEqual(first[0]["status"], "ok")
                messages = [{"role": "user", "content": "fictional tokenization"}]
                self.assertEqual(await backend.tokenize(messages), {"count": 1})
                connections = state.connections
                self.assertEqual(await backend.tokenize(messages), {"count": 1})
                self.assertEqual(state.connections, connections)  # Fully consumed JSON responses reuse keep-alive.
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await backend.close()
