"""A real HTTP/SSE server with scripted delays, not an LLM."""
import asyncio
import json
from contextlib import asynccontextmanager


@asynccontextmanager
async def serve():
    async def handle(reader, writer):
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            headers = head.decode().split("\r\n")
            size = int(next(line.split(":", 1)[1] for line in headers if line.lower().startswith("content-length:")))
            payload = json.loads(await reader.readexactly(size))
            words = sum(len((m.get("content") or "").split()) for m in payload["messages"])
            if headers[0].split()[1] == "/tokenize":
                body = json.dumps({"count": words}).encode()
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
                await writer.drain()
                return
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
            async def emit(obj):
                data = obj if isinstance(obj, str) else json.dumps(obj)
                writer.write(("data: " + data + "\n\n").encode())
                await writer.drain()
            await emit({"choices": [{"delta": {"role": "assistant"}}]})
            await asyncio.sleep(0.005 + words * 0.00004)
            count = min(payload["max_tokens"], 12)
            for _ in range(count):
                choice = {"delta": {"content": "x"}}
                if payload.get("logprobs"):
                    choice["logprobs"] = {"content": [{"token": "x", "logprob": 0}]}
                await emit({"choices": [choice]})
                await asyncio.sleep(0.002)
            await emit({"choices": [], "usage": {"prompt_tokens": words, "completion_tokens": count}})
            await emit("[DONE]")
        except (ConnectionError, asyncio.IncompleteReadError):
            pass  # Client timeout closes a live scripted stream.
        finally:
            writer.close()
            await writer.wait_closed()
    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    async with server:
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1"
