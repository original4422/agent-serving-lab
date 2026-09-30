"""OpenAI-compatible chat SSE transport; credentials never enter reports."""
import asyncio
import json
import time
import httpx


class OpenAIBackend:
    def __init__(self, base_url, model, api_key=None, timeout=60, logprobs=False, *, max_connections=100):
        self.base_url = base_url.rstrip("/")
        self.model, self.timeout, self.logprobs = model, timeout, logprobs
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        limits = httpx.Limits(max_connections=max_connections, max_keepalive_connections=20)
        self.client = httpx.AsyncClient(headers=headers, timeout=timeout, trust_env=False, limits=limits)

    async def close(self):
        await self.client.aclose()

    async def tokenize(self, messages):
        url = self.base_url.removesuffix("/v1") + "/tokenize"
        response = await self.client.post(url, json={"model": self.model, "messages": messages})
        response.raise_for_status()
        return response.json()

    async def stream(self, request, *, deadline_at=None):
        start = time.perf_counter()
        events, token_counts, usage = [], [], {}
        payload = {"model": self.model, "messages": request["messages"],
                   "max_tokens": request["max_tokens"], "temperature": 0,
                   "stream": True, "stream_options": {"include_usage": True}}
        if self.logprobs:
            payload["logprobs"] = True
        deadline = asyncio.timeout_at(deadline_at)
        try:
            async with deadline, asyncio.timeout(self.timeout):
                async with self.client.stream("POST", self.base_url + "/chat/completions", json=payload) as response:
                    response.raise_for_status()
                    data_lines = []
                    done = False
                    async for line in response.aiter_lines():
                        if line.startswith("data:"):
                            data_lines.append(line[5:].lstrip())
                        elif not line and data_lines:
                            data = "\n".join(data_lines)
                            data_lines = []
                            if data == "[DONE]":
                                done = True
                                break
                            obj = json.loads(data)
                            if obj.get("error"):
                                raise ValueError("stream_error")
                            if obj.get("usage"):
                                usage = obj["usage"]
                            choices = obj.get("choices", [])
                            if choices:
                                choice = choices[0]
                                delta = choice.get("delta", {})
                                if delta.get("content") or delta.get("reasoning_content") or delta.get("tool_calls"):
                                    events.append(time.perf_counter() - start)
                                    logs = (choice.get("logprobs") or {}).get("content")
                                    token_counts.append(len(logs) if logs and not delta.get("tool_calls") and not delta.get("reasoning_content") else None)
                    if not done:
                        raise ValueError("incomplete_stream")
            error = None
        except (httpx.HTTPError, TimeoutError, ValueError, KeyError, TypeError) as exc:
            # Do not persist server bodies, URLs or arbitrary exception messages.
            error = "deadline_during_stream" if deadline.expired() else (
                f"http_{exc.response.status_code}" if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__)
        return {"duration_s": time.perf_counter() - start, "chunk_times_s": events,
                "chunk_token_counts": token_counts, "usage": usage, "error": error}
