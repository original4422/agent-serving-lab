# Agent Serving Lab

Compare **client admission policies** for agent-shaped LLM workloads against an OpenAI-compatible streaming server. Replay short requests, long contexts, bursts and tool follow-ups; inspect queueing, tail latency, output throughput and long-waiting requests in JSON and Markdown.

The scheduler decides when an HTTP request is admitted. The inference server still controls continuous batching, prefill, decoding and KV cache. This project does not modify vLLM or implement an inference engine.

## Quick start

Python 3.11+ and [uv](https://docs.astral.sh/uv/) are required.

```sh
git clone https://github.com/original4422/agent-serving-lab.git
cd agent-serving-lab
uv sync --locked
uv run agent-serving-lab demo --logprobs --repeats 2
```

This starts a local HTTP/SSE server, runs all three policies, and writes `results/latest/{workload.json,report.json,report.md}`. **The demo is scripted, not an LLM benchmark.** Its timings verify the client, scheduler and reports; they are not evidence of model speed or policy improvements.

Run tests:

```sh
uv run python -m unittest discover -s tests -v
```

## Connect an existing model server

With a model already served by vLLM or another compatible server:

```sh
uv run agent-serving-lab run \
  --base-url http://localhost:8000/v1 --model YOUR_SERVED_MODEL \
  --tokenize --concurrency 4 --repeats 3 --output results/model-run
```

`--tokenize` calls vLLM's `/tokenize` extension **before** measurement to count the exact chat messages, including the server's chat template. Omit it for servers without that extension. `--logprobs` requests token-level logprobs when the model/server supports them. Neither feature silently falls back after a rejected request.

For authentication, set `SERVING_LAB_API_KEY` in the process environment, or select a different variable with `--api-key-env`. The client does not use ambient HTTP proxies. Keys, endpoint URLs and generated model text are not written to reports. Workload files include prompts; results are gitignored.

`--timeout` is the total deadline of each admitted HTTP request. A failed stream is not retried; its dependent requests are marked blocked. The CLI exits nonzero when any request fails or is blocked.

External runs are labeled `external_backend_unverified`: the tool cannot determine whether an endpoint runs a real model. When sharing a real experiment, record model revision, server version/arguments, hardware, tokenizer, cache/warm-up conditions and competing traffic alongside the output.

## Workload and policies

The default seed is 42 with 24 requests. Every eight requests arrive as one burst, with bursts 120 ms apart. Short/tool/burst prompt bodies use 16–48 repeated words; long contexts use 512–1024. These are **input token estimates**, labeled `word_estimate`, until `/tokenize` replaces them. No tokenizer-independent exact token count is claimed. `workload.json` freezes prompts, arrival times, generation limits and dependencies; reports include its SHA-256 and count/arrival distributions.

Each tool follow-up waits for its parent to finish plus a 30 ms tool delay. It replays a fixed assistant/tool transcript; it does not run a live tool or copy the previous model response. Its release time therefore changes with parent completion. This models dependency timing while holding input content fixed across policies.

| Policy | Admission order |
|---|---|
| `fcfs` | Earliest release, then workload order |
| `shortest-input` | Smallest known input token count/estimate, then FIFO |
| `aging` | Requests waiting at least `--aging` seconds go first in FIFO order; remaining requests use shortest input |

All policies are non-preemptive with the same `--concurrency` cap. They never use actual future output lengths. Aging stops new short requests from overtaking an already overdue request; occupied slots still have to complete or time out. A finite run's long wait is reported as a threshold violation, not proof of infinite starvation.

Replay a saved workload or select one policy:

```sh
uv run agent-serving-lab demo --workload results/latest/workload.json \
  --policy aging --aging 0.15 --starvation 0.5 --output results/replay
```

Custom workload JSON uses the same schema as the saved file. Each request has a unique `id`, `kind`, `arrival_s`, `messages`, positive `input_tokens`, `input_tokens_source`, and `max_tokens`; optional `after` and `tool_delay_s` describe a dependency. Cycles and missing parents are rejected.

`--repeats` randomizes policy order using `--seed` and reports each repetition separately. A single noisy run does not establish a winner. Server caches are not reset; prepare comparable server conditions when measuring policy effects.

## Reading the measurements

Times use a monotonic clock. p50/p95 use nearest-rank quantiles. Metrics are available overall and by workload kind.

| Metric | Definition |
|---|---|
| Queue | Admission minus eligible release; includes client scheduling delay |
| TTFT | First nonempty content/reasoning/tool delta minus release; includes queue/network/server time |
| Dispatch to first chunk | First nonempty delta minus HTTP call start |
| End to end | Completed stream minus eligible release |
| Inter-chunk interval | Time between received nonempty SSE deltas |
| ITL | Received token intervals **only** for streams where logprobs confirm exactly one token in every content chunk; otherwise unavailable |
| Output tokens/s | Successful requests' server-reported completion tokens divided by full run duration; unavailable if any successful request lacks usage |
| Requests/s | Successful requests divided by full run duration, including scheduled idle time |
| Queue threshold exceeded | Admitted requests waiting at least `--starvation` seconds; failures and blocked dependents are reported separately |

TTFT is a client-observed first-output approximation; ordinary streams do not expose the server's first internal token timestamp. Multi-token chunks do not provide individual token timestamps. Queue statistics include admitted failures; TTFT/E2E/interval distributions include successful requests only. Raw per-request records retain partial timing on failure. The run duration begins at workload time zero and ends after all requests reach a terminal state. Per-kind throughput uses that same overall duration.

## Verified status

- Local scripted HTTP/SSE integration, chunked transfer and fragmented UTF-8.
- Deterministic policy ordering, aging promotion, bounded concurrency and dependency failure propagation.
- Deadline and truncated-stream failures, token telemetry gaps, exact metric arithmetic.
- No GPU-backed model performance measurement has been completed. The implementation can target an existing compatible endpoint without installing an inference runtime.

## References and license

Original implementation, MIT; runtime dependency [HTTPX](https://github.com/encode/httpx) is BSD-3-Clause (its dependencies retain their own licenses). The dependency lockfile is committed.

Design references: [vLLM OpenAI-compatible serving](https://docs.vllm.ai/en/latest/serving/openai_compatible_server/) (Apache-2.0 project), [Nano-vLLM](https://github.com/GeeeekExplorer/nano-vllm) (MIT). No source from either engine is vendored. Nano-vLLM is an offline engine reference, not a supported HTTP backend by itself.
