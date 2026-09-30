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

This starts a local HTTP/SSE server, runs all three policies, and creates a unique `results/run-<UTC timestamp>-<suffix>/` directory containing `workload.json`, `report.json` and `report.md`. The CLI prints its absolute path before running requests. **The demo is scripted, not an LLM benchmark.** Its timings verify the client, scheduler and reports; they are not evidence of model speed or policy improvements.

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

`--concurrency` caps admitted HTTP operations. The CLI sets its HTTP connection capacity to the same value, so admission above 100 is no longer capped by a second transport queue. The existing limit of 20 idle keep-alive connections remains; request/task deadlines and HTTP timeout keep their meanings. Python callers constructing `OpenAIBackend` directly can pass `max_connections=concurrency`; omitting it retains the previous 100-connection constructor default.

`--timeout` is the total deadline of each admitted HTTP request. A failed stream is not retried; its dependent requests are marked blocked. Optional workload `deadline_s` also budgets queueing (see below). The CLI exits nonzero when any request fails, expires or is blocked.

External runs are labeled `external_backend_unverified`: the tool cannot determine whether an endpoint runs a real model. When sharing a real experiment, record model revision, server version/arguments, hardware, tokenizer, cache/warm-up conditions and competing traffic alongside the output.

## Run a real model on Apple silicon

The [M4/MLX experiment](experiments/mlx-m4-2026-09-30/README.md) includes pinned setup commands, a warm-up helper, the frozen workload and per-request measurements. It uses Qwen3-0.6B 4-bit weights (320 MiB), MLX-LM 0.31.3 and a separate Python environment; the main CLI keeps its HTTPX-only dependency.

On an Apple M4 with 24 GiB RAM, all **72 requests succeeded** across three admission policies and two repetitions. E2E p95 was **2.88 / 2.91 s** for FCFS, **2.67 / 2.18 s** for shortest input and **2.80 / 2.82 s** for aging. These measurements cover a 12-request synthetic workload with a 12-token generation limit. Shortest input finished this workload sooner in both repetitions; the varying difference calls for broader workloads before choosing a default policy.

This is real local Metal inference. The scripted quick-start demo remains a transport and scheduler check. The experiment records server-reported token usage and zero cached prompt tokens; ITL is unavailable because the streams lack token-level logprobs.

The [longer-generation follow-up](experiments/mlx-admission-2026-09-30/README.md) adds **192 successful requests** across a mixed burst and a finite short-request stream, with 64-token generations. Shortest-input improved short-request and overall mean E2E in all four workload/repetition comparisons, while increasing mean long-request waiting. On the short stream, aging reduced long-request mean queue time from **4.78 / 5.42 s to 1.30 / 1.31 s**, at the cost of higher short-request latency. Shortest-input worsened overall p95 in both stream repetitions. The report publishes the frozen protocol, per-class metrics and admission order, making that tradeoff visible.

## Workload and policies

The default seed is 42 with 24 requests. Every eight requests arrive as one burst, with bursts 120 ms apart. Short/tool/burst prompt bodies use 16–48 repeated words; long contexts use 512–1024. These are **input token estimates**, labeled `word_estimate`, until `/tokenize` replaces them. No tokenizer-independent exact token count is claimed. `workload.json` freezes prompts, arrival times, generation limits and dependencies; reports include its SHA-256 and count/arrival distributions.

Each tool follow-up waits for its parent to finish plus a 30 ms tool delay. It replays a fixed assistant/tool transcript; it does not run a live tool or copy the previous model response. Its release time therefore changes with parent completion. This models dependency timing while holding input content fixed across policies.

| Policy | Admission order |
|---|---|
| `fcfs` | Earliest release, then workload order |
| `shortest-input` | Smallest known input token count/estimate, then FIFO |
| `aging` | Requests waiting at least `--aging` seconds go first in FIFO order; remaining requests use shortest input |
| `earliest-deadline` (opt-in) | Earliest effective task/request deadline among ready requests, then release time and workload order; requests without deadlines go last |

`--policy all` runs the original three-policy baseline: FCFS, shortest-input and aging. Its seeded order and number of runs are unchanged. Select `--policy earliest-deadline` explicitly to run the deadline strategy alone. With no declared deadlines it follows FCFS.

All policies are non-preemptive with the same `--concurrency` cap. They never use actual future output lengths. Aging stops new short requests from overtaking an already overdue request; occupied slots still have to complete or time out. A finite run's long wait is reported as a threshold violation, not proof of infinite starvation.

Deadline admission uses the same effective budget as expiration and streaming: the earlier of the task's fixed deadline and the request's release-relative deadline. Equal task/request deadlines retain task attribution. Only eligible requests compete; future arrivals, unfinished parents and tool waits stay outside the ready queue. A newly urgent request does not preempt an active stream. Every task stage competes separately, without reserving its next slot or predicting future service time.

Compare the baseline and opt-in policy over one saved workload by selecting separate output directories:

```sh
uv run agent-serving-lab demo --workload examples/task-deadlines.json \
  --policy all --concurrency 2 --output results/task-baseline
uv run agent-serving-lab demo --workload examples/task-deadlines.json \
  --policy earliest-deadline --concurrency 2 --output results/task-earliest
```

That functional example includes an intentional task expiry, so each command exits 1 after saving its report. For a model endpoint, the same policy option is available with `run`.

Two additional generated profiles expose waiting tradeoffs: `mixed-burst` puts a long input at every fourth position in one arrival burst; `short-stream` puts two long inputs behind two initial short requests, then releases a finite stream of short inputs every 120 ms. Both use a 64-token output cap and require at least eight requests. Their input sizes remain word estimates.

```sh
uv run agent-serving-lab demo --profile short-stream --count 16 \
  --concurrency 2 --aging 1 --repeats 2
```

Replay a saved workload or select one policy:

```sh
uv run agent-serving-lab demo --workload "results/run-<UTC timestamp>-<suffix>/workload.json" \
  --policy aging --aging 0.15 --starvation 0.5 --output results/replay
```

### Budget queueing and streaming together

A request may include `"deadline_s": 0.5` for a 500 ms total budget from its **eligible release**. For a dependent request, that clock starts after its parent completes and the tool delay elapses, or at `arrival_s`, whichever is later. Omitting the field keeps the original behavior. Values must be positive and finite.

- A request still queued at its deadline becomes `expired` with `error: deadline_before_admission`. The scheduler wakes at the deadline even when all slots are occupied, records the observed expiration time, and never sends that request to the server. It has no `admitted_s`. Deadlines are checked again immediately before admission, including when selecting a large batch.
- An admitted request gets only the remaining budget. Expiration closes the local stream and records `error: deadline_during_stream`, preserving received chunks and usage. This does not guarantee that the server stops inference.
- `--timeout` remains an independent HTTP budget measured from admission. Whichever limit expires first wins: HTTP timeout is `failed`, workload deadline is `expired`. Neither is retried; their dependents are `blocked`. Blocking propagates through the full dependency chain before waiting for unrelated streams, regardless of request order.
- Cancelling the whole run propagates cancellation and closes its active client streams; it does not fabricate completed request records.

Try the checked-in scripted example. FCFS admits `hold-slot`, expires `queued` before admission, blocks `dependent`, and completes `after-success` within a budget starting at its own release. The command deliberately exits **1** because a request expires:

```sh
uv run agent-serving-lab demo --workload examples/deadlines.json \
  --policy fcfs --concurrency 1 --output results/deadlines
```

Custom workload JSON uses the same schema as the saved file. Each request has a unique `id`, `kind`, `arrival_s`, `messages`, positive `input_tokens`, `input_tokens_source`, and `max_tokens`; optional `after` and `tool_delay_s` describe a dependency; optional `deadline_s` sets its release-relative budget. Cycles and missing parents are rejected.

`--repeats` randomizes policy order using `--seed` and reports each repetition separately. A single noisy run does not establish a winner. Server caches are not reset; prepare comparable server conditions when measuring policy effects.

### Share one budget across a whole task

Optional `tasks` declare complete, non-overlapping linear chains. Each task has a unique `id`, ordered `request_ids` and positive finite `deadline_s`. The first request has no parent; every next request's `after` names its predecessor. A task may contain one request. Branches, missing members, reused members and dependencies entering or leaving a task are rejected. Requests outside tasks keep their existing behavior.

```json
"tasks": [
  {"id": "inventory-answer", "request_ids": ["lookup", "summary"], "deadline_s": 1.0}
]
```

A task's clock starts at its root request's `arrival_s`. Its fixed deadline includes queueing, every stream, tool delays and later request arrivals. A request keeps its own release-relative `deadline_s`; the earlier deadline limits admission and streaming. Equal deadlines are attributed to the task. The independent HTTP `--timeout` still produces a failed request when it expires first.

At the task deadline, a queued step expires without HTTP. A step whose eligible release is at or after the task deadline expires with `task_deadline_before_release`; it has neither `released_s` nor `admitted_s` and is excluded from queue statistics, including when a late scheduler wakeup observes both times already past. Following steps become blocked. An active stream receives the same fixed absolute deadline and closes locally when it expires, recording `task_deadline_during_stream`. Completed steps retain their original results.

Each round adds `tasks` and `task_metrics` to its JSON and a whole-task table to Markdown. A task counts as timely only when every step succeeds and the final observed completion is strictly before its deadline. Completion at or after the deadline is expired even when all request records say `ok`. Ordinary request failures make the task failed; either request-budget or task-budget expiry makes it expired. Task records contain the root arrival, fixed deadline, observed end, deciding request and reason. Request records carry `task_id`, task timing, `effective_deadline_s` and `deadline_source` (`task` or `request`); absolute times are relative to workload time zero. Successful task latency runs from root arrival to final completion. Each chain counts once, and any unsuccessful task makes the CLI exit nonzero.

Run the complete fictional example:

```sh
uv run agent-serving-lab demo --workload examples/task-deadlines.json \
  --policy fcfs --concurrency 2 --output results/task-deadlines
```

It finishes the two-step inventory task and an independent request, then expires the shipment task during its two-second tool wait. The expected report has **4 successful requests, 1 expired request and 1 blocked request**, but **1 timely task and 1 expired task**. The command exits **1**. This is a scripted functional example; tool waits and subsequent request messages are fixed workload inputs.

### A fixed deadline-admission tradeoff

[Deterministic tests](tests/test_deadline_admission.py) compare two two-stage tasks, A and B, arriving at time zero with concurrency 1 and aging threshold 0.2. A has input estimate 1 per stage; B has 10. Tool delays are zero. A fake backend advances a controlled clock; service durations live only in that backend, outside the admission policy's inputs. These are simulated examples, not model measurements.

In the first case, A's budget is 4 seconds and B's is 1; each stage takes a simulated 0.375 seconds. In the second case, A's budget is 0.625 seconds with 0.125-second stages; B's is 0.375 seconds but its first stage requires 0.5 seconds. B cannot finish within that second budget even when admitted first.

| Case | Policy | Timely tasks / total | A root queue (simulated s) | A outcome (simulated s) |
|---|---|---:|---:|---|
| Urgent B can finish | fcfs | 1 / 2 | 0 | completed at 1.125 |
| Urgent B can finish | shortest-input | 1 / 2 | 0 | completed at 0.75 |
| Urgent B can finish | aging | 1 / 2 | 0 | completed at 1.125 |
| Urgent B can finish | earliest-deadline | 2 / 2 | 0.75 | completed at 1.5 |
| Urgent B cannot finish | fcfs | 1 / 2 | 0 | completed at 0.5 |
| Urgent B cannot finish | shortest-input | 1 / 2 | 0 | completed at 0.25 |
| Urgent B cannot finish | aging | 1 / 2 | 0 | completed at 0.25 |
| Urgent B cannot finish | earliest-deadline | 0 / 2 | 0.375 | expired at 0.625 |

Deadline ordering saves B in the first case while delaying A. In the second, spending the earliest budget on B also makes A miss its deadline. The option lets users inspect that tradeoff using task outcomes and request queueing together. Reproduce the fixed cases without a model:

```sh
uv run python -m unittest discover -s tests -p test_deadline_admission.py -v
```

## Saved progress and interrupted experiments

The CLI saves the frozen workload and planned policy/repetition order before measurement, then saves each completed round before starting the next. Saving happens outside the round's measured duration. `report.json` is the authoritative snapshot; `report.md` is generated from it and can lag if writing is interrupted.

Each report includes `batch_status`, `planned_runs`, `completed_runs` and `run_plan`:

- `running`: the batch started but has no recorded terminal state. After a forced process termination, read the completed rounds from this snapshot.
- `interrupted`: cancellation reached the CLI; completed rounds remain saved.
- `error`: an unexpected exception stopped the batch. `error_type` and `error_stage` identify the failure without storing arbitrary exception messages.
- `completed`: every planned round finished. Request-level failures, expirations and blocked dependents keep their existing counts and nonzero exit status.

Incomplete rounds are not added to results. JSON snapshots use a temporary file and atomic replacement; if saving fails, the last successfully replaced JSON remains authoritative and may still say `running`. A terminal status cannot be saved when the output storage is unavailable. Each completed round retains its own timing, requests and metrics; incomplete batches do not imply a completed policy comparison.

The default creates a new output directory on each invocation. Use `--output results/my-run` to choose one. If that directory already contains any of `workload.json`, `report.json` or `report.md`, the command stops before creating a backend or making requests. Pick a new path to keep both experiments. Empty directories and directories containing unrelated files are accepted; unrelated files are retained. There is no automatic resume or retry.

## Reading the measurements

Times use a monotonic clock. p50/p95 use nearest-rank quantiles. Metrics include mean and maximum overall and by workload kind; the Markdown report shows each repetition and class separately. `failure_rate` is failed requests divided by all workload requests; expired requests and blocked dependents remain separate counts. `expiration_rate` is expired requests divided by all requests. Queue statistics cover admitted requests only; `expired_queue_s` separately reports release-to-expiration waiting for requests that never entered HTTP. TTFT and E2E distributions cover successful requests only, so read them together with status counts.

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
- [Connection-capacity regression](tests/test_connection_capacity.py): all 128 admitted requests reach a held localhost server before any stream is released; cancellation at concurrency 2 frees the client streams, a follow-up succeeds, and fully consumed tokenization responses reuse a keep-alive connection.
- Deterministic policy ordering, aging promotion, bounded concurrency and dependency failure propagation.
- Deadline and truncated-stream failures, token telemetry gaps, exact metric arithmetic.
- Real Qwen3-0.6B inference through MLX/Metal on Apple M4: 72/72 successful requests, with [reproduction commands and measurements](experiments/mlx-m4-2026-09-30/README.md).
- Two longer-generation admission workloads: 192/192 successful requests, with [short/long latency tradeoffs and frozen replay inputs](experiments/mlx-admission-2026-09-30/README.md).

A controlled localhost probe originally found 128 admitted requests but only 100 server arrivals while those streams were held open. The remaining 28 exhausted their request budgets without reaching the server; the longer pool timeout did not fire. The connection-capacity regression now checks exact arrivals before releasing streams. These counts verify transport behavior, not model throughput; closing a client stream still does not prove server inference has stopped.

## References and license

Original implementation, MIT; runtime dependency [HTTPX](https://github.com/encode/httpx) is BSD-3-Clause (its dependencies retain their own licenses). The dependency lockfile is committed.

Design references: [vLLM OpenAI-compatible serving](https://docs.vllm.ai/en/latest/serving/openai_compatible_server/) (Apache-2.0 project), [Nano-vLLM](https://github.com/GeeeekExplorer/nano-vllm) (MIT). No source from either engine is vendored. Nano-vLLM is an offline engine reference, not a supported HTTP backend by itself.
