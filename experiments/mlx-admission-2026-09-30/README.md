# Longer generations: short-request speed versus long-request waiting

Measured on 2026-09-30 with real Qwen3-0.6B 4-bit inference on the same Apple M4/Metal backend as the [initial experiment](../mlx-m4-2026-09-30/README.md). **192/192 measured requests and 32/32 warm-up requests succeeded.** Each generated exactly 64 completion tokens; all reported zero cached prompt tokens. There were no retries or extra measurement runs.

Shortest-input reduced short-request and overall mean E2E in both repetitions of both workloads, but postponed long inputs. On the finite short stream, aging cut long-request mean queue time from 4.775 / 5.421 s to 1.301 / 1.310 s, while short-request mean E2E rose from 2.085 / 2.365 s to 3.307 / 3.320 s. FCFS gave those early long requests the shortest wait. This is an observed latency/fairness tradeoff under client admission, with no server engine changes.

## Frozen design

[PROTOCOL.md](PROTOCOL.md) and the two workload files were committed with measurement code `4fc8d32` before starting the model. There are two 16-request workloads, three policies and two repetitions per workload. All prompts request a detailed checklist; output is capped at 64 tokens. Short and long prompt bodies contain 32–64 and 768–1280 repeated words. Admission uses `word_estimate`, while exact post-request usage is retained separately.

- **Mixed burst:** four long and 12 short requests arrive together; FCFS interleaves the long requests. Shortest-input admits all long inputs last (positions 13–16); aging promotes the first long request to position 5.
- **Finite short stream:** two short requests arrive at zero, two long requests at 50 ms, then 12 short requests every 120 ms through 1.44 s. The input rate exceeds the two-slot service rate inferred from the previous experiment. Shortest-input admits the long requests at positions 15–16; aging moves them to positions 5–6; FCFS uses positions 3–4. These orders were identical in both repetitions. See [admission-order.json](admission-order.json).

The client admits at most two requests. Aging promotes requests after 1 s of queueing; it cannot preempt an occupied slot, so observed long waits can exceed 1 s. Each admitted HTTP request has a 60 s deadline. The queue-violation threshold is 1 s. No workload dependencies are used here.

Both workloads use policy order shortest-input → FCFS → aging, then aging → shortest-input → FCFS, seeded at 42. `mixed-burst` ran first. Each workload received one sequential warm-up of all 16 prompts immediately before its six runs. One MLX service lifetime covered both workloads, with no restart or prompt-cache reuse between policies.

## Results

Every row has 16 successes, zero failures and zero blocked requests (failure rate 0). Times start at eligible release. With 16 or fewer observations per run/class, nearest-rank p95 equals the maximum. Two repetitions are individual observations, not a confidence interval.

| Workload | Repeat | Policy | Overall E2E mean / p95 (s) | Short E2E mean (s) | Long queue mean / max (s) | Long E2E max (s) |
|---|---:|---|---:|---:|---:|---:|
| mixed-burst | 1 | shortest-input | 3.441 / 7.712 | 2.362 | 4.840 / 5.646 | 7.712 |
| mixed-burst | 1 | fcfs | 5.372 / 9.236 | 5.486 | 3.405 / 6.960 | 8.530 |
| mixed-burst | 1 | aging | 4.892 / 9.267 | 4.625 | 4.048 / 6.947 | 8.580 |
| mixed-burst | 2 | aging | 4.991 / 9.816 | 4.568 | 4.415 / 7.659 | 9.293 |
| mixed-burst | 2 | shortest-input | 4.342 / 9.703 | 2.921 | 6.611 / 8.102 | 9.703 |
| mixed-burst | 2 | fcfs | 5.399 / 9.298 | 5.512 | 3.414 / 6.998 | 8.607 |
| short-stream | 1 | shortest-input | 2.639 / 6.518 | 2.085 | 4.775 / 4.775 | 6.518 |
| short-stream | 1 | fcfs | 3.426 / 5.248 | 3.575 | 0.627 / 0.627 | 2.381 |
| short-stream | 1 | aging | 3.275 / 5.222 | 3.307 | 1.301 / 1.302 | 3.052 |
| short-stream | 2 | aging | 3.289 / 5.223 | 3.320 | 1.310 / 1.310 | 3.077 |
| short-stream | 2 | shortest-input | 2.970 / 7.204 | 2.365 | 5.421 / 5.422 | 7.204 |
| short-stream | 2 | fcfs | 3.671 / 6.383 | 3.847 | 0.657 / 0.658 | 2.436 |

The mixed burst illustrates why mean and tail should both be reported: shortest-input has lower overall mean E2E in both repetitions, but its overall p95 is better than FCFS in repetition 1 and worse in repetition 2. On the short stream it improves the mean while worsening overall p95 in both repetitions. Aging reduces mean long waiting relative to shortest-input in all four comparisons, with higher mean E2E for short requests. Long-request maxima do not have the same ordering in every burst run.

The similar admission orders but differing timings show remaining runtime variation. Normal desktop processes were active; GPU utilization, thermal state and memory peaks were not sampled. These two synthetic workloads support the measured tradeoff, not a universal winning policy or indefinite-starvation claim. Generation is capped and answer quality is not evaluated. ITL is unavailable because token-level logprobs were not requested; ordinary SSE chunks are not counted as tokens.

## Reproduce

Use the pinned setup and server command in the [initial experiment](../mlx-m4-2026-09-30/README.md#reproduce). It pins MLX 0.32.3, MLX-LM 0.31.3 and model revision `73e3e38d981303bc594367cd910ea6eb48349da8`. The exact backend package list is [unchanged](../mlx-m4-2026-09-30/backend-requirements.txt). The ten model-file hashes and package list were rechecked before this run. Thinking and prompt cache remain disabled; server prompt/decode concurrency are both 2. Hardware and commands are recorded in [environment.json](environment.json).

With that localhost server running, run from the repository root:

```sh
for profile in mixed-burst short-stream; do
  uv run python -m examples.warmup \
    --base-url http://127.0.0.1:18081/v1 --model default_model \
    --workload "experiments/mlx-admission-2026-09-30/$profile/workload.json" \
    --output "results/mlx-admission/$profile/warmup.json" || break
  uv run agent-serving-lab run \
    --base-url http://127.0.0.1:18081/v1 --model default_model \
    --workload "experiments/mlx-admission-2026-09-30/$profile/workload.json" \
    --concurrency 2 --aging 1 --starvation 1 --repeats 2 --seed 42 \
    --output "results/mlx-admission/$profile" || break
done
```

Stop the server with Ctrl-C after measurement. The recorded run stopped MLX before the next managed model experiment; the earlier managed browser performance run had already ended. No other managed performance benchmark overlapped this measurement.

To generate fresh instances of either profile, use `--profile mixed-burst` or `--profile short-stream`, `--count 16`, and `--seed 42` instead of `--workload`. The frozen files above are the canonical replay input for these results.

## Records

- Mixed burst: [workload](mixed-burst/workload.json), [warm-up](mixed-burst/warmup.json), [per-request JSON](mixed-burst/report.json), [full metric tables](mixed-burst/report.md).
- Short stream: [workload](short-stream/workload.json), [warm-up](short-stream/warmup.json), [per-request JSON](short-stream/report.json), [full metric tables](short-stream/report.md).

Each report includes the frozen workload's SHA-256, all request timings, reported prompt/completion usage, queue thresholds and per-kind statistics. Generated text, weights and credentials are excluded. The generic CLI evidence field remains `external_backend_unverified`; the local model's provenance is documented separately here.
