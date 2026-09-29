# Admission fairness protocol

Frozen before model measurement on 2026-09-30. This extends the earlier 12-token [MLX experiment](../mlx-m4-2026-09-30/README.md) with longer capped generations and explicit long-request waiting comparisons.

- Two frozen workloads, each 16 independent requests; three policies, two repetitions: 192 measured requests. Sequential warm-up of each workload's 16 prompts immediately before that workload's six runs: 32 warm-up requests. Total model calls: 224. No retries or result-dependent extra runs.
- `mixed-burst`: all 16 arrive at zero; four long inputs interleaved with 12 short inputs in FCFS order.
- `short-stream`: two short requests at zero occupy the two slots; two long requests arrive at 50 ms; another 12 short requests arrive every 120 ms from 120 ms through 1.44 s. Finite overload, not a claim of indefinite starvation.
- Short prompt bodies contain 32–64 repeated words; long bodies 768–1280. Seed 42. Scheduling counts are body `word_estimate`, not exact chat-token counts. Every request asks for a detailed checklist and has the same 64-token output cap. Actual generation lengths come from server usage.
- Timing choice is based on the previous frozen experiment: short-request HTTP durations ranged 0.305–0.817 s (median 0.520 s) even at a 12-token cap. Two slots at that median serve about 3.85 requests/s, below the new 8.33 requests/s arrival rate. The 64-token cap adds decode work if reached. The arrival schedule will not be tuned after seeing new results; if it does not build a queue, report that finding.
- Client concurrency 2; aging threshold 1 s; queue-violation threshold 1 s; admitted-request timeout 60 s. The 1 s threshold is a promotion trigger, not a deadline: an occupied slot must finish before promotion can take effect.
- Same pinned Qwen3-0.6B 4-bit model and MLX runtime, temperature 0, thinking disabled, prompt cache disabled, server prefill/decode concurrency 2. One service lifetime for both workloads, `mixed-burst` first. No managed browser/context performance measurement concurrently; normal desktop processes remain active.
- Policy order seeded at 42 separately for each workload: shortest-input → FCFS → aging; then aging → shortest-input → FCFS. Two repetitions give individual observations, not significance estimates. No backend restart between policies.
- Record overall and per-kind queue/E2E mean, median, p95 and maximum; overall TTFT and throughput; long-request admission order; success/failure/blocked counts; exact post-request completion usage. At 16 or fewer observations per run/class, nearest-rank p95 equals maximum.
- Keep all per-request records and workload digests. No generated text, endpoint credentials or weights in the public results. ITL remains unavailable without token-level logprobs.

The question is whether shorter-input-first improves short/overall completion while postponing long requests, and whether aging trades some short-request benefit for less long-request waiting. Equal output caps isolate one source of variation but do not make input length an exact service-time predictor. MLX controls server batching; only client admission order changes.
