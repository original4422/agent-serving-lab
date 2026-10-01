# Qwen3-0.6B on Apple M4

Real local MLX/Metal inference, measured on 2026-09-30. All 72 measured requests succeeded; no requests failed or were blocked. The existing OpenAI-compatible SSE adapter worked without changes.

## Reproduce

Run from the repository root on an Apple silicon Mac. This backend environment is separate from the ordinary `uv sync --locked` client environment. The pinned model files total about 335 MiB, including 320 MiB of safetensors weights.

```sh
uv sync --locked
uv venv results/mlx-env --python 3.12
uv pip install --python results/mlx-env/bin/python \
  -r experiments/mlx-m4-2026-09-30/backend-requirements.txt
HF_HOME=results/hf-cache results/mlx-env/bin/hf download \
  mlx-community/Qwen3-0.6B-4bit \
  --revision 73e3e38d981303bc594367cd910ea6eb48349da8 \
  --local-dir results/qwen3-0.6b-4bit \
  --include '*.json' --include '*.txt' --include '*.safetensors' --include README.md
shasum -a 256 results/qwen3-0.6b-4bit/model.safetensors
# 392e8d466d56100ada00eb82031fb854297fc9e389b7d303eba3af114e87bce2

HF_HUB_OFFLINE=1 results/mlx-env/bin/mlx_lm.server \
  --model results/qwen3-0.6b-4bit --host 127.0.0.1 --port 18081 \
  --prompt-cache-size 0 --decode-concurrency 2 --prompt-concurrency 2 \
  --chat-template-args '{"enable_thinking":false}' --log-level WARNING
```

Leave that server running. In another terminal, from the repository root:

```sh
uv run python -m examples.warmup \
  --base-url http://127.0.0.1:18081/v1 --model default_model \
  --seed 42 --count 12 --output results/mlx-warmup.json
uv run agent-serving-lab run \
  --base-url http://127.0.0.1:18081/v1 --model default_model \
  --workload experiments/mlx-m4-2026-09-30/workload.json \
  --seed 42 --concurrency 2 --repeats 2 --output results/mlx-replay
```

Stop the server with Ctrl-C after the run. `default_model` is MLX-LM's alias for the model supplied at server startup. No remote model code is enabled. The model and runtime stay in gitignored `results/`.

## Conditions

- Apple M4, 10 GPU cores, 24 GiB unified memory; macOS 15.7.8, Metal available. Normal desktop processes remained active. The other managed browser benchmark finished before this experiment; no other managed model benchmark ran concurrently.
- Backend Python 3.12.14, MLX/MLX-Metal 0.32.3, MLX-LM 0.31.3, Transformers 5.17.0. All installed package versions are in [backend-requirements.txt](backend-requirements.txt).
- Model: [mlx-community/Qwen3-0.6B-4bit at the pinned revision](https://huggingface.co/mlx-community/Qwen3-0.6B-4bit/tree/73e3e38d981303bc594367cd910ea6eb48349da8), based on Qwen3-0.6B, Apache-2.0. File hashes and startup arguments are in [environment.json](environment.json).
- Client measurement code: commit `804f9e6`; temperature 0, maximum 12 generated tokens per request, two admitted requests at a time. All measured requests returned exactly 12 completion tokens. This measures capped generation, not answer quality.
- One sequential warm-up of all 12 workload requests before measurement; its timing and usage are in [warmup.json](warmup.json). Cold startup/model download is excluded. Prompt cache was disabled for the entire service lifetime; all 72 measured usage records reported `cached_tokens: 0`.
- Same frozen [workload.json](workload.json), seed 42, three requests each of short, long, tool follow-up and burst. Two arrival bursts at 0 and 120 ms. Follow-ups become eligible 30 ms after their parent finishes and replay a fixed tool transcript.
- Default aging threshold 200 ms; queue threshold 1 s. Actual order: shortest-input → FCFS → aging, then aging → shortest-input → FCFS. No backend restart between policies.
- Input scheduling counts remain `word_estimate`; MLX-LM was not queried through `/tokenize`. Server usage supplies exact post-request prompt/completion counts but does not retroactively change admission decisions. No logprobs were requested: inter-chunk times are recorded, ITL is unavailable.

## Measurements

Each row is 12 successful requests. Quantiles use nearest rank; with 12 requests, p95 is the maximum. TTFT and E2E start at eligible release, so both include client queueing and localhost transport. Output tokens/s uses server-reported completion tokens divided by full run duration.

| Repetition | Policy | Queue p95 (s) | TTFT p95 (s) | E2E p95 (s) | Output tokens/s |
|---|---|---:|---:|---:|---:|
| 1 | shortest-input | 2.2050 | 2.5887 | 2.6665 | 43.49 |
| 1 | fcfs | 2.5204 | 2.7982 | 2.8784 | 43.22 |
| 1 | aging | 2.4631 | 2.7195 | 2.8026 | 44.33 |
| 2 | aging | 2.4547 | 2.7324 | 2.8159 | 43.29 |
| 2 | shortest-input | 1.7245 | 2.1018 | 2.1794 | 50.59 |
| 2 | fcfs | 2.5581 | 2.8211 | 2.9060 | 42.93 |

Shortest-input reduced E2E p95 in both repetitions of this workload, with a notably larger difference in repetition 2. The experiment establishes a working real-model path and a measurable client-ordering effect under these conditions. Two repetitions of short synthetic generations are not enough to select a general scheduling policy.

Client admission controls which requests reach MLX-LM first. MLX-LM still controls its own prefill and decode batching; this experiment does not modify that engine. GPU utilization, power and memory peaks were not sampled.

[report.json](report.json) retains every measured timing and usage record without generated text. Its generic CLI evidence field remains `external_backend_unverified`: the CLI cannot attest an arbitrary endpoint. This experiment's local runtime/model provenance is recorded separately in [environment.json](environment.json).

Primary references: [MLX installation](https://ml-explore.github.io/mlx/build/html/install.html), [MLX-LM](https://github.com/ml-explore/mlx-lm), [upstream model license](https://huggingface.co/Qwen/Qwen3-0.6B/blob/main/LICENSE). MLX-LM is MIT; model weights are Apache-2.0. Neither source nor weights are vendored.
