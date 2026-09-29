import argparse
import asyncio
import hashlib
import math
import json
import os
from pathlib import Path
import random
from .backend import OpenAIBackend
from .metrics import markdown, summarize, distribution
from .mock_server import serve
from .scheduler import POLICIES, run
from .workload import generate, tokenize, validate


async def experiment(args, url, evidence):
    workload = json.loads(Path(args.workload).read_text()) if args.workload else generate(args.seed, args.count)
    validate(workload)
    backend = OpenAIBackend(url, args.model, os.environ.get(args.api_key_env), args.timeout, args.logprobs)
    try:
        if args.tokenize:
            await tokenize(workload, backend)
            if evidence == "scripted_http_not_llm":
                for r in workload["requests"]:
                    r["input_tokens_source"] = "scripted_word_count"
        digest = hashlib.sha256(json.dumps(workload, sort_keys=True).encode()).hexdigest()
        report = {"schema_version": 1, "evidence": evidence, "model": args.model,
                  "workload_sha256": digest, "workload_seed": workload.get("seed"),
                  "config": {"concurrency": args.concurrency, "aging_s": args.aging,
                             "timeout_s": args.timeout, "repeats": args.repeats,
                             "order_seed": args.seed},
                  "workload_summary": {"input_tokens": distribution([r["input_tokens"] for r in workload["requests"]]),
                      "arrival_s": distribution([r["arrival_s"] for r in workload["requests"]]),
                      "token_count_sources": sorted({r["input_tokens_source"] for r in workload["requests"]})}, "runs": []}
        rng = random.Random(args.seed)
        for repeat in range(args.repeats):
            policies = list(POLICIES) if args.policy == "all" else [args.policy]
            rng.shuffle(policies)
            for policy in policies:
                records, elapsed = await run(workload, backend, policy, args.concurrency, args.aging)
                report["runs"].append({"policy": policy, "repeat": repeat,
                    "metrics": summarize(records, elapsed, args.starvation),
                    "by_kind": {kind: summarize([r for r in records if r["kind"] == kind], elapsed, args.starvation)
                                for kind in sorted({r["kind"] for r in records})}, "requests": records})
        out = Path(args.output)
        out.mkdir(parents=True, exist_ok=True)
        (out / "workload.json").write_text(json.dumps(workload, indent=2) + "\n")
        (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        (out / "report.md").write_text(markdown(report))
        print(markdown(report))
        return int(any(r["metrics"]["failed"] or r["metrics"]["blocked"] for r in report["runs"]))
    finally:
        await backend.close()


async def execute(args):
    if args.command == "demo":
        async with serve() as url:
            return await experiment(args, url, "scripted_http_not_llm")
    return await experiment(args, args.base_url, "external_backend_unverified")


def main():
    parser = argparse.ArgumentParser(description="Compare client-admission policies over streaming HTTP")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("demo", "run"):
        p = sub.add_parser(command)
        if command == "run":
            p.add_argument("--base-url", required=True, help="OpenAI-compatible base URL ending /v1")
        p.add_argument("--model", default="scripted-demo" if command == "demo" else None, required=command == "run")
        p.add_argument("--workload", help="Saved workload JSON; otherwise generate a seeded mixture")
        p.add_argument("--seed", type=int, default=42)
        p.add_argument("--count", type=int, default=24)
        p.add_argument("--concurrency", type=int, default=2)
        p.add_argument("--policy", choices=["all", *POLICIES], default="all")
        p.add_argument("--repeats", type=int, default=1)
        p.add_argument("--aging", type=float, default=0.2)
        p.add_argument("--starvation", type=float, default=1.0)
        p.add_argument("--timeout", type=float, default=60)
        p.add_argument("--tokenize", action="store_true", help="Use vLLM /tokenize before timed runs")
        p.add_argument("--logprobs", action="store_true", help="Request token counts per chunk; unsupported servers may reject")
        p.add_argument("--api-key-env", default="SERVING_LAB_API_KEY")
        p.add_argument("--output", default="results/latest")
    args = parser.parse_args()
    if not all(math.isfinite(v) and v > 0 for v in (args.timeout, args.starvation, args.aging)) or args.repeats < 1 or args.concurrency < 1:
        parser.error("repeats, timeout and starvation threshold must be positive")
    raise SystemExit(asyncio.run(execute(args)))
