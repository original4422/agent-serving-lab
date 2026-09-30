import argparse
import asyncio
import hashlib
import math
import json
import os
from pathlib import Path
import random
from .backend import OpenAIBackend
from .metrics import markdown, summarize, summarize_tasks, distribution
from .tasks import outcomes
from .mock_server import serve
from .reporting import Output
from .scheduler import POLICIES, OPT_IN_POLICIES, run
from .workload import PROFILES, admission_profile, generate, tokenize, validate


async def experiment(args, url, evidence):
    if args.workload:
        workload = json.loads(Path(args.workload).read_text())
    elif args.profile == "agent-mix":
        workload = generate(args.seed, args.count)
    else:
        workload = admission_profile(args.profile, args.seed, args.count)
    validate(workload)
    output = Output(args.output)
    print(f"Results: {output.path.resolve()}", flush=True)
    rng = random.Random(args.seed)
    plan = []
    for repeat in range(args.repeats):
        policies = list(POLICIES) if args.policy == "all" else [args.policy]
        rng.shuffle(policies)
        plan.extend({"policy": policy, "repeat": repeat} for policy in policies)
    report = {"schema_version": 1, "evidence": evidence, "model": args.model,
              "batch_status": "running", "planned_runs": len(plan), "run_plan": plan,
              "completed_runs": 0, "runs": [],
              "config": {"concurrency": args.concurrency, "aging_s": args.aging,
                         "timeout_s": args.timeout, "repeats": args.repeats,
                         "starvation_s": args.starvation, "order_seed": args.seed}}

    def describe_workload():
        report.update({
            "workload_sha256": hashlib.sha256(json.dumps(workload, sort_keys=True).encode()).hexdigest(),
            "workload_seed": workload.get("seed"),
            "workload_summary": {
                "input_tokens": distribution([r["input_tokens"] for r in workload["requests"]]),
                "arrival_s": distribution([r["arrival_s"] for r in workload["requests"]]),
                "token_count_sources": sorted({r["input_tokens_source"] for r in workload["requests"]})}})

    describe_workload()
    output.workload(workload)
    output.snapshot(report)
    backend = None
    stage = "backend_setup"
    try:
        try:
            backend = OpenAIBackend(url, args.model, os.environ.get(args.api_key_env), args.timeout, args.logprobs,
                                    max_connections=args.concurrency)
            if args.tokenize:
                stage = "tokenize"
                await tokenize(workload, backend)
                if evidence == "scripted_http_not_llm":
                    for r in workload["requests"]:
                        r["input_tokens_source"] = "scripted_word_count"
                describe_workload()
                stage = "persist"
                output.workload(workload)
                output.snapshot(report)
            for entry in plan:
                stage = "run"
                records, elapsed = await run(workload, backend, entry["policy"], args.concurrency, args.aging)
                stage = "summarize"
                result = {**entry, "metrics": summarize(records, elapsed, args.starvation),
                    "by_kind": {kind: summarize([r for r in records if r["kind"] == kind], elapsed, args.starvation)
                                for kind in sorted({r["kind"] for r in records})}, "requests": records}
                if workload.get("tasks"):
                    result["tasks"] = outcomes(workload, records)
                    result["task_metrics"] = summarize_tasks(result["tasks"])
                report["runs"].append(result)
                stage = "persist"
                output.snapshot(report)
            stage = "backend_close"
        finally:
            if backend is not None:
                await backend.close()
    except (asyncio.CancelledError, KeyboardInterrupt):
        report["batch_status"] = "interrupted"
        if stage != "persist":
            output.snapshot(report)
        raise
    except Exception as exc:
        report.update(batch_status="error", error_type=type(exc).__name__, error_stage=stage)
        if stage != "persist":
            output.snapshot(report)
        raise
    report["batch_status"] = "completed"
    output.snapshot(report)
    print(markdown(report))
    return int(any(r["metrics"]["failed"] or r["metrics"]["blocked"] or r["metrics"]["expired"]
                   or any(task["status"] != "ok" for task in r.get("tasks", [])) for r in report["runs"]))


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
        p.add_argument("--profile", choices=PROFILES, default="agent-mix", help="Generated workload shape; ignored with --workload")
        p.add_argument("--seed", type=int, default=42)
        p.add_argument("--count", type=int, default=24)
        p.add_argument("--concurrency", type=int, default=2)
        p.add_argument("--policy", choices=["all", *POLICIES, *OPT_IN_POLICIES], default="all")
        p.add_argument("--repeats", type=int, default=1)
        p.add_argument("--aging", type=float, default=0.2)
        p.add_argument("--starvation", type=float, default=1.0)
        p.add_argument("--timeout", type=float, default=60)
        p.add_argument("--tokenize", action="store_true", help="Use vLLM /tokenize before timed runs")
        p.add_argument("--logprobs", action="store_true", help="Request token counts per chunk; unsupported servers may reject")
        p.add_argument("--api-key-env", default="SERVING_LAB_API_KEY")
        p.add_argument("--output", help="New result directory; default: unique results/run-<UTC>-<suffix>")
    args = parser.parse_args()
    if not all(math.isfinite(v) and v > 0 for v in (args.timeout, args.starvation, args.aging)) or args.repeats < 1 or args.concurrency < 1:
        parser.error("repeats, timeout and starvation threshold must be positive")
    raise SystemExit(asyncio.run(execute(args)))
