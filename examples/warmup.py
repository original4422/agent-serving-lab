"""Warm every prompt shape sequentially; save timing/usage, never generated text."""
import argparse
import asyncio
import json
from pathlib import Path

from serving_lab.backend import OpenAIBackend
from serving_lab.workload import generate, validate


async def warmup(args):
    workload = generate(args.seed, args.count)
    validate(workload)
    backend = OpenAIBackend(args.base_url, args.model)
    records = []
    try:
        for request in workload["requests"]:
            result = await backend.stream(request)
            records.append({"id": request["id"], **result})
            print(request["id"], result["error"], result["usage"], flush=True)
            if result["error"]:
                break
    finally:
        await backend.close()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(records, indent=2) + "\n")
    return int(any(record["error"] for record in records))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--count", type=int, default=12)
    parser.add_argument("--output", default="results/warmup.json")
    raise SystemExit(asyncio.run(warmup(parser.parse_args())))
