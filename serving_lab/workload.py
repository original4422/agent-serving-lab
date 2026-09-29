"""Seeded, serializable synthetic request graphs."""
import random
import math

PROFILES = ("agent-mix", "mixed-burst", "short-stream")


def admission_profile(name, seed=42, count=16):
    """Independent, capped generations for input-order/fairness experiments."""
    if name not in PROFILES[1:] or count < 8:
        raise ValueError("admission profiles require a known name and at least 8 requests")
    rng = random.Random(seed)
    requests = []
    long_indices = set(range(0, count, 4)) if name == "mixed-burst" else {2, 3}
    for i in range(count):
        kind = "long" if i in long_indices else "short"
        units = rng.randint(768, 1280) if kind == "long" else rng.randint(32, 64)
        arrival = 0 if name == "mixed-burst" or i < 2 else .05 if i < 4 else (i - 3) * .12
        requests.append({
            "id": f"r{i}", "kind": kind, "arrival_s": round(arrival, 6),
            "input_tokens": units, "input_tokens_source": "word_estimate",
            "messages": [{"role": "user", "content":
                "Write a detailed 20-step numbered checklist for reviewing this inventory. "
                "Explain each step. Inventory: " + "item " * units}],
            "max_tokens": 64,
        })
    return {"version": 1, "seed": seed, "profile": name, "requests": requests}


def generate(seed=42, count=24):
    rng = random.Random(seed)
    requests = []
    for i in range(count):
        kind = ["short", "long", "tool_followup", "burst"][i % 4]
        units = rng.randint(16, 48) if kind != "long" else rng.randint(512, 1024)
        parent = f"r{i-1}" if kind == "tool_followup" else None
        requests.append({
            "id": f"r{i}", "kind": kind,
            "arrival_s": round((i // 8) * 0.12, 6),
            "after": parent, "tool_delay_s": 0.03 if parent else 0,
            "input_tokens": units, "input_tokens_source": "word_estimate",
            "messages": [{"role": "user", "content": "Summarize: " + "item " * units}],
            "max_tokens": 12,
        })
        if parent:
            # A fixed transcript replay, not model-generated tool execution.
            requests[-1]["messages"] = [
                {"role": "user", "content": "Look up the item status."},
                {"role": "assistant", "content": None, "tool_calls": [{
                    "id": "lookup", "type": "function", "function": {
                        "name": "lookup_status", "arguments": "{}"}}]},
                {"role": "tool", "tool_call_id": "lookup", "content": "item " * units},
                {"role": "user", "content": "Summarize the result."},
            ]
    return {"version": 1, "seed": seed, "requests": requests}


def validate(workload):
    requests = workload["requests"]
    ids = {r["id"] for r in requests}
    if not requests or len(ids) != len(requests):
        raise ValueError("requests must be nonempty with unique IDs")
    parents = {r["id"]: r.get("after") for r in requests}
    for r in requests:
        if not all(math.isfinite(r.get(key, 0)) for key in ("arrival_s", "tool_delay_s", "input_tokens", "max_tokens")):
            raise ValueError("workload numbers must be finite")
        if r["arrival_s"] < 0 or r.get("tool_delay_s", 0) < 0 or r["input_tokens"] <= 0 or r["max_tokens"] <= 0:
            raise ValueError("invalid arrival, delay or token count")
        seen = {r["id"]}
        parent = parents[r["id"]]
        while parent:
            if parent not in ids or parent in seen:
                raise ValueError("missing dependency or dependency cycle")
            seen.add(parent)
            parent = parents[parent]


async def tokenize(workload, backend):
    """vLLM extension: tokenize exact chat messages before any timed run."""
    for r in workload["requests"]:
        data = await backend.tokenize(r["messages"])
        r["input_tokens"] = data["count"]
        r["input_tokens_source"] = "server_tokenize"
