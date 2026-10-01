"""Non-preemptive client admission with dependency-aware release times."""
import asyncio
import time
from .workload import validate

POLICIES = ("fcfs", "shortest-input", "aging")


def choose(ready, now, policy, aging_s):
    if policy not in POLICIES:
        raise ValueError("unknown policy")
    # Aging promotes overdue requests into FIFO ahead of all non-overdue work.
    if policy == "aging":
        overdue = [r for r in ready if now - r["released_s"] >= aging_s]
        if overdue:
            return min(overdue, key=lambda r: (r["released_s"], r["index"]))
    if policy == "fcfs":
        return min(ready, key=lambda r: (r["released_s"], r["index"]))
    return min(ready, key=lambda r: (r["request"]["input_tokens"], r["released_s"], r["index"]))


async def run(workload, backend, policy="fcfs", concurrency=2, aging_s=0.2):
    validate(workload)
    if concurrency < 1 or aging_s <= 0 or policy not in POLICIES:
        raise ValueError("invalid scheduler configuration")
    start = time.perf_counter()
    pending = [{"request": r, "index": i} for i, r in enumerate(workload["requests"])]
    ready, active, finished, records = [], {}, {}, []
    now = lambda: time.perf_counter() - start
    while pending or ready or active:
        t = now()
        for entry in pending[:]:
            r = entry["request"]
            parent = r.get("after")
            if parent and parent not in finished:
                continue
            if parent and finished[parent]["status"] != "ok":
                record = {"id": r["id"], "kind": r["kind"], "status": "blocked", "end_s": t,
                          "error": "dependency_failed"}
                records.append(record)
                finished[r["id"]] = record
                pending.remove(entry)
                continue
            release = max(r["arrival_s"], finished[parent]["end_s"] + r.get("tool_delay_s", 0)) if parent else r["arrival_s"]
            entry["released_s"] = release
            if release <= t:
                ready.append(entry)
                pending.remove(entry)
        while ready and len(active) < concurrency:
            entry = choose(ready, now(), policy, aging_s)
            ready.remove(entry)
            entry["admitted_s"] = now()
            active[asyncio.create_task(backend.stream(entry["request"]))] = entry
        if not active:
            if pending:
                releases = [e["released_s"] for e in pending if "released_s" in e]
                if releases:
                    await asyncio.sleep(max(0, min(releases) - now()))
            continue
        releases = [e["released_s"] for e in pending if "released_s" in e]
        timeout = max(0, min(releases) - now()) if releases and len(active) < concurrency else None
        done, _ = await asyncio.wait(active, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            entry = active.pop(task)
            result = task.result()
            r = entry["request"]
            record = {"id": r["id"], "kind": r["kind"], "status": "failed" if result["error"] else "ok",
                      "released_s": entry["released_s"], "admitted_s": entry["admitted_s"], "end_s": now(),
                      "input_tokens": r["input_tokens"], "input_tokens_source": r["input_tokens_source"], **result}
            records.append(record)
            finished[r["id"]] = record
    return records, now()
