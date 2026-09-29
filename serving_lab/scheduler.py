"""Non-preemptive client admission with dependency-aware release times."""
import asyncio
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
    clock = asyncio.get_running_loop().time
    start = clock()
    pending = [{"request": r, "index": i} for i, r in enumerate(workload["requests"])]
    ready, active, finished, records = [], {}, {}, []
    now = lambda: clock() - start

    def expire(entry, at):
        r = entry["request"]
        if "deadline_s" not in r or at < entry["released_s"] + r["deadline_s"]:
            return False
        record = {"id": r["id"], "kind": r["kind"], "status": "expired",
                  "released_s": entry["released_s"], "end_s": at,
                  "deadline_s": r["deadline_s"], "error": "deadline_before_admission",
                  "chunk_times_s": [], "chunk_token_counts": [], "usage": {}}
        records.append(record)
        finished[r["id"]] = record
        return True

    try:
        while pending or ready or active:
            t = now()
            blocked_any = False
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
                    blocked_any = True
                    continue
                release = max(r["arrival_s"], finished[parent]["end_s"] + r.get("tool_delay_s", 0)) if parent else r["arrival_s"]
                entry["released_s"] = release
                if release <= t:
                    ready.append(entry)
                    pending.remove(entry)
            expired_any = False
            for entry in ready[:]:
                if expire(entry, now()):
                    ready.remove(entry)
                    expired_any = True
            if blocked_any or expired_any:
                continue  # Propagate all blocked descendants before waiting for active streams.
            while ready and len(active) < concurrency:
                entry = choose(ready, now(), policy, aging_s)
                ready.remove(entry)
                admitted_s = now()
                if expire(entry, admitted_s):
                    expired_any = True
                    break
                entry["admitted_s"] = admitted_s
                r = entry["request"]
                options = {"deadline_at": start + entry["released_s"] + r["deadline_s"]} if "deadline_s" in r else {}
                active[asyncio.create_task(backend.stream(r, **options))] = entry
            if expired_any:
                continue
            if not active:
                if pending:
                    releases = [e["released_s"] for e in pending if "released_s" in e]
                    if releases:
                        await asyncio.sleep(max(0, min(releases) - now()))
                continue
            releases = [e["released_s"] for e in pending if "released_s" in e]
            wakeups = releases + [e["released_s"] + e["request"]["deadline_s"]
                                  for e in ready if "deadline_s" in e["request"]]
            timeout = max(0, min(wakeups) - now()) if wakeups else None
            done, _ = await asyncio.wait(active, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                entry = active.pop(task)
                result = task.result()
                r = entry["request"]
                record = {"id": r["id"], "kind": r["kind"], "status": "expired" if result["error"] == "deadline_during_stream" else "failed" if result["error"] else "ok",
                          "released_s": entry["released_s"], "admitted_s": entry["admitted_s"], "end_s": now(),
                          "input_tokens": r["input_tokens"], "input_tokens_source": r["input_tokens_source"], **result}
                if "deadline_s" in r:
                    record["deadline_s"] = r["deadline_s"]
                records.append(record)
                finished[r["id"]] = record
    finally:
        for task in active:
            task.cancel()
        await asyncio.gather(*active, return_exceptions=True)
    return records, now()
