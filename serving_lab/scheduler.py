"""Non-preemptive client admission with dependency-aware release times."""
import asyncio
from .workload import validate
from .tasks import task_index

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
    identities = task_index(workload)
    pending = [{"request": r, "index": i, **identities.get(r["id"], {})}
               for i, r in enumerate(workload["requests"])]
    ready, active, finished, records = [], {}, {}, []
    now = lambda: clock() - start

    def budget(entry):
        # A request budget begins at eligible release. A task budget is fixed.
        limits = []
        if "task_deadline_at_s" in entry:
            limits.append((entry["task_deadline_at_s"], "task"))
        if "released_s" in entry and "deadline_s" in entry["request"]:
            limits.append((entry["released_s"] + entry["request"]["deadline_s"], "request"))
        return min(limits, key=lambda limit: limit[0]) if limits else (None, None)

    def metadata(entry):
        result = {key: entry[key] for key in ("task_id", "task_arrival_s", "task_deadline_at_s") if key in entry}
        at, source = budget(entry)
        if at is not None:
            result.update(effective_deadline_s=at, deadline_source=source)
        if "deadline_s" in entry["request"]:
            result["deadline_s"] = entry["request"]["deadline_s"]
        return result

    def expire(entry, at):
        deadline, source = budget(entry)
        if deadline is None or at < deadline:
            return False
        r = entry["request"]
        if "released_s" not in entry:
            error = "task_deadline_before_release"
        else:
            error = "task_deadline_before_admission" if source == "task" else "deadline_before_admission"
        record = {"id": r["id"], "kind": r["kind"], "status": "expired",
                  "end_s": at, "error": error, **metadata(entry),
                  "chunk_times_s": [], "chunk_token_counts": [], "usage": {}}
        if "released_s" in entry:
            record["released_s"] = entry["released_s"]
        records.append(record)
        finished[r["id"]] = record
        return True

    try:
        while pending or ready or active:
            t = now()
            blocked_any = False
            expired_any = False
            for entry in pending[:]:
                r = entry["request"]
                parent = r.get("after")
                if parent and parent not in finished:
                    continue
                if parent and finished[parent]["status"] != "ok":
                    record = {"id": r["id"], "kind": r["kind"], "status": "blocked", "end_s": t,
                              "error": "dependency_failed", **metadata(entry)}
                    records.append(record)
                    finished[r["id"]] = record
                    pending.remove(entry)
                    blocked_any = True
                    continue
                release = max(r["arrival_s"], finished[parent]["end_s"] + r.get("tool_delay_s", 0)) if parent else r["arrival_s"]
                entry["eligible_s"] = release
                if ("task_deadline_at_s" in entry and entry["task_deadline_at_s"] <= release
                        and expire(entry, t)):
                    # A late wakeup cannot invent release after the task budget ended.
                    pending.remove(entry)
                    expired_any = True
                elif release <= t:
                    entry["released_s"] = release
                    ready.append(entry)
                    pending.remove(entry)
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
                deadline, _ = budget(entry)
                options = {"deadline_at": start + deadline} if deadline is not None else {}
                active[asyncio.create_task(backend.stream(r, **options))] = entry
            if expired_any:
                continue
            wakeups = [e["eligible_s"] for e in pending if "eligible_s" in e]
            wakeups += [e["task_deadline_at_s"] for e in pending
                        if "eligible_s" in e and "task_deadline_at_s" in e]
            wakeups += [budget(e)[0] for e in ready if budget(e)[0] is not None]
            if not active:
                if wakeups:
                    await asyncio.sleep(max(0, min(wakeups) - now()))
                continue
            timeout = max(0, min(wakeups) - now()) if wakeups else None
            done, _ = await asyncio.wait(active, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                entry = active.pop(task)
                result = task.result()
                r = entry["request"]
                if result["error"] == "deadline_during_stream" and budget(entry)[1] == "task":
                    result = {**result, "error": "task_deadline_during_stream"}
                record = {"id": r["id"], "kind": r["kind"], "status": "expired" if result["error"] in ("deadline_during_stream", "task_deadline_during_stream") else "failed" if result["error"] else "ok",
                          "released_s": entry["released_s"], "admitted_s": entry["admitted_s"], "end_s": now(),
                          "input_tokens": r["input_tokens"], "input_tokens_source": r["input_tokens_source"], **metadata(entry), **result}
                records.append(record)
                finished[r["id"]] = record
    finally:
        for task in active:
            task.cancel()
        await asyncio.gather(*active, return_exceptions=True)
    return records, now()
