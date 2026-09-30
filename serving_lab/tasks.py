"""Fixed budgets and outcomes for explicitly declared, complete linear tasks."""
import math


def validate_tasks(workload):
    tasks = workload.get("tasks", [])
    requests = {r["id"]: r for r in workload["requests"]}
    task_ids, members, next_member = set(), set(), {}
    for task in tasks:
        if not isinstance(task["id"], str) or not task["id"] or task["id"] in task_ids:
            raise ValueError("task IDs must be nonempty and unique")
        task_ids.add(task["id"])
        chain = task["request_ids"]
        if not isinstance(chain, list) or not chain:
            raise ValueError("task request_ids must be a nonempty ordered list")
        if not math.isfinite(task["deadline_s"]) or task["deadline_s"] <= 0:
            raise ValueError("task deadline_s must be positive and finite")
        for index, name in enumerate(chain):
            if name not in requests or name in members:
                raise ValueError("task members must exist and cannot overlap or repeat")
            expected_parent = chain[index - 1] if index else None
            if requests[name].get("after") != expected_parent:
                raise ValueError("task members must form an ordered chain starting at a root")
            members.add(name)
            next_member[name] = chain[index + 1] if index + 1 < len(chain) else None
    for request in requests.values():
        parent = request.get("after")
        if parent in members and next_member[parent] != request["id"]:
            raise ValueError("task chains must be complete, without branches or outside dependents")


def task_index(workload):
    requests = {r["id"]: r for r in workload["requests"]}
    result = {}
    for task in workload.get("tasks", []):
        arrival = requests[task["request_ids"][0]]["arrival_s"]
        identity = {"task_id": task["id"], "task_arrival_s": arrival,
                    "task_deadline_at_s": arrival + task["deadline_s"]}
        for name in task["request_ids"]:
            result[name] = identity
    return result


def outcomes(workload, records):
    by_id = {r["id"]: r for r in records}
    index = task_index(workload)
    result = []
    for task in workload.get("tasks", []):
        chain = task["request_ids"]
        if any(name not in by_id for name in chain):
            raise ValueError("task outcome requires every member's terminal record")
        identity = index[chain[0]]
        terminal = next((by_id[name] for name in chain if by_id[name]["status"] != "ok"), by_id[chain[-1]])
        status = terminal["status"]
        reason = terminal.get("error")
        if status == "blocked":
            status = "failed"
        elif status == "ok" and terminal["end_s"] >= identity["task_deadline_at_s"]:
            status, reason = "expired", "task_deadline_at_completion"
        result.append({"id": task["id"], "request_ids": chain,
                       "arrival_s": identity["task_arrival_s"],
                       "deadline_at_s": identity["task_deadline_at_s"], "deadline_s": task["deadline_s"],
                       "end_s": terminal["end_s"], "status": status,
                       "deciding_request_id": terminal["id"], "reason": reason})
    return result
