"""Metrics preserve missing token telemetry instead of counting SSE chunks as tokens."""
from collections import Counter
import html
import json
import math


def distribution(values):
    values = sorted(values)
    if not values:
        return {"n": 0, "mean": None, "p50": None, "p95": None, "max": None}
    return {"n": len(values), "p50": values[math.ceil(0.5 * len(values)) - 1],
            "p95": values[math.ceil(0.95 * len(values)) - 1], "max": values[-1],
            "mean": sum(values) / len(values)}


def summarize(records, elapsed, starvation_s):
    admitted = [r for r in records if "admitted_s" in r]
    ok = [r for r in admitted if r["status"] == "ok"]
    waits = [r["admitted_s"] - r["released_s"] for r in admitted]
    metrics = {
        "requests": len(records), "succeeded": len(ok),
        "finish_reason_counts": dict(Counter(r["finish_reason"] for r in ok if r.get("finish_reason") is not None)),
        "finish_reason_missing": sum(r.get("finish_reason") is None for r in ok),
        "failed": sum(r["status"] == "failed" for r in records),
        "expired": sum(r["status"] == "expired" for r in records),
        "expiration_rate": sum(r["status"] == "expired" for r in records) / len(records) if records else None,
        "expired_queue_s": distribution([r["end_s"] - r["released_s"] for r in records
                                         if r.get("error") in ("deadline_before_admission", "task_deadline_before_admission")]),
        "blocked": sum(r["status"] == "blocked" for r in records),
        "failure_rate": sum(r["status"] == "failed" for r in records) / len(records) if records else None,
        "elapsed_s": elapsed, "success_requests_per_s": len(ok) / elapsed,
        "queue_s": distribution(waits),
        "e2e_s": distribution([r["end_s"] - r["released_s"] for r in ok]),
        "ttft_s": distribution([r["admitted_s"] - r["released_s"] + r["chunk_times_s"][0] for r in ok if r["chunk_times_s"]]),
        "dispatch_to_first_chunk_s": distribution([r["chunk_times_s"][0] for r in ok if r["chunk_times_s"]]),
        "inter_chunk_s": distribution([b - a for r in ok for a, b in zip(r["chunk_times_s"], r["chunk_times_s"][1:])]),
        "itl_s": distribution([b - a for r in ok if r["chunk_token_counts"] and all(n == 1 for n in r["chunk_token_counts"])
                               for a, b in zip(r["chunk_times_s"], r["chunk_times_s"][1:])]),
        "starvation_threshold_s": starvation_s,
        "queue_threshold_exceeded": sum(w >= starvation_s for w in waits),
    }
    counts = [r["usage"].get("completion_tokens") for r in ok]
    metrics["output_tokens_per_s"] = sum(counts) / elapsed if counts and all(n is not None for n in counts) else None
    metrics["usage_coverage"] = sum(n is not None for n in counts)
    return metrics


def summarize_tasks(tasks):
    ok = [task for task in tasks if task["status"] == "ok"]
    return {"tasks": len(tasks), "succeeded": len(ok),
            "failed": sum(task["status"] == "failed" for task in tasks),
            "expired": sum(task["status"] == "expired" for task in tasks),
            "e2e_s": distribution([task["end_s"] - task["arrival_s"] for task in ok])}


def markdown(report):
    lines = ["# Agent Serving Lab", "", f"Evidence: **{report['evidence']}**", "",
             "Client admission only; no server scheduling or engine changes.", "",
             "| Repeat | Policy | OK / failed / blocked | Expired | Queue p95 (s) | TTFT p95 (s) | E2E p95 (s) | requests/s |",
             "|---:|---|---:|---:|---:|---:|---:|---:|"]
    if "batch_status" in report:
        lines[2:2] = [f"Batch: **{report['batch_status']}** — {report['completed_runs']} / {report['planned_runs']} rounds completed.", ""]
    fmt = lambda x: "n/a" if x is None else f"{x:.4f}"
    for run in report["runs"]:
        m = run["metrics"]
        lines.append(f"| {run['repeat'] + 1} | {run['policy']} | {m['succeeded']} / {m['failed']} / {m['blocked']} | {m.get('expired', 0)} | {fmt(m['queue_s']['p95'])} | {fmt(m['ttft_s']['p95'])} | {fmt(m['e2e_s']['p95'])} | {fmt(m['success_requests_per_s'])} |")
    lines.extend(["", "## Per-kind waiting and completion", "",
                  "| Repeat | Policy | Kind | OK / failed / blocked | Expired | Queue mean / p95 / max (s) | E2E mean / p95 / max (s) |",
                  "|---:|---|---|---:|---:|---:|---:|"])
    for run in report["runs"]:
        for kind, m in run["by_kind"].items():
            queue = " / ".join(fmt(m["queue_s"][k]) for k in ("mean", "p95", "max"))
            e2e = " / ".join(fmt(m["e2e_s"][k]) for k in ("mean", "p95", "max"))
            lines.append(f"| {run['repeat'] + 1} | {run['policy']} | {kind} | {m['succeeded']} / {m['failed']} / {m['blocked']} | {m.get('expired', 0)} | {queue} | {e2e} |")
    lines.extend(["", "## Successful-stream finish reasons", "",
                  "| Repeat | Policy | Reported reasons | Missing |",
                  "|---:|---|---|---:|"])
    for run in report["runs"]:
        m = run["metrics"]
        counts = html.escape(json.dumps(m.get("finish_reason_counts", {}), sort_keys=True, ensure_ascii=False)).replace("|", "&#124;")
        lines.append(f"| {run['repeat'] + 1} | {run['policy']} | {counts} | {m.get('finish_reason_missing', m['succeeded'])} |")
    lines.extend(["", "Reasons describe server-reported endings of successful streams; length and tool_calls remain transport successes.",
                  "Failed or expired streams may retain a partial observation in JSON and are excluded from these counts."])
    if any("tasks" in run for run in report["runs"]):
        lines.extend(["", "## Whole-task budgets", "",
                      "| Repeat | Policy | Tasks | Timely / failed / expired | Timely E2E mean / p95 (s) |",
                      "|---:|---|---:|---:|---:|"])
        for run in report["runs"]:
            if "task_metrics" not in run:
                continue
            m = run["task_metrics"]
            lines.append(f"| {run['repeat'] + 1} | {run['policy']} | {m['tasks']} | {m['succeeded']} / {m['failed']} / {m['expired']} | {fmt(m['e2e_s']['mean'])} / {fmt(m['e2e_s']['p95'])} |")
        lines.extend(["", "Each declared linear task counts once; timely completion requires every stage to succeed before the task deadline."])
    lines.extend(["", "Full per-request timing, token coverage, chunk intervals and starvation-threshold counts are in JSON.",
                  "Policies share the same workload; dependency release times depend on parent completion.", ""])
    return "\n".join(lines)
