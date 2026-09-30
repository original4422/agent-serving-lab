"""Metrics preserve missing token telemetry instead of counting SSE chunks as tokens."""
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
        "failed": sum(r["status"] == "failed" for r in records),
        "expired": sum(r["status"] == "expired" for r in records),
        "expiration_rate": sum(r["status"] == "expired" for r in records) / len(records) if records else None,
        "expired_queue_s": distribution([r["end_s"] - r["released_s"] for r in records
                                         if r.get("error") == "deadline_before_admission"]),
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
    lines.extend(["", "Full per-request timing, token coverage, chunk intervals and starvation-threshold counts are in JSON.",
                  "Policies share the same workload; dependency release times depend on parent completion.", ""])
    return "\n".join(lines)
