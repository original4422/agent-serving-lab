"""Opt-in deadline admission: fixed benefit and harm cases, not model timings."""
import asyncio
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, patch

from serving_lab.cli import experiment
from serving_lab.scheduler import POLICIES, budget, choose, run
from serving_lab.tasks import outcomes
from test_reporting import arguments, completed


def entry(index, release=0, *, tokens=10, request_budget=None, task_deadline=None):
    value = {"index": index, "released_s": release, "request": {"input_tokens": tokens}}
    if request_budget is not None:
        value["request"]["deadline_s"] = request_budget
    if task_deadline is not None:
        value["task_deadline_at_s"] = task_deadline
    return value


def workload(deadlines):
    requests = []
    for name, tokens in (("A", 1), ("B", 10)):
        for stage in range(2):
            request = {"id": f"{name}{stage}", "kind": name, "arrival_s": 0,
                       "input_tokens": tokens, "input_tokens_source": "controlled_estimate",
                       "max_tokens": 1, "messages": [{"role": "user", "content": f"Fictional stage {name}{stage}"}]}
            if stage:
                request.update(after=f"{name}0", tool_delay_s=0)
            requests.append(request)
    return {"version": 1, "requests": requests, "tasks": [
        {"id": name, "request_ids": [f"{name}0", f"{name}1"], "deadline_s": deadline}
        for name, deadline in deadlines.items()]}


def response(error=None):
    return {"error": error, "chunk_times_s": [], "chunk_token_counts": [], "usage": {}}


async def simulate(work, hidden_durations, policy):
    clock, order = [1024.0], []

    class Backend:
        async def stream(self, request, **options):
            order.append(request["id"])
            finish = clock[0] + hidden_durations[request["kind"]]
            deadline = options["deadline_at"]
            clock[0] = min(finish, deadline)
            return response("deadline_during_stream" if finish >= deadline else None)

    loop = asyncio.get_running_loop()
    with patch.object(loop, "time", side_effect=lambda: clock[0]):
        records, _ = await run(work, Backend(), policy=policy, concurrency=1, aging_s=.2)
    return records, outcomes(work, records), order


class DeadlineAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        asyncio.get_running_loop().set_debug(False)  # Synthetic clock jumps are not slow callbacks.

    def test_effective_deadline_sort_uses_same_budget_as_enforcement(self):
        ready = [entry(0), entry(1, task_deadline=8), entry(2, 2, request_budget=2.5),
                 entry(3, 1, request_budget=8, task_deadline=4),
                 entry(4, .5, request_budget=2.25, task_deadline=9)]
        selected = []
        while ready:
            item = choose(ready, 2, "earliest-deadline", .2)
            ready.remove(item)
            selected.append(item["index"])
        self.assertEqual(selected, [4, 3, 2, 1, 0])
        self.assertEqual(budget(entry(5, 1, request_budget=1, task_deadline=2)), (2, "task"))

    def test_equal_deadline_ties_and_no_deadline_fcfs(self):
        early = entry(9, .5, request_budget=1.5)
        later = entry(3, 1, task_deadline=2)
        stable = entry(2, 1, task_deadline=2)
        ready = [later, stable, early]
        self.assertIs(choose(ready, 1, "earliest-deadline", .2), early)
        self.assertIs(choose([later, stable], 1, "earliest-deadline", .2), stable)
        unconstrained = [entry(4, .5, tokens=1), entry(2, 0, tokens=100), entry(1, 0, tokens=200)]
        while unconstrained:
            expected = choose(unconstrained, 1, "fcfs", .2)
            self.assertIs(choose(unconstrained, 1, "earliest-deadline", .2), expected)
            unconstrained.remove(expected)

    async def test_fixed_benefit_and_harm_cases(self):
        cases = [
            ({"A": 4, "B": 1}, {"A": .375, "B": .375},
             {"fcfs": (1, 1.125, 0), "shortest-input": (1, .75, 0), "aging": (1, 1.125, 0),
              "earliest-deadline": (2, 1.5, .75)}),
            ({"A": .625, "B": .375}, {"A": .125, "B": .5},
             {"fcfs": (1, .5, 0), "shortest-input": (1, .25, 0), "aging": (1, .25, 0),
              "earliest-deadline": (0, .625, .375)}),
        ]
        for deadlines, durations, expected in cases:
            work = workload(deadlines)
            for policy, (timely, a_end, a_queue) in expected.items():
                with self.subTest(deadlines=deadlines, policy=policy):
                    records, tasks, order = await simulate(work, durations, policy)
                    self.assertEqual(sum(task["status"] == "ok" for task in tasks), timely)
                    self.assertEqual(tasks[0]["end_s"], a_end)
                    root = next(record for record in records if record["id"] == "A0")
                    self.assertEqual(root["admitted_s"] - root["released_s"], a_queue)
                    if policy == "earliest-deadline":
                        self.assertEqual(order, ["B0", "B1", "A0", "A1"] if timely else ["B0", "A0", "A1"])

    async def test_ineligible_urgent_request_waits_and_does_not_preempt_active_stream(self):
        work = workload({"A": 2, "B": .5})
        work["requests"] = [request for request in work["requests"] if request["id"].endswith("0")]
        work["requests"][1]["arrival_s"] = .25
        for task in work["tasks"]:
            task["request_ids"] = task["request_ids"][:1]
        clock, calls, cancellations = [1024.0], [], []
        release = asyncio.Event()
        original_wait, original_sleep = asyncio.wait, asyncio.sleep
        waits = 0

        class Backend:
            async def stream(self, request, **options):
                calls.append((request["id"], clock[0] - 1024))
                if request["id"] == "A0":
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        cancellations.append(request["id"])
                        raise
                else:
                    clock[0] += .125
                return response()

        async def wait(active, *, timeout, return_when):
            nonlocal waits
            waits += 1
            await original_sleep(0)
            if waits == 1:
                self.assertEqual(calls, [("A0", 0)])
                self.assertEqual(timeout, .25)
                clock[0] = 1024.25
                return set(), set(active)
            if waits == 2:
                self.assertEqual(calls, [("A0", 0)])
                clock[0] = 1024.5
                release.set()
            return await original_wait(active, return_when=return_when)

        loop = asyncio.get_running_loop()
        with patch.object(loop, "time", side_effect=lambda: clock[0]), \
                patch("serving_lab.scheduler.asyncio.wait", side_effect=wait):
            records, _ = await run(work, Backend(), policy="earliest-deadline", concurrency=1)
        self.assertEqual(calls, [("A0", 0), ("B0", .5)])
        self.assertEqual(cancellations, [])
        self.assertEqual([record["status"] for record in records], ["ok", "ok"])

    async def test_default_plan_stays_three_policies_and_opt_in_runs_alone(self):
        self.assertEqual(POLICIES, ("fcfs", "shortest-input", "aging"))
        for policy, expected in (("all", ["shortest-input", "fcfs", "aging", "aging", "shortest-input", "fcfs"]),
                                 ("earliest-deadline", ["earliest-deadline", "earliest-deadline"])):
            with self.subTest(policy=policy), TemporaryDirectory() as directory:
                args = arguments(directory)
                args.policy = policy
                backend = type("Backend", (), {"close": AsyncMock()})()
                fake_run = AsyncMock(return_value=completed())
                with patch("serving_lab.cli.OpenAIBackend", return_value=backend), \
                        patch("serving_lab.cli.run", fake_run), redirect_stdout(io.StringIO()):
                    code = await experiment(args, "unused", "fake_test")
                report = json.loads((Path(directory) / "report.json").read_text())
                self.assertEqual(code, 0)
                self.assertEqual([step["policy"] for step in report["run_plan"]], expected)
                self.assertEqual([round_["policy"] for round_ in report["runs"]], expected)
                self.assertEqual([call.args[2] for call in fake_run.await_args_list], expected)
                self.assertEqual(report["planned_runs"], len(expected))
                self.assertEqual(report["completed_runs"], len(expected))
                self.assertEqual([step["repeat"] for step in report["run_plan"]],
                                 [0, 0, 0, 1, 1, 1] if policy == "all" else [0, 1])
