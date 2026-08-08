"""Reproducible local scheduler benchmark; not a CI timing gate."""

from __future__ import annotations

import argparse
import statistics
import time

from routine_engine import RoutineEngine


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--steps", type=int, default=256, choices=range(1, 257))
    parser.add_argument("--runs", type=int, default=20)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")

    engine = RoutineEngine()
    engine.register("noop", lambda _: None)
    workflow = {
        "id": "scheduler-benchmark",
        "max_concurrency": 32,
        "steps": [{"id": f"step-{index}", "action": "noop"} for index in range(args.steps)],
    }
    engine.plan(workflow)
    durations = []
    for _ in range(args.runs):
        started = time.perf_counter()
        result = engine.run(workflow)
        assert result.succeeded
        durations.append(time.perf_counter() - started)

    median = statistics.median(durations)
    p95 = sorted(durations)[max(0, int(len(durations) * 0.95) - 1)]
    print(f"steps={args.steps} runs={args.runs} median_ms={median * 1000:.2f} p95_ms={p95 * 1000:.2f}")


if __name__ == "__main__":
    main()
