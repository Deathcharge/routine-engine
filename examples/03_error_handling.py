"""Show retries, failure, and downstream skip semantics."""

from routine_engine import ActionContext, RoutineEngine


def unreliable(context: ActionContext) -> str:
    if context.attempt < 3:
        raise RuntimeError("temporary failure")
    return "recovered"


def always_fails(_: ActionContext) -> None:
    raise ValueError("invalid source data")


engine = RoutineEngine()
engine.register("unreliable", unreliable)
engine.register("always_fails", always_fails)
engine.register("publish", lambda context: context.params["value"])

result = engine.run(
    {
        "id": "error-example",
        "steps": [
            {"id": "retry", "action": "unreliable", "retries": 2},
            {"id": "failure", "action": "always_fails"},
            {
                "id": "blocked",
                "action": "publish",
                "needs": ["failure"],
                "with": {"value": "never reached"},
            },
        ],
    }
)

for step in result.steps.values():
    print(step.step_id, step.status.value, step.attempts, step.error)
