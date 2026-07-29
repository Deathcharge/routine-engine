"""Register two application actions and run a dependency-aware workflow."""

from routine_engine import ActionContext, RoutineEngine


def normalize(context: ActionContext) -> str:
    return str(context.params["name"]).strip().title()


def greet(context: ActionContext) -> str:
    return f"Hello, {context.params['name']}!"


engine = RoutineEngine()
engine.register("normalize", normalize)
engine.register("greet", greet)

result = engine.run(
    {
        "id": "basic-example",
        "steps": [
            {"id": "name", "action": "normalize", "with": {"name": "{{ input.name }}"}},
            {
                "id": "message",
                "action": "greet",
                "needs": ["name"],
                "with": {"name": "{{ steps.name.output }}"},
            },
        ],
    },
    {"name": "  ada  "},
)

print(result.steps["message"].output)
