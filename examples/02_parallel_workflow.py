"""Run independent asynchronous actions concurrently."""

import asyncio

from routine_engine import ActionContext, RoutineEngine


async def inspect_region(context: ActionContext) -> dict[str, str]:
    await asyncio.sleep(0.05)
    return {"region": str(context.params["region"]), "status": "ready"}


async def main() -> None:
    engine = RoutineEngine()
    engine.register("inspect_region", inspect_region)
    result = await engine.arun(
        {
            "id": "parallel-example",
            "max_concurrency": 2,
            "steps": [
                {"id": "east", "action": "inspect_region", "with": {"region": "east"}},
                {"id": "west", "action": "inspect_region", "with": {"region": "west"}},
            ],
        }
    )
    print(result.to_dict())


asyncio.run(main())
