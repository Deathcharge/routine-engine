# Competitive position

Research date: 2026-08-08. This is a product-boundary decision, not a claim that Routine Engine replaces a general orchestrator.

## What established products cover

- [Prefect tasks](https://docs.prefect.io/v3/concepts/tasks) provide tracked state, retries, caching, timeouts, parallelism, and transactional semantics as part of a larger orchestration platform.
- [Temporal workflow execution](https://docs.temporal.io/workflow-execution) uses durable event history and deterministic replay for reliable, long-running distributed applications.
- [Dagster sensors](https://docs.dagster.io/guides/automate/sensors) sit in an automation platform with schedules, asset events, sensors, and a web UI.
- [Hamilton](https://hamilton.apache.org/concepts/) models Python functions as dataflow nodes and offers caching, visualization, dynamic execution, and broad integrations.
- [redun](https://insitro.github.io/redun/) provides lazy DAG construction, caching, provenance, and multiple compute backends.
- [doit](https://pydoit.org/) focuses on Python-defined task automation and incremental file-dependency builds.
- [Pydra](https://nipype.github.io/pydra/) focuses on reproducible, concurrent scientific dataflows with caching and environment support.
- [Dagu](https://docs.dagu.cloud/) offers a broad local-first orchestrator with scheduling, queues, approvals, a UI, and operational integrations.

## Chosen wedge

Competing feature-for-feature would create an immature copy of mature platforms. Routine Engine instead targets a narrower integration problem: an application needs a versioned workflow contract and durable local execution, but should not need to deploy or adopt an orchestration control plane.

The differentiators are therefore:

1. Zero runtime dependencies and an intentionally small public Python surface.
2. Workflow-supplied data cannot import code, evaluate expressions, or execute shell text.
3. Explicit, enforceable limits on definitions, inputs, outputs, errors, retries, and concurrency.
4. Deterministic plans and portable JSON results suited to CI, APIs, and contract fixtures.
5. Atomic local checkpoints with resumable application-owned runs.
6. A versioned JSON Schema that other systems can validate without importing Python.

## Deliberate non-goals

Routine Engine will not add a scheduler, hosted UI, distributed worker protocol, secrets service, asset catalog, or workflow-supplied plugin loader merely to match comparison tables. Those capabilities would change its operational and security model. A consumer needing them should adopt an established orchestrator or build a narrow adapter around Routine Engine's versioned contract.

## Next evidence

The product should earn additional scope through real consumers. The next decision gate is an external Samsarix application using workflow schema v1 and recording measurable value such as reduced bespoke orchestration code, restart recovery, or auditable release evidence.
