# Productization decision record

- Last updated: 2026-08-31
- Owner: Samsarix LLC
- Product: Samsarix Routine Engine

## Executive decision

This repository is now shaped as a focused local workflow component, not a visual automation platform and not an AI-agent framework.

**Ideal customer:** a Python team with a small number of application-owned, dependency-aware routines that must be testable and auditable but do not justify deploying an orchestration control plane.

**Job to be done:** run trusted Python operations in a validated DAG, apply bounded retry/concurrency policy, and obtain one exact result for every step from a library call or CI-friendly CLI.

**Wedge:** zero runtime dependencies, no server, explicit action registration, portable JSON definitions, and honest local persistence. The product should take minutes to adopt and be easy to remove.

## What the audit found

The initial repository did not deliver its README contract:

- Its wheel built successfully but contained metadata and no importable product package.
- The README referenced a missing `requirements.txt`, missing documentation, nonexistent CI, an MIT license that contradicted `LICENSE`, and “Production Ready” status.
- The root examples called APIs that did not exist.
- All 27 original tests asserted against `MagicMock`; none imported product code.
- `routine_engine/workflow_engine` was a large extraction coupled to private `apps.backend.*` modules and claimed a visual UI, 500+ integrations, scheduling, and production characteristics that the repository could not independently provide.
- `routine_engine/helix_flow` was a second, separately packaged AI chain/agent concept with provider and platform coupling.
- Security review found same-prefix filesystem containment errors, fail-open optional authorization, and risky legacy network/code execution surfaces. The directly actionable legacy defaults and path checks were hardened, while the entire extracted tree was excluded from the distributable package.

## Product and market reasoning

Prefect already presents Python functions as observable workflows with states, retries, validation, deployments, workers, and a server/cloud ecosystem. Dagster positions itself as a data orchestrator around assets, lineage, observability, and data-platform operations. Competing broadly with either from this repository would be fiction, not productization.

The viable gap is below those systems: deterministic application routines where a control plane is unwanted. Packaging guidance also favors a `src` layout because it prevents repository-root imports from disguising missing package contents and makes tests exercise the installed shape.

Primary references:

- [Prefect flows](https://docs.prefect.io/v3/concepts/flows) and [tasks](https://docs.prefect.io/v3/concepts/tasks)
- [Dagster documentation](https://docs.dagster.io/)
- [PyPA: src layout vs flat layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)
- [PyPA `pyproject.toml` specification](https://packaging.python.org/en/latest/specifications/pyproject-toml/)
- [PyPA command-line tools guide](https://packaging.python.org/en/latest/guides/creating-command-line-tools/)
- [Python 3.9.25 release and end-of-life notice](https://www.python.org/downloads/release/python-3925/)

## Supported 0.2 contract

- Python 3.10–3.13
- `src/routine_engine` only
- zero runtime dependencies
- explicit registration of trusted sync/async Python callables
- validated acyclic JSON/Python definitions
- bounded parallelism, retries, and retry delay
- exact input/prior-output references without expression evaluation
- exact step/run status and timestamps
- optional atomic, bounded local JSON history
- CLI validation, execution, demo, explicit plugins, and meaningful exit codes
- versioned workflow schema, deterministic plans, and per-step local recovery

## Explicitly out of scope

- a hosted service, API server, dashboard, or visual editor
- built-in cron/scheduling, queues, distributed workers, or remote execution
- workflow-authored Python, shell, SQL, filesystem, HTTP, or LLM actions
- automatic plugin discovery or imports selected by workflow data
- secrets management, authentication, tenancy, billing, or RBAC
- promises of exactly-once execution or multi-process transactional persistence
- backward compatibility with extracted Helix module paths

These boundaries are the safety and usability strategy, not a backlog accident.

## Architecture decisions

1. **Use `src` packaging.** Only the coherent public implementation enters wheels. The extracted source remains visible for provenance but cannot accidentally become the product.
2. **No runtime framework dependency.** Dataclasses and explicit validation are enough for the bounded JSON contract.
3. **Functions are trusted; definitions are data.** Users register actions in code or name an explicit CLI plugin. A workflow can never choose an import, command, or code string.
4. **Fail exactly.** Exceptions are captured per attempt, failed dependencies become skipped steps, independent branches proceed, and cancellation propagates.
5. **Persist locally and honestly.** Same-directory temporary files plus `os.replace` protect individual writes. The store documents that different processes need external coordination.
6. **Constrain resource policy.** Step count, concurrency, retries, and delay are validated upper bounds.
7. **Use Python 3.10 as the floor.** Python 3.9 reached end of life in October 2025.

## Security disposition

The supported engine executes only application-registered callables. It contains no evaluator, shell, HTTP client, database driver, filesystem action, provider SDK, API server, or credential store. JSON inputs are structurally bounded, parameters must be finite JSON data, and persisted output must also be JSON-compatible.

Registration remains a privileged boundary: a dangerous registered action is dangerous application code. Routine Engine cannot sandbox it. The legacy tree is not packaged or supported; its network and code actions are disabled, its optional builder authorization fails closed, and filesystem containment uses canonical path components instead of string prefixes.

## Release state and remaining gates

The engineering release candidate includes the library, CLI, examples, behavioral test suite, strict lint/type configuration, coverage threshold, build checks, wheel smoke test, CI matrix, security policy, changelog, and truthful docs.

At the owner's direction on 2026-07-29, the existing BSL parameters were updated to identify Samsarix LLC, Samsarix Routine Engine, the 2026 copyright, and `contact@samsarix.com`. The production-use threshold, June 16, 2027 change date, and Apache License 2.0 change license were preserved. Public package upload and release tagging remain separate owner-authorized actions and are not part of this productization commit.

## 0.2.1 wrap-up audit

Baseline: `e4d1f17` on clean `main`; 48 tests passed with Python 3.11. The new failure-path suite reproduced 13 failing cases before the fixes. Passing the old suite had not established these invariants.

Locally actionable findings and fixes:

| Priority | Finding | Resolution |
| --- | --- | --- |
| P0 | History pruning could discard active checkpoints | Evict only terminal history; reject new runs when active capacity is full |
| P1 | Storage errors leaked asynchronous work after caller failure | Cancel and await outstanding tasks; keep the last durable checkpoint resumable |
| P1 | Direct workflow constructors bypassed graph/resource validation | Revalidate and detach workflow values at every engine entry |
| P1 | Resume could repeat exhausted failures, accept inconsistent checkpoints, or overlap execution | Preserve terminal outcomes; validate dependencies/IDs/types/budgets; same-store run claims |
| P1 | Nested action mutation changed earlier results and retry inputs | Detached per-attempt contexts |
| P1 | CLI JSON was parsed before bounded reads and could raise raw recursion errors | Bounded binary reads and actionable parsing failures |
| P1 | Source archive omitted fixtures/examples/docs required by its own tests | Explicit manifest and extracted-sdist test gate |
| P1 | Ruff basename exclusion also skipped supported source | Root-relative legacy exclusion, verified file selection |
| P2 | Plugin setup and release-state documentation drifted | Correct module invocation, compatibility/publication checklist, updated roadmap |

Acceptance now includes a real child-process exit followed by resume, full source-archive tests in a temporary directory, and wheel-only CLI/schema/consumer execution without checkout imports. CI retains verified artifacts keyed to its commit. Exact wrap-up verification and hashes are recorded in the wrap-up PR; commands are in [RELEASING.md](RELEASING.md).

Remaining gates: explicit package-index publishing authorization/identity and license-policy confirmation, plus production adoption in a consumer-owned environment. No credential, infrastructure, package upload, or license change is needed for local evaluation. Highest-value later work is a real application adapter, then optional persistence backends/timeouts—not a new UI or control plane.
