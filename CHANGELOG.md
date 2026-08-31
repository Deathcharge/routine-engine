# Changelog

All notable changes will be documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project does not claim semantic-version stability before 1.0.

## [Unreleased]

## 0.2.1 - 2026-08-31

### Fixed

- Revalidate public workflow objects, reject non-object inputs and non-integer schema versions, and bound CLI reads before JSON parsing.
- Preserve active history and reject duplicate IDs and overlapping same-store execution.
- Validate checkpoint structure/dependencies and preserve terminal failures across process recovery instead of resetting retry budgets.
- Cancel and await async actions on persistence failures; detach nested action context per attempt.
- Resolve whitespace-wrapped references consistently and keep malformed exception messages within the step-error contract.
- Include test fixtures, schemas, examples, and documentation in source distributions; verify source archives and installed wheels outside the checkout.
- Correct the Ruff archive exclusion so supported source code is actually linted; pin CI action revisions and retain verified artifacts.
- Document recovery limitations, plugin import setup, compatibility, and owner-controlled publication gates.

## [0.2.0] - 2026-08-08

### Changed

- Reframed the repository as Samsarix Routine Engine, a focused local-first Python workflow component.
- Updated the existing BSL licensor, licensed-work identity, copyright, and licensing contact for Samsarix LLC without changing its production threshold, change date, or change license.
- Isolated the supported product under `src/routine_engine`; archived Helix-era source is excluded from wheels.
- Replaced aspirational and inaccurate documentation with the implemented contract and release gates.
- Hardened archived filesystem containment and changed legacy authorization to fail closed; legacy code and network actions are disabled.

### Added

- Deterministic execution plans through `RoutineEngine.plan()` and the `routine-engine plan` command.
- A distributable workflow v1 JSON Schema and `routine-engine schema` discovery command.
- Atomic per-step checkpoints and `resume()` / `aresume()` recovery that reuses successful work.
- `routine-engine history`, `show`, and `resume` commands for persisted local runs.
- An explicit workflow `schema_version` contract, currently pinned to version 1.
- Validated acyclic workflow and step models with explicit resource limits.
- Trusted sync/async action registration, bounded concurrency/retries, references, and exact terminal results.
- Atomic local JSON workflow/run persistence with bounded history.
- `routine-engine` CLI for validation, execution, explicit plugins, and a built-in demo.
- Behavioral unit/integration tests, coverage gate, linting, typing, package checks, wheel smoke test, and CI matrix.
- Productization decision record, security policy, API guide, and runnable examples.
- A tested release-readiness consumer fixture plus use-case and competitive-position guidance.

### Security

- Bounded workflow definitions, run inputs, step outputs, JSON nesting, and persisted error text.
- Synchronous actions now run in bounded worker threads so they cannot serialize or block the async scheduler.
- State storage is capped at 64 MiB and migrates version 1 history forward to the resumable version 2 format.

[Unreleased]: https://github.com/Deathcharge/routine-engine/commits/main
[0.2.0]: https://github.com/Deathcharge/routine-engine/pull/2
