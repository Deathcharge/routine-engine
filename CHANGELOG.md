# Changelog

All notable changes will be documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project does not claim semantic-version stability before 1.0.

## [Unreleased]

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

[Unreleased]: https://github.com/Deathcharge/routine-engine/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Deathcharge/routine-engine/compare/v0.1.0...v0.2.0
