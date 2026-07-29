# Changelog

All notable changes will be documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project does not claim semantic-version stability before 1.0.

## [Unreleased]

### Changed

- Reframed the repository as Samsarix Routine Engine, a focused local-first Python workflow component.
- Updated the existing BSL licensor, licensed-work identity, copyright, and licensing contact for Samsarix LLC without changing its production threshold, change date, or change license.
- Isolated the supported product under `src/routine_engine`; archived Helix-era source is excluded from wheels.
- Replaced aspirational and inaccurate documentation with the implemented contract and release gates.
- Hardened archived filesystem containment and changed legacy authorization to fail closed; legacy code and network actions are disabled.

### Added

- Validated acyclic workflow and step models with explicit resource limits.
- Trusted sync/async action registration, bounded concurrency/retries, references, and exact terminal results.
- Atomic local JSON workflow/run persistence with bounded history.
- `routine-engine` CLI for validation, execution, explicit plugins, and a built-in demo.
- Behavioral unit/integration tests, coverage gate, linting, typing, package checks, wheel smoke test, and CI matrix.
- Productization decision record, security policy, API guide, and runnable examples.

[Unreleased]: https://github.com/Deathcharge/routine-engine/compare/v0.1.0...HEAD
