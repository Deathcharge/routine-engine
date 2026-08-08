# Samsarix Routine Engine roadmap

This roadmap separates repository readiness, package publication, and portfolio adoption. Passing one gate does not imply the next.

## Product boundary

Routine Engine is a reusable, independently versioned SDK for bounded application-owned workflows. It is not a hosted orchestrator. Consumers integrate through the exported Python API, workflow schema v1, CLI behavior, and portable result records—not private repository imports.

## Version 0.2 release candidate

Implemented on the 0.2 branch:

- Explicit workflow schema v1 and a distributable JSON Schema.
- Deterministic execution plans and a CI-friendly `plan` command.
- Bounded definitions, inputs, outputs, JSON depth, errors, retries, and concurrency.
- Non-blocking synchronous actions under bounded local concurrency.
- Atomic run creation, per-step checkpoints, history inspection, and crash-safe resume of successful work.
- State schema v2 with version 1 migration and a 64 MiB cap.
- A release-readiness consumer fixture and evidence-backed competitive boundary.

Release gates still required:

- Build and inspect both sdist and wheel from a clean tree.
- Install the wheel into a clean environment and run API, CLI, schema, and consumer smoke tests.
- Pass the supported Python matrix, lint, formatting, strict typing, branch coverage, dependency audit, and source security scan.
- Record the exact commit, artifact digests, hosted checks, and rollback ref in the release pull request.

## Publication

Publication is a separate owner decision. Before publishing to a package index:

- Confirm package-name ownership and Samsarix LLC publishing credentials.
- Confirm that the Business Source License terms and metadata are the intended commercial policy.
- Tag an immutable version and attach artifact hashes and release notes.
- Document the supported compatibility window and vulnerability response process.

## Portfolio adoption

- Integrate one Samsarix application through workflow schema v1 or the public Python API.
- Keep consumer-owned fixtures covering limits, failures, resume behavior, privacy, and version compatibility.
- Measure whether the engine removes bespoke orchestration code or improves restart/audit behavior.
- Add adapters only when a real consumer proves the need; do not grow into a generic control plane speculatively.

## Candidate follow-ups

- Optional per-step timeouts with clearly documented thread-cancellation limits.
- Pluggable persistence behind a small checkpoint protocol for applications needing database-backed coordination.
- First-class idempotency metadata and application-defined compensation hooks.
- Structured lifecycle events for observability adapters.
- A documented schema/version deprecation policy before 1.0.

## Completion evidence

A milestone is complete only when its exact commit, commands and results, artifact digest, consumer or deployment, and rollback path are recorded. README claims must not exceed that evidence.
