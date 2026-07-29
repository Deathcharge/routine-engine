# Samsarix Routine Engine roadmap

This roadmap separates four gates: merge, release, publication, and flagship adoption. Passing one does not imply the next.

## Product boundary

Portfolio role: **reusable library or sdk**. Keep this as a small, independently versioned package. Samsarix Unified should consume it only through a public API adapter; private monorepo imports and copied implementations are out of scope.

Current disposition: Merge the productization branch after exact-head verification and rollback-ref creation; release and adoption remain separate decisions.

## Stabilize the productized default

- Keep the default branch buildable from a clean checkout and preserve exact-head CI evidence.
- Keep Samsarix LLC branding, package identity, license metadata, and compatibility aliases internally consistent.
- Preserve the pre-productization default under a rollback ref before merging; do not delete legacy history.
- The productization work is now committed, pushed, clean, and green in hosted checks.
- Next: prove one consumer, enforce payload and storage limits, and pin the supported release contract.
- Review priority: Capture dirty work deliberately.
- Review priority: fix license and payload/storage limits.
- Review priority: pin CI.
- Review priority: green tests/build.
- Review priority: prove one consumer or freeze.

## Release candidate

- Build and install the wheel in a clean environment.
- Prove one real consumer and a versioned compatibility fixture.
- Publish only after package-name ownership, licensing, provenance, and rollback are recorded.

Current hardening backlog:

- The entire candidate is uncommitted and absent remotely.
- No total workflow-file, nesting, persisted-record, output, or error-message size cap.
- Sync actions can block the event loop; actions have no timeout, isolation, idempotency key, compensation, or durable queue.
- Local JSON persistence is single-process and may retain secrets in plaintext.
- The market already has many DAG/workflow libraries; differentiation is weak without a portfolio consumer.
- License metadata is stale and internally risky: the BSL names Helix Collective and “Helix Licensing System,” has no Additional Use Grant, and does not clearly identify this package.
- Untracked CI and all static implementation claims remain unverified.

## Samsarix adoption

- Define a public API, event, schema, artifact, or deployment contract before connecting to Samsarix Unified.
- Add a consumer-owned contract fixture covering authentication, privacy, limits, errors, and version compatibility.
- Make one implementation canonical; remove or freeze duplicate behavior only after parity and rollback are proven.
- Record an owner, support level, compatibility window, and measurable adoption signal.

## Completion evidence

A milestone is complete only when its exact commit, commands and results, artifact digest, consumer or deployment, and rollback path are recorded in a pull request or release record. README claims must not exceed that evidence.
