# Release and compatibility checklist

Samsarix Routine Engine is a local Python library/CLI, not a deployed service. A green repository is not proof of production adoption or permission to upload a package.

## Reproducible acceptance procedure

Use a clean checkout and an activated Python 3.10–3.13 virtual environment:

```bash
python -m pip install -e ".[dev]"
python -m ruff check .
python -m ruff format --check .
python -m mypy src
python -m pytest --cov --cov-report=term-missing
python -m build --outdir dist/0.2.1
python -m twine check dist/0.2.1/*
python scripts/verify_release.py --dist dist/0.2.1
```

The verifier rejects missing/ambiguous artifacts, checks the typed API/schema and absence of archived source, extracts the sdist safely into a temporary directory, runs its full tests, installs the wheel with no dependencies into a separate environment, and exercises CLI/schema/consumer persistence without checkout imports. It prints artifact SHA-256 hashes. Use a new output directory for subsequent versions.

CI repeats the quality checks on Python 3.13 and tests Python 3.10–3.13 on Ubuntu and Windows. Its verified wheel/sdist artifacts are retained for 14 days under the exact workflow commit. The explicit process-exit recovery regression proves successful checkpoints survive a stopped process; it does not simulate power loss or arbitrary filesystem failures.

Before merging: inspect exact-head checks and actionable review feedback, record hashes in the PR, and retain the prior main commit as the rollback point. Do not use force pushes or bypass failing checks.

## Compatibility promise

- Supported import surface: exports from `routine_engine`; archived monorepo modules are not APIs.
- Python test matrix: 3.10–3.13. Other versions are not yet part of the support promise.
- Workflow schema v1: defaults are normalized, unknown fields are rejected, graph/reference checks supplement JSON Schema. The Python API additionally requires an integer schema-version value (not a float or boolean).
- State schema v2: v1 history remains readable and migrates on the next write, but old runs without snapshots cannot be resumed. Back up state before upgrading; downgrading a migrated state file is not supported.
- The latest 0.2.x candidate receives best-effort fixes. Before 1.0, public changes must be described in the changelog; incompatible workflow-shape changes require a new schema version.
- Reuse terminal checkpoints only with the same compatible trusted action code. Resume is at-least-once for uncheckpointed side effects. Use run/step IDs as application idempotency keys and never persist credentials.
- CLI: exit 0 for success, 1 for a completed failed/cancelled run, and 2 for validation/configuration/storage errors. Stdout carries results; stderr carries diagnostics.

## Owner-controlled publication gates

No package upload or release tag is created by the wrap-up patch. Before publication, the owner must:

1. Confirm ownership of `samsarix-routine-engine` in the intended package index and configure a trusted publishing identity or an externally supplied, secret-scoped credential. Never commit an API token.
2. Confirm the existing Business Source License terms and commercial-use policy. This patch does not change them.
3. Explicitly authorize the version tag, target index, and package upload; publish only artifacts from the approved green commit.
4. Download and install the published wheel in a new environment, run the demo/schema checks, and compare its SHA-256 against the approved artifact.

Production integration into another Samsarix repository is also separate work. Consumer authorization, idempotency, state file permissions, redaction, and operational recovery must be validated in that application's environment.
