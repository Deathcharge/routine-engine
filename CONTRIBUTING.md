# Contributing to Samsarix Routine Engine

Thank you for helping keep Routine Engine small, dependable, and honest.

## Setup

```bash
git clone https://github.com/Deathcharge/routine-engine.git
cd routine-engine
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"
```

Use `.venv/bin/python` on macOS or Linux.

## Required checks

```bash
ruff check .
ruff format --check .
mypy src
pytest --cov --cov-report=term-missing
python -m build
python -m twine check dist/*
```

Pull requests should add behavioral tests, update user-facing docs when the contract changes, and avoid expanding the focused scope without a written product rationale. The coverage floor is 90%; new critical paths should be covered regardless of the aggregate number.

Do not add actions that execute workflow-supplied code, commands, paths, SQL, URLs, or provider calls to the built-in set. Application integrations belong behind explicit registration.

Report security concerns privately to [support@samsarix.com](mailto:support@samsarix.com), not in a public issue.
