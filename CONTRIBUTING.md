# Contributing to frameguard

Thanks for taking the time. This is a small library with a focused job, so
contributions don't need to be big to matter.

## Reporting bugs

Open an issue. Include:
- what you were doing
- the full error message
- your Python, PySpark, and frameguard versions

Schema mismatch bugs are especially welcome — if frameguard raised when it
shouldn't have, or didn't raise when it should, that's exactly the kind of
thing we want to know about.

## Suggesting changes

Open an issue before writing code. A quick description of the problem and
what you have in mind is enough. We'll let you know if it fits the direction
of the library.

## Submitting a PR

1. Fork the repo and create a branch
2. `pip install -e ".[pyspark,dev]"`
3. Write code, write tests
4. `ruff check frameguard/ tests/` and `mypy frameguard/ --ignore-missing-imports`
5. `pytest tests/pyspark/ -q`
6. Open the PR with a short description of what changed and why

Keep PRs focused. One thing at a time.

## Roadmap

PySpark is the first integration. Polars and pandas are next. If you want to
help build those, say so in an issue and we'll coordinate.

The core enforcement mechanism (`_enforcement.py`) is intentionally simple
and dependency-free. New integrations should follow the same principle.
