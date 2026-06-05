# Contributing to Astrum

Thank you for your interest in Astrum.

Astrum is still an early-stage project, and community feedback is extremely important. At this stage, I care not only about code contributions, but also about real usage stories, API design feedback, documentation issues, and honest criticism.

If you try Astrum and something feels confusing, too magical, too limited, or badly named, that feedback is very valuable.

## What kind of feedback is helpful?

The most helpful feedback includes:

- Whether the API feels natural for real Python projects.
- Whether the task and dependency model is easy to understand.
- Whether `Ref`, `F`, `depends_on`, and `run` are clear enough.
- Whether error messages are useful.
- Whether the documentation explains the right things.
- Whether the examples match real-world use cases.
- Whether Astrum's scope is clear.
- Whether Astrum should or should not support a specific feature.
- Where Astrum feels better or worse than plain `asyncio.gather`.
- Where Astrum feels too small or too large compared with Airflow, Prefect, or Dagster.

You do not need to submit code to help the project. A clear issue with a real scenario is already a great contribution.

## Project scope

Astrum is an in-process async DAG orchestrator for Python functions.

It is designed for workflows that are more structured than plain scripts or `asyncio.gather`, but too small or too local to require a full workflow platform.

Good use cases include:

- AI Agent workflows.
- RAG pipelines.
- Multi-step async business logic.
- API aggregation.
- File processing pipelines.
- Local automation.
- Small ETL-style workflows.
- Testing and simulation workflows.

Astrum is not intended to replace:

- Airflow.
- Prefect.
- Dagster.
- Celery.
- Distributed job queues.
- Cron systems.
- Persistent workflow engines.
- Cross-machine schedulers.

If you are unsure whether your use case fits Astrum, please open an issue and describe your workflow.

## Reporting bugs

When reporting a bug, please include:

- Astrum version.
- Python version.
- Operating system.
- A minimal reproduction example.
- Expected behavior.
- Actual behavior.
- Full error traceback, if available.

Example:

    astrum version: 0.1.1
    python version: 3.11
    os: macOS / Linux / Windows

    Expected:
    The downstream task should receive the upstream value.

    Actual:
    The task failed with ...

A small runnable example is much more useful than a large project snippet.

## Suggesting API improvements

API feedback is especially welcome.

Astrum's public API is still young, so early feedback can strongly influence the design before it becomes stable.

When suggesting an API change, please try to explain:

- What you tried to build.
- What felt awkward.
- What you expected the API to look like.
- Whether the problem is about naming, typing, behavior, documentation, or missing features.

Example topics:

- Is `depends_on` clear enough?
- Is `Ref` easy to understand?
- Is field selection with `F` intuitive?
- Should task declaration be more explicit or more concise?
- Should execution reports expose more details?
- Should retry behavior be configured differently?

## Documentation contributions

Documentation improvements are highly welcome.

Good documentation contributions include:

- Fixing unclear explanations.
- Improving examples.
- Adding real-world workflows.
- Adding diagrams.
- Adding comparisons with other tools.
- Adding "when not to use Astrum" cases.
- Improving English or Chinese wording.

If you find a confusing paragraph, please open an issue even if you do not have time to fix it.

## Development setup

Clone the repository:

    git clone https://github.com/DuskXi/astrum.git
    cd astrum

Create and activate a virtual environment:

    python -m venv .venv
    source .venv/bin/activate

On Windows PowerShell:

    .venv\Scripts\Activate.ps1

Install the project in editable mode:

    pip install -e .

If the project provides development dependencies, install them as well:

    pip install -e ".[dev]"

Run tests:

    pytest

Build documentation locally, if documentation dependencies are installed:

    mkdocs serve

## Pull request guidelines

Before opening a pull request, please make sure that:

- The change has a clear purpose.
- The change fits Astrum's project scope.
- Existing tests pass.
- New behavior is covered by tests when appropriate.
- Public API changes are explained clearly.
- Documentation is updated if user-facing behavior changes.

For large changes, please open an issue first so we can discuss the direction.

## Commit style

There is no strict commit format yet.

Clear commit messages are preferred:

    fix: handle downstream task failure correctly
    docs: improve quick start example
    test: add retry behavior tests
    refactor: simplify execution report internals

## Version stability

Astrum is currently in the `0.1.x` stage.

The core idea is stable:

- declare tasks;
- declare dependencies;
- run a DAG;
- pass upstream results downstream;
- execute independent branches concurrently;
- return a structured report.

However, some public APIs may still change before `0.2.0` based on real community feedback.

If you are using Astrum in a real project, I would love to hear about it. Your use case can help shape the API.
