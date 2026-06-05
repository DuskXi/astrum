# Astrum

Astrum is a lightweight, in-process async DAG orchestration library for Python. It turns regular sync or async functions into dependency-aware workflows, runs independent branches concurrently, and returns a structured execution report.

```python
import asyncio
from astrum import F, Ref, task, run, AstrumConfig

@task("load")
async def load() -> dict:
    return {"value": 42}

@task("print_value")
async def print_value(value: Ref[int, F("load", "value")]) -> None:
    print(value)

asyncio.run(run(
    target_tasks=["print_value"],
    config=AstrumConfig(skip_type_check=True),
))
```

## When to use it

- You want local workflow orchestration inside one Python process.
- Your tasks form a DAG and independent branches should run concurrently.
- You need execution state, timing, failure, cancellation, and retry reports.
- You want to inject upstream return values into downstream parameters with `Ref` / `F` annotations.

## Installation

```bash
pip install astrum
```

For Rich-powered terminal visualization:

```bash
pip install "astrum[viz]"
```

## Next steps

- Start with the [Quickstart](quickstart.md).
- Read the [Guide](guide.md) for namespaces, manual DAGs, data transport, and retries.
- Use the [External API Reference](api/external/index.md) for user-facing public API details.
