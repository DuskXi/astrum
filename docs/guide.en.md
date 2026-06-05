# Guide

This page introduces Astrum through two questions: how to organize tasks, and how to pass data between them. The examples are intentionally small so you can copy and adapt them.

## Module-level `@task`

The most direct style registers functions in the default namespace and calls `run()`.

```python
from astrum import task, run


@task("extract")
async def extract() -> dict:
    return {"rows": 100}


@task("load", depends_on=["extract"])
async def load() -> None:
    print("loaded")


report = await run(target_tasks=["load"])
```

## Direct `namespace=...`

In real projects, give each workflow a namespace to avoid task name collisions across modules.

```python
from astrum import task, run, clear_registry


@task("extract", namespace="daily_report")
async def extract() -> None:
    ...


@task("load", depends_on=["extract"], namespace="daily_report")
async def load() -> None:
    ...


report = await run(target_tasks=["load"], namespace="daily_report")
clear_registry("daily_report")
```

## `with use_namespace`

If a group of tasks belongs to the same namespace, use the context manager to avoid repeating `namespace=`.

```python
from astrum import task, run, use_namespace


with use_namespace("analytics"):

    @task("load_csv")
    async def load_csv() -> None:
        ...

    @task("clean_csv", depends_on=["load_csv"])
    async def clean_csv() -> None:
        ...


report = await run(target_tasks=["clean_csv"], namespace="analytics")
```

## `SchedulerRegistry`

Create an explicit registry when you do not want to rely on the global registry. This style is useful for libraries, tests, and code that packages multiple workflows.

```python
from astrum import SchedulerRegistry


workflow = SchedulerRegistry("billing")


@workflow.task("fetch_invoice")
async def fetch_invoice() -> dict:
    return {"invoice_id": "INV-001"}


@workflow.task("send_invoice", depends_on=["fetch_invoice"])
async def send_invoice() -> None:
    print("sent")


report = await workflow.run(target_tasks=["send_invoice"])
```

## Manual DAGs

Decorator mode fits most application code. If you want to separate task functions from graph structure, create `TaskOrder` objects and pass them to `DynamicScheduler`.

```python
from astrum import DynamicScheduler, TaskOrder


async def extract() -> None:
    ...


async def load() -> None:
    ...


extract_order = TaskOrder("extract")
load_order = TaskOrder("load", dependencies=[extract_order])

scheduler = DynamicScheduler(
    tasks=[("extract", extract), ("load", load)],
    task_order=[extract_order, load_order],
)

report = await scheduler.execute()
```

Pass function references such as `extract`, not already-created coroutine objects such as `extract()`.

## Annotation-driven data transport

Prefer `Ref` and `F` for declaring where arguments come from. `F("task", "field")` reads a field from an upstream task result and injects it into the current parameter.

```python
from astrum import F, Ref, task, run, AstrumConfig


@task("load_order")
async def load_order() -> dict:
    return {"order_id": "A-001", "amount": 128}


@task("format_order")
async def format_order(
    order_id: Ref[str, F("load_order", "order_id")],
    amount: Ref[int, F("load_order", "amount")],
) -> dict:
    return {"message": f"order {order_id}: ${amount}"}


report = await run(
    target_tasks=["format_order"],
    config=AstrumConfig(skip_type_check=True),
)
```

Common locators:

- `F("source", "name")`: read a dict key or object attribute.
- `F("source", 0)`: read a list/tuple index.
- `F("source")`: pass the whole upstream result.

## Explicit `TaskData` data transport

Use explicit `TaskData` when data relationships are generated dynamically, or when you need precise control over sources, targets, and type constraints.

```python
from astrum import AstrumConfig, run, task
from astrum.data_transport import DTRela, DataItem, TaskData


@task("load_user", data=TaskData())
async def load_user() -> dict:
    return {"name": "Alice"}


@task(
    "greet",
    data=TaskData(
        input_data_item=[
            DataItem(
                allow_data_model=str,
                from_relation=DTRela(key="name", related_task="load_user"),
                to_relation=DTRela(key="name", related_task="greet"),
            )
        ]
    ),
)
async def greet(name: str) -> None:
    print(f"hello {name}")


report = await run(
    target_tasks=["greet"],
    config=AstrumConfig(skip_type_check=True),
)
```

For day-to-day application code, prefer annotation-driven transport. Explicit `TaskData` is better for framework integration, legacy migration, and dynamic DAGs.

## Inspect the execution plan

`build_scheduler()` builds a scheduler without running it. You can inspect the stage plan first, then call `execute()`.

```python
from astrum import build_scheduler


scheduler = build_scheduler(target_tasks=["greet"])
plan = scheduler.get_execute_timeline()
print(plan.get_visualization_table())

report = await scheduler.execute()
```

## Retries and failures

`@task(..., retry=N)` retries a failed task up to N times. If a task ultimately fails, Astrum cancels parallel work that should no longer continue and records the reason in `ExecutionReport.error_summary`.

```python
@task("flaky", retry=2)
async def flaky() -> None:
    ...


report = await run(target_tasks=["flaky"])
for stat in report.task_statistics:
    print(stat.task_name, stat.status, stat.attempt_count)
```

## Configuration

`AstrumConfig` controls scheduler behavior in one object.

```python
from astrum import AstrumConfig, run


report = await run(
    target_tasks=["greet"],
    config=AstrumConfig(
        concurrency_limit=4,
        silence=True,
        visualize=False,
        skip_type_check=True,
    ),
)
```

