# Astrum Usage Guide

Astrum is a lightweight, in-process async DAG orchestrator. It organizes regular Python functions into dependency-aware workflows, runs independent branches concurrently, and returns a structured execution report.

This guide is for users who want to use Astrum directly in application code. It covers decorators, namespaces, registries, manual DAGs, data transport, retries, visualization, and troubleshooting.

## Installation

```bash
pip install astrum
```

For Rich-powered terminal visualization:

```bash
pip install "astrum[viz]"
```

## When to Use Astrum

Astrum is a good fit when:

- You want local workflow orchestration inside one Python process.
- Your sync or async functions form a DAG.
- Some tasks can run in parallel, while others must wait for upstream results.
- You need execution state, timing, failures, cancellations, and retry records.
- You want upstream task results to be injected into downstream function parameters.

Astrum is not a distributed workflow engine. If you need cross-machine scheduling, persistent queues, a cron platform, or long-running distributed workflows, use a heavier workflow or queue system.

## Quick Start

The recommended entry point is decorator mode. This example has 4 tasks: two parameter-free tasks run in parallel, and two downstream tasks receive values with `Ref/F`.

```python
import asyncio

from astrum import F, AstrumConfig, Ref, run, task


@task("load_users")
async def load_users() -> dict:
    await asyncio.sleep(0.1)
    return {"users": ["Alice", "Bob"]}


@task("load_orders")
async def load_orders() -> dict:
    await asyncio.sleep(0.1)
    return {"orders": ["A-001", "A-002", "A-003"]}


@task("build_report", depends_on=["load_users", "load_orders"])
async def build_report(
    users: Ref[list, F("load_users", "users")],
    orders: Ref[list, F("load_orders", "orders")],
) -> dict:
    return {"summary": f"{len(users)} users, {len(orders)} orders"}


@task("publish_report", depends_on=["build_report"])
async def publish_report(
    summary: Ref[str, F("build_report", "summary")],
) -> None:
    print(summary)


async def main() -> None:
    report = await run(
        target_tasks=["publish_report"],
        config=AstrumConfig(skip_type_check=True, silence_warnings=True),
    )
    print(report.execution_state)
    print(f"{report.successful_tasks}/{report.total_tasks} tasks completed")


asyncio.run(main())
```

Output:

```text
2 users, 3 orders
completed
4/4 tasks completed
```

## Ways to Organize Tasks

### Module-level `@task`

The most direct style registers tasks in the default namespace:

```python
from astrum import run, task


@task("extract")
async def extract() -> dict:
    return {"rows": 100}


@task("load", depends_on=["extract"])
async def load() -> None:
    print("loaded")


report = await run(target_tasks=["load"])
```

### Direct `namespace=...`

In real projects, give each workflow a namespace to avoid task name collisions across modules.

```python
from astrum import clear_registry, run, task


@task("extract", namespace="daily_report")
async def extract() -> None:
    ...


@task("load", depends_on=["extract"], namespace="daily_report")
async def load() -> None:
    ...


report = await run(target_tasks=["load"], namespace="daily_report")
clear_registry("daily_report")
```

### `with use_namespace`

If a group of tasks belongs to the same namespace, use the context manager to avoid repeating `namespace=`.

```python
from astrum import run, task, use_namespace


with use_namespace("analytics"):

    @task("load_csv")
    async def load_csv() -> None:
        ...

    @task("clean_csv", depends_on=["load_csv"])
    async def clean_csv() -> None:
        ...


report = await run(target_tasks=["clean_csv"], namespace="analytics")
```

### `SchedulerRegistry`

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

### Manual DAGs

If you want to separate task functions from graph structure, create `TaskOrder` objects and pass them to `DynamicScheduler`.

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

## Data Transport

### Annotation-driven `Ref/F`

Prefer `Ref` and `F` for declaring where arguments come from. `F("task", "field")` reads a field from an upstream task result and injects it into the current parameter.

```python
from astrum import F, Ref, task


@task("load_order")
async def load_order() -> dict:
    return {"order_id": "A-001", "amount": 128}


@task("format_order")
async def format_order(
    order_id: Ref[str, F("load_order", "order_id")],
    amount: Ref[int, F("load_order", "amount")],
) -> dict:
    return {"message": f"order {order_id}: ${amount}"}
```

Common locators:

- `F("source", "name")`: read a dict key or object attribute.
- `F("source", 0)`: read a list/tuple index.
- `F("source")`: pass the whole upstream result.

`F` can also refer to task functions directly:

```python
@task("load_user")
async def load_user() -> dict:
    return {"name": "Alice"}


@task("greet")
async def greet(name: Ref[str, F(load_user, "name")]) -> None:
    print(f"hello {name}")
```

If the same function object is registered under multiple task IDs, callable references become ambiguous. Use string task IDs in that case.

### Return-side `T`

Downstream `F` annotations cover most cases. If you want an upstream return annotation to declare the target, use `T`.

```python
from astrum import Ref, T, task


@task("source")
async def source() -> Ref[dict, T("target", "local_name", "remote_name")]:
    return {"local_name": "Alice"}


@task("target")
async def target(remote_name: str) -> None:
    print(remote_name)
```

### Explicit `TaskData`

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
    config=AstrumConfig(skip_type_check=True, silence_warnings=True),
)
```

For day-to-day application code, prefer annotation-driven transport. Explicit `TaskData` is better for framework integration, legacy migration, and dynamic DAGs.

## Inspect the Execution Plan

`build_scheduler()` builds a scheduler without running it. You can inspect the stage plan first, then call `execute()`.

```python
from astrum import AstrumConfig, build_scheduler


scheduler = build_scheduler(
    target_tasks=["greet"],
    config=AstrumConfig(skip_type_check=True, silence_warnings=True),
)
plan = scheduler.get_execute_timeline()
print(plan.get_visualization_table())

report = await scheduler.execute()
```

Useful `ExecutionPlan` methods:

- `get_visualization_table()`: returns a text stage table.
- `get_dependency_graph_info()`: returns dependency graph information.
- `get_full_visualization()`: returns the full text view.

## Read the Execution Report

Both `run()` and `scheduler.execute()` return `ExecutionReport`.

```python
print(report.execution_state)
print(report.total_tasks)
print(report.successful_tasks)
print(report.failed_tasks)
print(report.total_duration)
print(report.error_summary)
print(report.original_tasks)
```

Per-task statistics:

```python
for stat in report.task_statistics:
    print(stat.task_name, stat.status, stat.duration, stat.attempt_count)
```

## Retries and Failures

`@task(..., retry=N)` retries a failed task up to N times.

```python
from astrum import run, task


attempts = 0


@task("flaky", retry=2)
async def flaky() -> None:
    global attempts
    attempts += 1
    if attempts < 3:
        raise RuntimeError("temporary failure")


report = await run(target_tasks=["flaky"])
for stat in report.task_statistics:
    print(stat.task_name, stat.status, stat.attempt_count)
```

If a task ultimately fails, Astrum stops downstream dependent tasks and cancels parallel work that should no longer continue. Failure details are recorded in `report.error_summary` and each task statistic's `error_message`.

## Configuration

`AstrumConfig` controls scheduler behavior in one object.

```python
from astrum import AstrumConfig


config = AstrumConfig(
    concurrency_limit=4,
    silence=True,
    visualize=False,
    skip_type_check=True,
    infer_via_ast=False,
    strict_topology=False,
    silence_warnings=True,
)
```

Common options:

- `concurrency_limit`: global concurrency limit. `None` means unlimited.
- `silence`: suppress execution-time logs.
- `visualize`: print Rich DAG visualization after scheduler construction. Requires `astrum[viz]`.
- `skip_type_check`: skip data transport type checks.
- `infer_via_ast`: try AST return-type inference when annotations are missing.
- `strict_topology`: strictly validate data relations against explicit topology declarations.
- `silence_warnings`: suppress automatic data relation completion warnings.
- `ignore_tail_task`: tail tasks that should not be awaited at the end.

## Visualization

After installing `astrum[viz]`, enable visualization:

```python
from astrum import AstrumConfig, build_scheduler


scheduler = build_scheduler(config=AstrumConfig(visualize=True))
```

Visualization prints a DAG tree and Data Transport Matrix during scheduler construction. It is useful for debugging complex data flows.

## Troubleshooting

### Passing an already-created coroutine object

Wrong:

```python
DynamicScheduler(
    tasks=[("load", load())],
    task_order=[TaskOrder("load")],
)
```

Correct:

```python
DynamicScheduler(
    tasks=[("load", load)],
    task_order=[TaskOrder("load")],
)
```

### Duplicate task names

Task IDs must be unique inside one namespace. In tests, notebooks, or hot-reload flows, clear the namespace first:

```python
from astrum import clear_registry

clear_registry("my_namespace")
```

### Data transport cannot find a field

`F("source", "key")` expects the upstream result to be a dict or an object with that attribute. Use an integer index for tuple/list results, or omit the locator to pass the whole result.

### Visualization prints nothing

Install the optional dependency and set `visualize=True`:

```bash
pip install "astrum[viz]"
```

## Best Practices

- Prefer decorator mode: start with `@task`, `depends_on`, and `run()`.
- Use a dedicated `namespace` for each real workflow.
- Use `clear_registry()` in tests to isolate the global registry.
- Use stable, readable task IDs such as `load_orders` and `build_report`.
- Use `depends_on` for execution dependencies and `Ref/F` for data dependencies.
- Add parameter and return type annotations for functions that participate in data transport.
- Pass function references to `DynamicScheduler`; do not call the functions early.
- Start with a simple DAG, verify the report, then add data transport, visualization, and stricter type checks.

## API Cheat Sheet

Common root imports:

```python
from astrum import (
    AstrumConfig,
    task,
    run,
    build_scheduler,
    build_task_orders,
    use_namespace,
    clear_registry,
    SchedulerRegistry,
    DynamicScheduler,
    TaskOrder,
    Ref,
    F,
    T,
)
```

Advanced data transport objects:

```python
from astrum.data_transport import TaskData, DataItem, DTRela
```

