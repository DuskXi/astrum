# Quickstart

The recommended entry point is decorator mode: register functions with `@task`, declare execution dependencies with `depends_on`, pass upstream values with `Ref` / `F`, and call `run()`.

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
        config=AstrumConfig(skip_type_check=True),
    )
    print(report.execution_state)
    print(f"{report.successful_tasks}/{report.total_tasks} tasks completed")


asyncio.run(main())
```

Here, `load_users` and `load_orders` do not receive parameters, so they can start in the same stage. `build_report` and `publish_report` receive parameters from upstream task results.

## Inspect the plan

Build a scheduler first if you want to inspect the execution timeline before running:

```python
from astrum import AstrumConfig, build_scheduler

scheduler = build_scheduler(
    target_tasks=["publish_report"],
    config=AstrumConfig(skip_type_check=True),
)
plan = scheduler.get_execute_timeline()
print(plan.get_visualization_table())
```

## Read the report

Both `run()` and `DynamicScheduler.execute()` return an `ExecutionReport`:

```python
report = await run(
    target_tasks=["publish_report"],
    config=AstrumConfig(skip_type_check=True),
)

print(report.execution_state)
print(report.total_tasks)
print(report.successful_tasks)
print(report.failed_tasks)
print(report.error_summary)
```
