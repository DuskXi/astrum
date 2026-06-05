# 快速开始

最推荐的入口是装饰器模式：用 `@task` 注册任务，用 `depends_on` 声明执行依赖，用 `Ref` / `F` 把上游返回值传给下游参数，然后调用 `run()`。

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

在这个例子中，`load_users` 和 `load_orders` 不接收参数，会在同一个阶段并行启动；`build_report` 和 `publish_report` 接收来自上游任务的参数。

## 查看计划

如果想在执行前查看 DAG 会被拆成几个阶段，可以先构建 scheduler：

```python
from astrum import AstrumConfig, build_scheduler

scheduler = build_scheduler(
    target_tasks=["publish_report"],
    config=AstrumConfig(skip_type_check=True),
)
plan = scheduler.get_execute_timeline()
print(plan.get_visualization_table())
```

## 读取报告

`run()` 和 `DynamicScheduler.execute()` 都返回 `ExecutionReport`：

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
