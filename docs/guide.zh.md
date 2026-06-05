# 使用指南

这页按“怎么组织任务”和“怎么传数据”两条线介绍 Astrum 的常见用法。例子都保持最小化，方便复制到自己的项目里改。

## 模块级 `@task`

最直接的写法是把函数注册到默认命名空间，然后调用 `run()`。

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

## 直接指定 namespace

真实项目中建议给每条工作流指定命名空间，避免不同模块里的任务名冲突。

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

## 使用 `with use_namespace`

如果一组任务都属于同一个命名空间，可以用上下文管理器少写重复参数。

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

## 使用 `SchedulerRegistry`

如果你不想使用全局注册表，可以显式创建一个注册器对象。这个写法适合库代码、测试和需要封装多个工作流的场景。

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

## 手动 DAG

装饰器模式适合大多数业务代码。若你想把任务函数和 DAG 结构完全分开，可以直接创建 `TaskOrder` 并传给 `DynamicScheduler`。

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

传给 `tasks` 的必须是函数引用，例如 `extract`，不要传已经调用后的协程对象 `extract()`。

## 注解式数据传输

推荐使用 `Ref` 和 `F` 声明参数来源。`F("task", "field")` 表示从上游任务返回值中读取字段并注入当前参数。

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

常见定位方式：

- `F("source", "name")`：从 dict key 或对象属性读取。
- `F("source", 0)`：从 list/tuple index 读取。
- `F("source")`：传递整个上游返回值。

## 显式 `TaskData` 数据传输

当数据关系需要动态生成，或你希望精确控制来源、去向和类型约束时，可以显式声明 `TaskData`。

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

日常业务代码优先使用注解式写法；显式 `TaskData` 更适合框架集成、旧代码迁移和动态 DAG。

## 查看执行计划

`build_scheduler()` 会构建 scheduler，但不会立即执行。你可以先查看阶段计划，再调用 `execute()`。

```python
from astrum import build_scheduler


scheduler = build_scheduler(target_tasks=["greet"])
plan = scheduler.get_execute_timeline()
print(plan.get_visualization_table())

report = await scheduler.execute()
```

## 重试和失败

`@task(..., retry=N)` 表示失败后最多重试 N 次。最终失败时，Astrum 会取消仍在运行且不应继续等待的并行任务，并把原因写入 `ExecutionReport.error_summary`。

```python
@task("flaky", retry=2)
async def flaky() -> None:
    ...


report = await run(target_tasks=["flaky"])
for stat in report.task_statistics:
    print(stat.task_name, stat.status, stat.attempt_count)
```

## 配置

`AstrumConfig` 集中控制调度器行为。

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

