# Astrum 使用指南

Astrum 是一个轻量、进程内运行的 async DAG 编排库。它把普通 Python 函数组织成有依赖关系的工作流，让无依赖分支并行执行，并在结束后返回结构化执行报告。

这份指南面向想在业务代码里直接使用 Astrum 的用户，覆盖装饰器、命名空间、注册表、手动 DAG、数据传输、重试、可视化和常见排查。

## 安装

```bash
pip install astrum
```

如果需要 Rich 终端可视化：

```bash
pip install "astrum[viz]"
```

## 什么时候使用 Astrum

Astrum 适合这些场景：

- 你希望在单个 Python 进程内编排本地工作流。
- 你有一组同步或异步函数，需要按 DAG 依赖执行。
- 一部分任务可以并行，一部分任务必须等待上游结果。
- 你希望拿到执行状态、耗时、失败原因、取消状态和重试记录。
- 你希望上游任务返回值自动注入下游任务参数。

如果你需要跨机器调度、持久化队列、定时任务平台或长期运行的分布式工作流，Astrum 当前不是这类系统的替代品。

## 快速开始

推荐从模块级装饰器开始。下面的例子有 4 个任务：前两个任务无参数并行执行，后两个任务通过 `Ref/F` 接收上游数据。

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

输出：

```text
2 users, 3 orders
completed
4/4 tasks completed
```

## 组织任务的几种方式

### 模块级 `@task`

最直接的写法是把任务注册到默认命名空间：

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

### 直接指定 `namespace`

真实项目中建议给每条工作流指定命名空间，避免不同模块里的任务名冲突。

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

### 使用 `with use_namespace`

如果一组任务都属于同一个命名空间，可以用上下文管理器减少重复参数。

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

### 使用 `SchedulerRegistry`

如果你不想依赖全局注册表，可以显式创建注册器对象。这个写法适合库代码、测试和封装多个工作流的场景。

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

### 手动构建 DAG

如果你想把任务函数和 DAG 结构完全分开，可以直接创建 `TaskOrder` 并传给 `DynamicScheduler`。

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

## 数据传输

### 注解式 `Ref/F`

推荐使用 `Ref` 和 `F` 声明参数来源。`F("task", "field")` 表示从上游任务返回值中读取字段并注入当前参数。

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

常见定位方式：

- `F("source", "name")`：从 dict key 或对象属性读取。
- `F("source", 0)`：从 list/tuple index 读取。
- `F("source")`：传递整个上游返回值。

`F` 也支持传入 callable 引用：

```python
@task("load_user")
async def load_user() -> dict:
    return {"name": "Alice"}


@task("greet")
async def greet(name: Ref[str, F(load_user, "name")]) -> None:
    print(f"hello {name}")
```

如果同一个函数对象被注册成多个任务 ID，callable 引用会变得不明确，此时请使用字符串任务 ID。

### 返回值声明去向 `T`

下游参数上的 `F` 已经能覆盖大多数场景。若你希望在上游返回值上声明去向，可以使用 `T`。

```python
from astrum import Ref, T, task


@task("source")
async def source() -> Ref[dict, T("target", "local_name", "remote_name")]:
    return {"local_name": "Alice"}


@task("target")
async def target(remote_name: str) -> None:
    print(remote_name)
```

### 显式 `TaskData`

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
    config=AstrumConfig(skip_type_check=True, silence_warnings=True),
)
```

日常业务代码优先使用注解式写法；显式 `TaskData` 更适合框架集成、旧代码迁移和动态 DAG。

## 查看执行计划

`build_scheduler()` 会构建 scheduler，但不会立即执行。你可以先查看阶段计划，再调用 `execute()`。

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

`ExecutionPlan` 常用方法：

- `get_visualization_table()`：返回文本阶段表。
- `get_dependency_graph_info()`：返回依赖关系说明。
- `get_full_visualization()`：返回完整文本视图。

## 读取执行报告

`run()` 和 `scheduler.execute()` 都返回 `ExecutionReport`。

```python
print(report.execution_state)
print(report.total_tasks)
print(report.successful_tasks)
print(report.failed_tasks)
print(report.total_duration)
print(report.error_summary)
print(report.original_tasks)
```

查看每个任务的统计：

```python
for stat in report.task_statistics:
    print(stat.task_name, stat.status, stat.duration, stat.attempt_count)
```

## 重试和失败

`@task(..., retry=N)` 表示失败后最多重试 N 次。

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

如果某个任务最终失败，Astrum 会停止后续依赖任务，并取消仍在运行但不应继续等待的并行任务。错误会进入 `report.error_summary` 和对应任务统计的 `error_message`。

## 配置

`AstrumConfig` 集中控制调度器行为。

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

常见配置：

- `concurrency_limit`：全局并发上限，`None` 表示不限制。
- `silence`：是否静默执行期日志。
- `visualize`：构建 DAG 后打印 Rich 可视化，需要安装 `astrum[viz]`。
- `skip_type_check`：跳过数据传输类型检查。
- `infer_via_ast`：缺少返回类型注解时尝试 AST 推断。
- `strict_topology`：严格校验数据关系和显式拓扑声明是否一致。
- `silence_warnings`：静默自动补全数据关系时的警告。
- `ignore_tail_task`：执行结束后不等待的末端任务列表。

## 可视化

安装 `astrum[viz]` 后，可以启用：

```python
from astrum import AstrumConfig, build_scheduler


scheduler = build_scheduler(config=AstrumConfig(visualize=True))
```

可视化会在构建调度器时输出 DAG 依赖树和 Data Transport Matrix，适合调试复杂数据流。

## 常见错误

### 传入已调用的协程对象

错误写法：

```python
DynamicScheduler(
    tasks=[("load", load())],
    task_order=[TaskOrder("load")],
)
```

正确写法：

```python
DynamicScheduler(
    tasks=[("load", load)],
    task_order=[TaskOrder("load")],
)
```

### 任务名重复

同一个命名空间内不能重复注册相同任务 ID。测试、Notebook 或热重载场景中，可以先清理：

```python
from astrum import clear_registry

clear_registry("my_namespace")
```

### 数据传输取不到字段

`F("source", "key")` 要求上游返回值是 dict 或有对应属性的对象。若上游返回 tuple/list，请使用整数 index；若要传整个返回值，请省略 locator。

### 可视化没有输出

确认已经安装可选依赖，并且设置了 `visualize=True`：

```bash
pip install "astrum[viz]"
```

## 最佳实践

- 优先使用装饰器模式，从 `@task`、`depends_on` 和 `run()` 开始。
- 为真实项目中的每条工作流使用独立 `namespace`。
- 在测试中使用 `clear_registry()` 隔离全局注册表。
- 任务 ID 使用稳定、可读的字符串，例如 `load_orders`、`build_report`。
- `depends_on` 表达执行依赖，`Ref/F` 表达数据依赖。
- 给参与数据传输的函数补全参数和返回值类型注解。
- 传给 `DynamicScheduler` 的任务必须是函数引用，不要提前调用。
- 从简单 DAG 开始，确认执行报告正确后，再加入数据传输、可视化和严格类型检查。

## API 速查

常用根导入：

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

高级数据传输对象从子模块导入：

```python
from astrum.data_transport import TaskData, DataItem, DTRela
```

