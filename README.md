# Astrum

[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](#installation--安装)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](#license--许可证)
[![Docs](https://img.shields.io/badge/docs-MkDocs%20%2B%20Material-526CFE.svg)](https://duskxi.github.io/astrum/)
[![PyPI](https://img.shields.io/pypi/v/astrum.svg)](https://pypi.org/project/astrum/)

**A lightweight, in-process async DAG orchestrator for Python.**  
Astrum 是一个轻量、进程内运行的异步复杂多依赖任务调度库，用 Python 装饰器/静态DAG声明任务图，声明式高级依赖传递，自动并行执行无依赖/依赖分支，并返回结构化执行报告。

可榨干复杂多依赖并行任务系统的最后一滴时间，使用场景多见于需要在单个 Python 进程内编排工作流的业务代码中，或者需要在测试环境里模拟复杂 DAG 执行的场景。如Multi-Agent系统、复杂数据处理流程、异步任务编排等。

Astrum is a lightweight, in-process, asynchronous task scheduling library designed for complex multi-dependency workflows. It utilizes Python decorators and static DAGs (Directed Acyclic Graphs) to declare task graphs, features declarative advanced dependency passing, and automatically executes independent or branching dependencies in parallel while returning a structured execution report.

It is engineered to squeeze every last drop of performance out of complex, multi-dependency parallel task systems. Common use cases include business logic requiring workflow orchestration within a single Python process, or scenarios needing to simulate complex DAG execution in testing environments—such as Multi-Agent systems, complex data processing pipelines, and asynchronous task orchestration.

## Installation / 安装

```bash
pip install astrum
```

If you want terminal visualization powered by Rich:

```bash
pip install "astrum[viz]"
```

如果需要在终端查看 DAG 和数据传输可视化，请安装 `viz` 可选依赖。

Examples that render Rich tables or trees, such as the coffee shop workflow, require the `viz` extra.

## Quick Start / 快速开始

The recommended entry point is decorator mode: register functions with `@task`, declare execution dependencies with `depends_on`, pass upstream values with `Ref` / `F`, and call `run()`.

推荐从装饰器模式开始：用 `@task` 注册任务，用 `depends_on` 声明执行依赖，用 `Ref` / `F` 传递上游返回值，然后调用 `run()`。

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

Expected output:

```text
2 users, 3 orders
completed
4/4 tasks completed
```

`load_users` and `load_orders` have no input parameters, so they can run in parallel. `build_report` and `publish_report` receive data from upstream task results.

`load_users` 和 `load_orders` 没有入参，会并行执行；`build_report` 和 `publish_report` 通过 `Ref/F` 接收上游任务结果。

## Why Astrum / 为什么使用 Astrum

- **Plain Python tasks / 普通 Python 函数即任务**：同步函数和异步函数都可以注册为 DAG task。
- **Async DAG execution / 异步 DAG 执行**：没有依赖关系的任务自动并行启动。
- **Structured reports / 结构化报告**：执行状态、耗时、失败摘要、取消状态和重试记录都会进入 `ExecutionReport`。
- **Data transport / 数据传输**：用 `Ref[T, F("task", "field")]` 声明下游参数来自哪里。
- **Namespaces and registries / 命名空间与注册表**：可用 `namespace`、`use_namespace()` 或 `SchedulerRegistry` 隔离工作流。
- **Retries / 重试**：用 `@task(..., retry=N)` 为单个任务设置失败重试次数。

## When Not to Use It / 不适合场景

Astrum is intentionally small and in-process. It is not a replacement for distributed workflow engines.

Astrum 当前不是分布式调度系统。如果你的需求是下面这些场景，通常应该选择更重的工作流平台或队列系统：

- Cross-machine scheduling / 跨机器调度
- Persistent queues / 持久化队列
- Cron-like scheduled jobs / 定时任务平台
- Long-running distributed workflows / 长生命周期分布式工作流
- Worker fleet management / 多 worker 集群管理

## Core Concepts / 核心概念

- **Task / 任务**：一个已注册的 Python callable。任务可以是 `async def`，也可以是普通 `def`。
- **DAG / 有向无环图**：任务之间的依赖关系。某个任务只有在上游依赖完成后才会启动。
- **Scheduler / 调度器**：把 DAG 拆成执行阶段，启动可并行任务，并收集执行结果。
- **ExecutionReport / 执行报告**：`run()` 和 `scheduler.execute()` 的返回值，包含状态、耗时、统计和错误摘要。
- **Data Transport / 数据传输**：把上游任务的返回值注入下游函数参数。推荐使用 `Ref` / `F` 注解。

## Documentation / 文档

The full documentation is bilingual and built with MkDocs + Material for MkDocs.

完整文档支持中英文，并使用 MkDocs + Material for MkDocs 构建。

- [中文文档入口](https://duskxi.github.io/astrum/)
- [English documentation](https://duskxi.github.io/astrum/en/)
- [快速开始 / Quickstart](https://duskxi.github.io/astrum/quickstart/)
- [中文使用指南](USAGE.zh.md)
- [English Usage Guide](USAGE.en.md)
- [外部 API Reference](https://duskxi.github.io/astrum/api/external/)
- [Internal API Reference](https://duskxi.github.io/astrum/en/api/internal/)

The local Markdown files remain available in `docs/` for offline reading and contributions.

## Examples / 示例

- [Fast Start](docs/examples/fast-start.zh.md)：从串行、并行、fan-in 到异步重试的工作流模式速览。
- [Coffee Shop](docs/examples/coffee-shop.zh.md)：用咖啡店流程解释 `TaskData` / `DataItem` / `DTRela` 数据流。
- [Stateless Text Retriever](docs/examples/stateless-text-retriever.zh.md)：用复杂检索链路展示 fan-out、fan-in、多分支评分和 rerank。

The corresponding English pages are available next to the Chinese pages with `.en.md` filenames.

## Development / 开发

This project uses `uv` for dependency management.

```bash
uv sync --extra docs --extra viz
uv run pytest
uv run mkdocs serve
uv run mkdocs build --strict
```

Common checks before publishing:

```bash
uv run pytest
uv run mkdocs build --strict
```

## Project Status / 项目状态

Current version: **0.1.0**.

Astrum 0.1.0 initial public release; APIs may change in 0.1.x.

Astrum 已经初次发布到 PyPI。在 `0.1.x` 阶段仍可能围绕 API 命名、文档和类型检查体验做小幅改进。

## License / 许可证

Astrum is released under the MIT License.

Astrum 使用 MIT 许可证发布。
