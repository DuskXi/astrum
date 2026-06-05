# Astrum

Astrum 是一个轻量、进程内运行的 async DAG 编排库。它适合把一组 Python 函数组织成有依赖关系的工作流，让无依赖分支并行执行，并在结束后返回结构化执行报告。

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

## 适合场景

- 在单个 Python 进程内运行本地异步工作流。
- 用 DAG 表达任务依赖，并自动并行运行互不依赖的任务。
- 需要执行状态、耗时、失败原因、重试记录等报告。
- 希望通过 `Ref` / `F` 注解把上游任务返回值注入下游函数参数。

## 安装

```bash
pip install astrum
```

如果需要 Rich 终端可视化：

```bash
pip install "astrum[viz]"
```

## 下一步

- 阅读 [快速开始](quickstart.md) 编写第一个 DAG。
- 阅读 [使用指南](guide.md) 理解命名空间、手动 DAG、数据传输和重试。
- 查阅 [外部 API Reference](api/external/index.md) 获取面向用户的公开 API。
