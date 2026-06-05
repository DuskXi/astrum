"""Astrum 全局调度引擎配置。

通过一个不可变的 :class:`AstrumConfig` 数据类来集中管理调度器生命周期中
所有可调节的行为参数。用户只需在 ``build_scheduler()`` / ``run()`` 时传入一个
config 实例即可一次性控制全部内部开关。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class AstrumConfig:
    """Astrum 调度引擎一站式配置对象。

    所有参数均有安全的默认值，用户可按需覆盖。

    **类型检查 / AST 推断**

    - ``skip_type_check`` – 跳过 ``allow_data_model`` 的类型匹配校验。
      开启后 DataItem 上的类型约束将不会生效。默认 ``False``。
    - ``infer_via_ast`` – 在函数缺少返回值注解 (``-> type``) 时，自动使用
      AST 静态分析来推断返回类型。默认 ``False``。
    - ``strict_topology`` – 严格拓扑校验模式。当 ``True`` 时，如果
      data transport 推导出的 from/to 关系没有在 ``TaskData.from_tasks``
      / ``to_tasks`` 中显式声明就会报错。默认 ``False``。

    **数据传输**

    - ``allow_no_dir_definition`` – 允许 DataItem 缺少来源或去向定义。
      在使用装饰器模式时通常设为 ``True``，因为框架会自动补全；手动模式下
      设为 ``False`` 可以捕捉用户遗漏。默认 ``True``。
    - ``auto_sync_dependencies`` – 自动将 data transport 推导出来的
      ``from_tasks`` 关系同步回任务图的 ``dependencies``。关闭后需用户
      手动声明 ``depends_on``。默认 ``True``。

    **可视化**

    - ``visualize`` – 在 DAG 构建完成后是否使用 Rich 在终端打印 DAG 树和
      Data Transport Matrix。默认 ``False``。
    - ``silence_warnings`` – 静默自动补全过程中的 DEBUG/WARNING 日志输出
      （如 "undeclared relation has been automatically declared"）。默认 ``False``。

    **执行引擎**

    - ``silence`` – 静默模式。当为 ``True`` 时调度器不输出执行期日志。
      默认 ``True``。
    - ``concurrency_limit`` – 全局并发限制。设为 ``None`` 表示不限制；
      设为正整数 N 时，内部自动创建 ``asyncio.Semaphore(N)``。默认 ``None``。
    - ``ignore_tail_task`` – 执行完成后不等待的末端任务列表。默认空列表。

    Astrum's one-stop configuration object for the scheduling engine.

    All parameters have safe defaults and can be overridden as needed.

    **Type checking / AST inference**

    - ``skip_type_check`` – Skip type matching validation for ``allow_data_model``.
      When enabled, type constraints on DataItem will not take effect. Defaults to ``False``.
    - ``infer_via_ast`` – When a function lacks a return value annotation
      (``-> type``), automatically use AST static analysis to infer the return type.
      Defaults to ``False``.
    - ``strict_topology`` – Strict topology validation mode. When ``True``, an error
      is raised if from/to relations inferred by data transport are not explicitly
      declared in ``TaskData.from_tasks`` / ``to_tasks``. Defaults to ``False``.

    **Data transport**

    - ``allow_no_dir_definition`` – Allow DataItem to lack source or destination
      definitions. This is usually set to ``True`` in decorator mode because the
      framework completes them automatically; in manual mode, setting it to ``False``
      can catch omissions. Defaults to ``True``.
    - ``auto_sync_dependencies`` – Automatically sync ``from_tasks`` relations inferred
      by data transport back to the task graph's ``dependencies``. When disabled,
      users must manually declare ``depends_on``. Defaults to ``True``.

    **Visualization**

    - ``visualize`` – Whether to use Rich to print the DAG tree and Data Transport
      Matrix in the terminal after DAG construction completes. Defaults to ``False``.
    - ``silence_warnings`` – Silence DEBUG/WARNING log output during automatic
      completion, such as "undeclared relation has been automatically declared".
      Defaults to ``False``.

    **Execution engine**

    - ``silence`` – Silence mode. When ``True``, the scheduler does not output
      execution-time logs. Defaults to ``True``.
    - ``concurrency_limit`` – Global concurrency limit. Set to ``None`` for no limit;
      set to a positive integer N to automatically create ``asyncio.Semaphore(N)``
      internally. Defaults to ``None``.
    - ``ignore_tail_task`` – List of tail tasks not to wait for after execution
      completes. Defaults to an empty list.
    """

    # ── 类型检查 ──
    skip_type_check: bool = False
    infer_via_ast: bool = False
    strict_topology: bool = False

    # ── 数据传输 ──
    allow_no_dir_definition: bool = True
    auto_sync_dependencies: bool = True

    # ── 可视化 ──
    visualize: bool = False
    silence_warnings: bool = False

    # ── 执行引擎 ──
    silence: bool = True
    concurrency_limit: int | None = None
    ignore_tail_task: list[str] = field(default_factory=list)

    def build_semaphore(self) -> asyncio.Semaphore | None:
        """根据 ``concurrency_limit`` 创建信号量对象。

        Create a semaphore object according to ``concurrency_limit``.
        """
        if self.concurrency_limit is not None and self.concurrency_limit > 0:
            return asyncio.Semaphore(self.concurrency_limit)
        return None
