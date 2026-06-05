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
        """根据 ``concurrency_limit`` 创建信号量对象。"""
        if self.concurrency_limit is not None and self.concurrency_limit > 0:
            return asyncio.Semaphore(self.concurrency_limit)
        return None
