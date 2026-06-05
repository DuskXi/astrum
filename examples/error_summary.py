from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from astrum import DynamicScheduler, TaskOrder

from rich.console import Console
from rich.panel import Panel
from rich.table import Table


console = Console()


async def download_orders() -> None:
    """入口任务：下载订单文件。这个任务会成功完成。"""

    await asyncio.sleep(0.02)
    console.log("orders downloaded")


async def download_inventory() -> None:
    """入口任务：下载库存文件。

    示例故意让它失败，用于观察调度器如何：
    1. 记录失败任务；
    2. 停止后续依赖任务；
    3. 取消已经启动但还没完成的并行任务；
    4. 在 ExecutionReport.error_summary 中归纳错误。
    """

    await asyncio.sleep(0.03)
    raise RuntimeError("inventory service returned HTTP 503")


async def warm_cache() -> None:
    """入口任务：模拟长耗时并行任务。

    当 download_inventory 失败时，这个任务通常还在运行，会被调度器取消。
    """

    await asyncio.sleep(0.5)
    console.log("cache warmed")


async def reconcile_orders() -> None:
    """下游任务：依赖订单和库存，因此上游失败后不会被启动。"""

    await asyncio.sleep(0.01)
    console.log("orders reconciled")


def build_error_table(report) -> Table:
    table = Table(title="Task status summary")
    table.add_column("Task")
    table.add_column("Stage", justify="right")
    table.add_column("Status")
    table.add_column("Error")

    for task in report.task_statistics:
        table.add_row(
            task.task_name,
            str(task.stage_id),
            task.status,
            task.error_message or "-",
        )

    return table


async def main() -> None:
    download_orders_order = TaskOrder("download_orders")
    download_inventory_order = TaskOrder("download_inventory")
    warm_cache_order = TaskOrder("warm_cache")
    reconcile_order = TaskOrder(
        "reconcile_orders",
        dependencies=[download_orders_order, download_inventory_order, warm_cache_order],
    )

    scheduler = DynamicScheduler(
        tasks=[
            ("download_orders", download_orders),
            ("download_inventory", download_inventory),
            ("warm_cache", warm_cache),
            ("reconcile_orders", reconcile_orders),
        ],
        task_order=[download_orders_order, download_inventory_order, warm_cache_order, reconcile_order],
    )

    report = await scheduler.execute()

    console.print(Panel.fit("Failure report example", style="bold red"))
    console.print(f"state={report.execution_state}, completed={report.successful_tasks}, failed={report.failed_tasks}")
    console.print(build_error_table(report))

    console.print("\n[bold]error_summary[/bold]")
    for index, message in enumerate(report.error_summary, start=1):
        console.print(f"{index}. {message}")


if __name__ == "__main__":
    asyncio.run(main())
