from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from astrum import build_scheduler, build_task_orders, task, use_namespace
from astrum.models import TaskOrder

from rich.console import Console
from rich.table import Table
from rich.tree import Tree

console = Console()


# 写法一：直接给 @task 传 namespace 参数。
# 任务函数定义在模块顶层，无需先实例化任何注册器对象，对纯 OOP 代码最友好。
@task("load_sales_csv", namespace="analytics")
async def load_sales_csv() -> None:
    """读取销售 CSV。无依赖，因此可以作为 DAG 入口并行启动。"""

    await asyncio.sleep(0.05)
    console.log("loaded sales csv")


@task("load_product_api", namespace="analytics")
async def load_product_api() -> None:
    """读取产品元数据。和销售 CSV 相互独立。"""

    await asyncio.sleep(0.05)
    console.log("loaded product api")


# 写法二：使用 use_namespace 上下文管理器，块内的 @task 自动归属到该命名空间。
# 适合"一组任务集中声明"的代码片段，避免在每个 @task 上重复书写 namespace=。
with use_namespace("analytics"):

    @task("load_campaign_api")
    async def load_campaign_api() -> None:
        """读取营销活动数据，后续用于归因分析。"""

        await asyncio.sleep(0.04)
        console.log("loaded campaign api")

    @task("clean_sales", depends_on=["load_sales_csv"])
    async def clean_sales() -> None:
        """只依赖销售数据：清洗空值、统一币种字段。"""

        await asyncio.sleep(0.03)
        console.log("cleaned sales")

    @task("join_product_catalog", depends_on=["clean_sales", "load_product_api"])
    async def join_product_catalog() -> None:
        """等待销售清洗和产品元数据后，把产品维度 join 到销售事实表。"""

        await asyncio.sleep(0.02)
        console.log("joined product catalog")

    @task("attribute_campaigns", depends_on=["clean_sales", "load_campaign_api"])
    async def attribute_campaigns() -> None:
        """等待销售清洗和活动数据后，计算活动归因。"""

        await asyncio.sleep(0.02)
        console.log("attributed campaigns")

    @task("build_dashboard", depends_on=["join_product_catalog", "attribute_campaigns"])
    async def build_dashboard() -> None:
        """两个分析分支都完成后构建仪表盘数据。"""

        await asyncio.sleep(0.02)
        console.log("built dashboard")

    @task("send_digest", depends_on=["build_dashboard"])
    async def send_digest() -> None:
        """最后一步：发布摘要。"""

        await asyncio.sleep(0.01)
        console.log("sent digest")


def render_dependency_tree(task_orders: list[TaskOrder]) -> Tree:
    task_map = {task.task_name: task for task in task_orders}
    dependents: dict[str, list[str]] = {task.task_name: [] for task in task_orders}
    for task in task_orders:
        for dependency in task.dependencies:
            dependents[dependency.task_name].append(task.task_name)

    roots = [task.task_name for task in task_orders if not task.dependencies]
    tree = Tree("[bold]Decorator-built DAG[/bold]")

    def add_children(parent: Tree, task_name: str, seen: set[str]) -> None:
        branch = parent.add(task_name)
        if task_name in seen:
            branch.add("[dim]already shown[/dim]")
            return
        for child in dependents.get(task_name, []):
            add_children(branch, child, seen | {task_name})

    for root in roots:
        add_children(tree, root, set())

    tree.add(f"[dim]total tasks: {len(task_map)}[/dim]")
    return tree


def render_stage_table(scheduler) -> Table:
    plan = scheduler.get_execute_timeline()
    table = Table(title="Execution stages")
    table.add_column("Stage", justify="right")
    table.add_column("Start tasks")
    table.add_column("Wait for")
    table.add_column("Parallel view")

    for stage in plan.stages:
        table.add_row(
            str(stage.stage_id),
            ", ".join(stage.start_tasks) or "-",
            ", ".join(stage.wait_for_tasks) or "-",
            ", ".join(stage.parallel_tasks) or "-",
        )

    return table


async def main() -> None:
    # 调度入口同样支持两种命名空间选择方式：
    # 1) 直接给 build_scheduler / build_task_orders 传 namespace=
    scheduler = build_scheduler(target_tasks=["send_digest"], namespace="analytics")
    task_orders = build_task_orders(target_tasks=["send_digest"], namespace="analytics")

    # 2) 通过 use_namespace 上下文管理器，块内调用无需重复传 namespace=
    #    with use_namespace("analytics"):
    #        scheduler = build_scheduler(target_tasks=["send_digest"])

    console.print(render_dependency_tree(task_orders))
    console.print(render_stage_table(scheduler))

    report = await scheduler.execute()
    console.print("\n[bold]Execution summary[/bold]")
    console.print(f"state={report.execution_state}, completed={report.successful_tasks}/{report.total_tasks}")
    console.print(report.original_tasks)


if __name__ == "__main__":
    asyncio.run(main())
