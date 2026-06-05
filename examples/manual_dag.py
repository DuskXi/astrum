from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from astrum import DynamicScheduler, TaskOrder


async def extract_orders() -> None:
    """入口任务：模拟从订单库读取本批次订单。"""

    await asyncio.sleep(0.05)
    print("[extract_orders] fetched 3 orders")


async def extract_customers() -> None:
    """入口任务：模拟从客户系统读取客户档案。

    它和 extract_orders 没有依赖关系，所以调度器会在第一阶段并行启动二者。
    """

    await asyncio.sleep(0.05)
    print("[extract_customers] fetched customer profiles")


async def extract_exchange_rates() -> None:
    """入口任务：模拟读取汇率表，供后续收入计算使用。"""

    await asyncio.sleep(0.03)
    print("[extract_exchange_rates] fetched rates")


async def validate_orders() -> None:
    """依赖订单数据：检查字段完整性、状态合法性等。"""

    await asyncio.sleep(0.04)
    print("[validate_orders] orders validated")


async def enrich_customers() -> None:
    """依赖客户档案：补充客户分层、区域等分析维度。"""

    await asyncio.sleep(0.03)
    print("[enrich_customers] customer profiles enriched")


async def build_revenue_report() -> None:
    """汇总任务：需要订单验证结果、客户维度和汇率全部就绪。"""

    await asyncio.sleep(0.02)
    print("[build_revenue_report] revenue report built")


async def build_risk_report() -> None:
    """另一条汇总分支：只依赖订单验证和客户维度。"""

    await asyncio.sleep(0.02)
    print("[build_risk_report] risk report built")


async def publish_report() -> None:
    """尾部任务：等待所有报告构建完成后发布。"""

    await asyncio.sleep(0.01)
    print("[publish_report] reports published")


async def main() -> None:
    # 预设 DAG 写法适合“流程结构由业务代码显式声明”的场景。
    # TaskOrder 的 dependencies 表示“当前任务必须等待哪些任务完成”。
    extract_orders_order = TaskOrder(task_name="extract_orders")
    extract_customers_order = TaskOrder(task_name="extract_customers")
    extract_rates_order = TaskOrder(task_name="extract_exchange_rates")
    validate_orders_order = TaskOrder(task_name="validate_orders", dependencies=[extract_orders_order])
    enrich_customers_order = TaskOrder(task_name="enrich_customers", dependencies=[extract_customers_order])
    revenue_report_order = TaskOrder(
        task_name="build_revenue_report",
        dependencies=[validate_orders_order, enrich_customers_order, extract_rates_order],
    )
    risk_report_order = TaskOrder(
        task_name="build_risk_report",
        dependencies=[validate_orders_order, enrich_customers_order],
    )
    publish_order = TaskOrder(task_name="publish_report", dependencies=[revenue_report_order, risk_report_order])

    scheduler = DynamicScheduler(
        tasks=[
            # 直接传入函数引用，而不是已经调用后的协程对象
            ("extract_orders", extract_orders),
            ("extract_customers", extract_customers),
            ("extract_exchange_rates", extract_exchange_rates),
            ("validate_orders", validate_orders),
            ("enrich_customers", enrich_customers),
            ("build_revenue_report", build_revenue_report),
            ("build_risk_report", build_risk_report),
            ("publish_report", publish_report),
        ],
        task_order=[
            extract_orders_order,
            extract_customers_order,
            extract_rates_order,
            validate_orders_order,
            enrich_customers_order,
            revenue_report_order,
            risk_report_order,
            publish_order,
        ],
    )

    plan = scheduler.get_execute_timeline()
    print("\nPlanned stages:")
    print(plan.get_visualization_table())

    report = await scheduler.execute()
    print("\nExecution summary:")
    print(f"- state: {report.execution_state}")
    print(f"- tasks: {report.successful_tasks}/{report.total_tasks} completed")
    print(f"- dependency map: {report.original_tasks}")


if __name__ == "__main__":
    asyncio.run(main())
