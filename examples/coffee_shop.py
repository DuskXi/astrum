import asyncio
from rich.console import Console
from pydantic import BaseModel

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from astrum.decorators import task, build_scheduler, clear_registry, use_namespace
from astrum.data_transport import TaskData, DataItem, DTRela
from astrum.config import AstrumConfig

console = Console()


# 定义基础模型
class OrderInfo(BaseModel):
    order_id: str
    drink: str
    customer: str


# 静态数据注入函数
def get_store_name():
    return "Astrum 极客咖啡馆"


# 在模块顶层定义各种独立任务
# 这里展现了现实生活中的咖啡制作流水线


# 1. 咖啡店前台接单
# 该任务没有显式指定依赖，作为图的起点。输出包含订单ID、饮品类型和客户名。
@task(task_id="take_order", data=TaskData())
async def take_order():
    console.print("[bold yellow]👩‍🍳 前台[/bold yellow]: 欢迎光临！正在为您下单...")
    await asyncio.sleep(0.1)
    # 直接返回调用字面量，AST _guess_type_from_node 会提取 OrderInfo
    return OrderInfo(order_id="ORDER-2026", drink="焦糖玛奇朵", customer="Alice")


# 2. 磨豆子
# 依赖接单结果，只需要知道具体的饮品类型
@task(
    task_id="grind_beans",
    data=TaskData(
        input_data_item=[
            DataItem(
                allow_data_model=OrderInfo,
                # 从 take_order 获取 drink，系统会自动补全依赖和双向数据定义
                from_relation=DTRela(key="drink", related_task="take_order"),
                to_relation=DTRela(key="drink_type", related_task="grind_beans"),
            )
        ]
    ),
)
async def grind_beans(drink_type: str):
    console.print(f"[bold cyan]⚙️ 磨豆机[/bold cyan]: 正在为 [italic]{drink_type}[/italic] 研磨咖啡豆...")
    await asyncio.sleep(0.2)
    # 测试局部变量标注
    res: dict = {"beans_ready": True}
    return res


# 3. 萃取咖啡液
# 需要用到磨好的豆子，同时还需要知道是什么饮品
@task(
    task_id="brew_coffee",
    data=TaskData(
        input_data_item=[
            DataItem(allow_data_model=dict, from_relation=DTRela(key="beans_ready", related_task="grind_beans"), to_relation=DTRela(key="beans", related_task="brew_coffee")),
            DataItem(allow_data_model=OrderInfo, from_relation=DTRela(key="drink", related_task="take_order"), to_relation=DTRela(key="drink_type", related_task="brew_coffee")),
        ]
    ),
)
async def brew_coffee(beans: bool, drink_type: str):
    console.print(f"[bold red]☕ 咖啡机[/bold red]: 咖啡豆就绪({beans})，正在萃取 [italic]{drink_type}[/italic] 的浓缩液...")
    await asyncio.sleep(0.3)
    # 测试字典字面量返回
    return {"coffee_liquid": f"热气腾腾的{drink_type}浓缩"}


# 4. 打奶泡 (与 2,3 并行)
# 只要接到单知道要做什么，就可以开始准备奶泡
@task(
    task_id="prepare_milk",
    data=TaskData(
        input_data_item=[DataItem(allow_data_model=OrderInfo, from_relation=DTRela(key="drink", related_task="take_order"), to_relation=DTRela(key="drink_type", related_task="prepare_milk"))]
    ),
)
async def prepare_milk(drink_type: str):
    console.print(f"[bold white]🥛 奶泡机[/bold white]: 正在为 [italic]{drink_type}[/italic] 打发绵密的奶泡...")
    await asyncio.sleep(0.2)
    # 模拟经过多层传递
    result_dict: dict = {"milk_foam": "香甜绵密奶泡"}
    final_res = result_dict
    return final_res


# 5. 组合出品
# 需要汇聚浓缩液、奶泡，并带上订单ID
@task(
    task_id="assemble_drink",
    data=TaskData(
        input_data_item=[
            DataItem(allow_data_model=dict, from_relation=DTRela(key="coffee_liquid", related_task="brew_coffee"), to_relation=DTRela(key="coffee", related_task="assemble_drink")),
            DataItem(allow_data_model=dict, from_relation=DTRela(key="milk_foam", related_task="prepare_milk"), to_relation=DTRela(key="milk", related_task="assemble_drink")),
            DataItem(allow_data_model=OrderInfo, from_relation=DTRela(key="order_id", related_task="take_order"), to_relation=DTRela(key="order_id", related_task="assemble_drink")),
        ]
    ),
)
def assemble_drink(coffee: str, milk: str, order_id: str):
    console.print(f"[bold magenta]🎨 咖啡师[/bold magenta]: 正在为订单 {order_id} 进行拉花组合... ({coffee} + {milk})")
    out: dict = {"final_drink": f"完美的 {coffee} 与 {milk} 融合"}
    return out


# 6. 最终交付顾客
# 获取成品的饮品，客户的名称，同时通过 from_function 注入常量数据（店铺名）
@task(
    task_id="serve_customer",
    data=TaskData(
        input_data_item=[
            DataItem(allow_data_model=OrderInfo, from_relation=DTRela(key="customer", related_task="take_order"), to_relation=DTRela(key="customer_name", related_task="serve_customer")),
            DataItem(allow_data_model=dict, from_relation=DTRela(key="final_drink", related_task="assemble_drink"), to_relation=DTRela(key="drink", related_task="serve_customer")),
            DataItem(
                # 使用 from_function 自动调用外部函数，无需依赖其他任务节点
                from_relation=DTRela(key="store_name", related_task="serve_customer", from_function=get_store_name),
                to_relation=DTRela(key="store_name", related_task="serve_customer"),
            ),
        ]
    ),
)
def serve_customer(customer_name: str, drink: str, store_name: str):
    console.print(f"[bold green]🛎️ 服务员[/bold green]: {customer_name}，您的 '{drink}' 做好了！感谢光临 {store_name}，祝您今天愉快！")
    return {"served": True}


async def main():
    console.print("\n[bold]=== 构建并规划咖啡制作任务流 ===[/bold]\n")

    # 使用 AstrumConfig 一站式配置所有行为参数
    cfg = AstrumConfig(
        visualize=True,  # 在终端打印 DAG 树和数据矩阵
        infer_via_ast=True,  # 开启 AST 静态分析推断函数返回类型
        skip_type_check=False,  # 不跳过类型安全校验
        strict_topology=False,  # 宽松拓扑校验（自动补全不报错）
        silence_warnings=False,  # 静默自动补全过程中的 DEBUG/WARNING 日志
        silence=False,  # 调度器静默执行
    )
    scheduler = build_scheduler(config=cfg)

    console.print("\n[bold]=== 开始执行咖啡制作 DAG ===[/bold]\n")
    report = await scheduler.execute()
    console.print(f"\n[bold]=== 执行完毕 ({report.execution_state}) ===[/bold]\n")
    if report.execution_state == "failed":
        for index, msg in enumerate(report.error_summary, start=1):
            console.print(f"[red]{index}. {msg}[/red]")


if __name__ == "__main__":
    asyncio.run(main())
