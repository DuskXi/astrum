from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from astrum import F, clear_registry, task
from astrum.config import AstrumConfig
from astrum.data_transport import DTRela, DataItem, TaskData, Ref
from astrum.decorators import build_scheduler


class OrderInfo(BaseModel):
    order_id: str
    drink: str
    customer: str


CLASSIC_NS = "classic_transport"
ANNOTATED_NS = "annotated_transport"


def register_classic_flow() -> list[str]:
    delivered: list[str] = []

    @task("take_order", namespace=CLASSIC_NS, data=TaskData())
    async def classic_take_order() -> OrderInfo:
        return OrderInfo(order_id="ORDER-1001", drink="latte", customer="Alice")

    @task(
        "grind_beans",
        namespace=CLASSIC_NS,
        data=TaskData(
            input_data_item=[
                DataItem(
                    allow_data_model=str,
                    from_relation=DTRela(key="drink", related_task="take_order"),
                    to_relation=DTRela(key="drink_type", related_task="grind_beans"),
                )
            ]
        ),
    )
    async def classic_grind_beans(drink_type: str) -> dict:
        return {"beans_ready": f"ground beans for {drink_type}"}

    @task(
        "brew_coffee",
        namespace=CLASSIC_NS,
        data=TaskData(
            input_data_item=[
                DataItem(
                    allow_data_model=str,
                    from_relation=DTRela(key="beans_ready", related_task="grind_beans"),
                    to_relation=DTRela(key="beans", related_task="brew_coffee"),
                ),
                DataItem(
                    allow_data_model=str,
                    from_relation=DTRela(key="drink", related_task="take_order"),
                    to_relation=DTRela(key="drink_type", related_task="brew_coffee"),
                ),
            ]
        ),
    )
    async def classic_brew_coffee(beans: str, drink_type: str) -> dict:
        return {"coffee_liquid": f"{drink_type} brewed with {beans}"}

    @task(
        "prepare_milk",
        namespace=CLASSIC_NS,
        data=TaskData(
            input_data_item=[
                DataItem(
                    allow_data_model=str,
                    from_relation=DTRela(key="drink", related_task="take_order"),
                    to_relation=DTRela(key="drink_type", related_task="prepare_milk"),
                )
            ]
        ),
    )
    async def classic_prepare_milk(drink_type: str) -> dict:
        return {"milk_foam": f"steamed milk for {drink_type}"}

    @task(
        "assemble_drink",
        namespace=CLASSIC_NS,
        data=TaskData(
            input_data_item=[
                DataItem(
                    allow_data_model=str,
                    from_relation=DTRela(key="coffee_liquid", related_task="brew_coffee"),
                    to_relation=DTRela(key="coffee", related_task="assemble_drink"),
                ),
                DataItem(
                    allow_data_model=str,
                    from_relation=DTRela(key="milk_foam", related_task="prepare_milk"),
                    to_relation=DTRela(key="milk", related_task="assemble_drink"),
                ),
                DataItem(
                    allow_data_model=str,
                    from_relation=DTRela(key="order_id", related_task="take_order"),
                    to_relation=DTRela(key="order_id", related_task="assemble_drink"),
                ),
            ]
        ),
    )
    async def classic_assemble_drink(coffee: str, milk: str, order_id: str) -> dict:
        return {"final_drink": f"{order_id}: {coffee} + {milk}"}

    @task(
        "serve_customer",
        namespace=CLASSIC_NS,
        data=TaskData(
            input_data_item=[
                DataItem(
                    allow_data_model=str,
                    from_relation=DTRela(key="customer", related_task="take_order"),
                    to_relation=DTRela(key="customer_name", related_task="serve_customer"),
                ),
                DataItem(allow_data_model=str, from_relation=DTRela(key="final_drink", related_task="assemble_drink"), to_relation=DTRela(key="drink", related_task="serve_customer")),
            ]
        ),
    )
    async def classic_serve_customer(customer_name: str, drink: str) -> dict:
        message = f"{customer_name} receives {drink}"
        delivered.append(message)
        return {"served": True, "message": message}

    return delivered


def register_annotated_flow() -> list[str]:
    delivered: list[str] = []

    @task("take_order", namespace=ANNOTATED_NS)
    async def annotated_take_order() -> OrderInfo:
        return OrderInfo(order_id="ORDER-1001", drink="latte", customer="Alice")

    @task("grind_beans", namespace=ANNOTATED_NS)
    async def annotated_grind_beans(drink_type: Ref[str, F("take_order", "drink")]) -> dict:
        return {"beans_ready": f"ground beans for {drink_type}"}

    @task("brew_coffee", namespace=ANNOTATED_NS)
    async def annotated_brew_coffee(beans: Ref[str, F(annotated_grind_beans, "beans_ready")], drink_type: Ref[str, F("take_order", "drink")]) -> dict:
        return {"coffee_liquid": f"{drink_type} brewed with {beans}"}

    @task("prepare_milk", namespace=ANNOTATED_NS)
    async def annotated_prepare_milk(drink_type: Ref[str, F("take_order", "drink")]) -> dict:
        return {"milk_foam": f"steamed milk for {drink_type}"}

    @task("assemble_drink", namespace=ANNOTATED_NS)
    async def annotated_assemble_drink(coffee: Ref[str, F("brew_coffee", "coffee_liquid")], milk: Ref[str, F("prepare_milk", "milk_foam")], order_id: Ref[str, F("take_order", "order_id")]) -> dict:
        return {"final_drink": f"{order_id}: {coffee} + {milk}"}

    @task("serve_customer", namespace=ANNOTATED_NS)
    async def annotated_serve_customer(customer_name: Ref[str, F("take_order", "customer")], drink: Ref[str, F("assemble_drink", "final_drink")]) -> dict:
        message = f"{customer_name} receives {drink}"
        delivered.append(message)
        return {"served": True, "message": message}

    return delivered


async def run_namespace(namespace: str, label: str) -> list[str]:
    scheduler = build_scheduler(namespace=namespace, config=AstrumConfig(skip_type_check=True, silence=True, silence_warnings=True, visualize=True))
    report = await scheduler.execute()
    print(f"{label}: {report.execution_state}")
    return [stat.task_name for stat in report.task_statistics if stat.status == "completed"]


async def main() -> None:
    clear_registry(CLASSIC_NS)
    clear_registry(ANNOTATED_NS)

    classic_delivered = register_classic_flow()
    annotated_delivered = register_annotated_flow()

    classic_tasks = await run_namespace(CLASSIC_NS, "classic TaskData flow")
    annotated_tasks = await run_namespace(ANNOTATED_NS, "annotated Ref flow")

    print(f"classic completed tasks: {classic_tasks}")
    print(f"annotated completed tasks: {annotated_tasks}")
    print(f"classic delivery: {classic_delivered[-1]}")
    print(f"annotated delivery: {annotated_delivered[-1]}")
    print(f"same result: {classic_delivered == annotated_delivered}")


if __name__ == "__main__":
    asyncio.run(main())
