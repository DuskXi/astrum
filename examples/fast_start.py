import asyncio

from astrum import F, AstrumConfig, Ref, task
from astrum.decorators import build_scheduler

CONFIG = AstrumConfig(skip_type_check=True, silence=True, silence_warnings=True, visualize=True)


# --- Example 1 Serial: Serial 2 Task ---
async def example_1():
    """
    例子1: 该例子为双任务依赖执行，任务2依赖于任务1，因此是串行
    :return:
    """
    # ┌------------------┐
    # |   ┌----------┐   |
    # |   |          |   |
    # |   |  Task 1  |   |
    # |   |          |   |
    # |   └----------┘   |
    # |        ↓         |
    # |   ┌----------┐   |
    # |   |          |   |
    # |   |  Task 2  |   |
    # |   |          |   |
    # |   └----------┘   |
    # └------------------┘

    @task("task1", namespace="example_1")
    def task1() -> str:
        return "task1"

    @task("task2", namespace="example_1")
    def task2(prev_result: Ref[str, F("task1")]) -> str:
        return "task2" + prev_result

    scheduler = build_scheduler(namespace="example_1", config=CONFIG)
    report = await scheduler.execute()
    result = report.task_return_set["task2"]
    assert result == "task2task1"


# --- Example 1 End ---


# --- Example 2 Parallel: Parallel 2 Tasks ---
async def example_2():
    """
    例子2: 该例子为双任务并发，没有依赖关系
    :return:
    """
    # ┌---------------------------------┐
    # |   ┌----------┐   ┌----------┐   |
    # |   |          |   |          |   |
    # |   |  Task 1  |   |  Task 2  |   |
    # |   |          |   |          |   |
    # |   └----------┘   └----------┘   |
    # └---------------------------------┘

    @task("task1", namespace="example_2")
    def task1() -> str:
        return "task1"

    @task("task2", namespace="example_2")
    def task2() -> str:
        return "task2"

    scheduler = build_scheduler(namespace="example_2", config=CONFIG)
    report = await scheduler.execute()
    result1 = report.task_return_set["task1"]
    result2 = report.task_return_set["task2"]
    assert result1 == "task1"
    assert result2 == "task2"


# --- Example 2 End ---


# --- Example 3 Triangle : Mixed Serial and Parallel 3 Tasks ---
async def example_3():
    """
    例子3: 该例子为两个任务被一个任务同时依赖，因此前两个任务并发，而最后一个任务同时等待前两个完成
    :return:
    """
    # ┌---------------------------------┐
    # |   ┌----------┐   ┌----------┐   |
    # |   |          |   |          |   |
    # |   |  Task 1  |   |  Task 2  |   |
    # |   |          |   |          |   |
    # |   └----------┘   └----------┘   |
    # |        |                |       |
    # |        └→ ┌----------┐ ←┘       |
    # |           |          |          |
    # |           |  Task 3  |          |
    # |           |          |          |
    # |           └----------┘          |
    # └---------------------------------┘

    @task("task1", namespace="example_3")
    def task1() -> str:
        return "task1"

    @task("task2", namespace="example_3")
    def task2() -> str:
        return "task2"

    @task("task3", namespace="example_3")
    def task3(prev1: Ref[str, F("task1")], prev2: Ref[str, F("task2")]) -> str:
        return "task3" + prev1 + prev2

    scheduler = build_scheduler(namespace="example_3", config=CONFIG)
    report = await scheduler.execute()
    result = report.task_return_set["task3"]
    assert result == "task3task1task2"


# --- Example 3 End ---


# --- Example 4 Complex Graph: Mixed Serial and Parallel Multiple Tasks ---
async def example_4():
    """
    例子4: 复合情况，task 1/2/3内含并行的task1/2，task3依赖于task1/2，而task4/5之间为串行，但是task1/2/3和task4/5之间又是并行的，并且互不干扰
    :return:
    """
    # ┌---------------------------------------------------┐
    # |   ┌----------┐   ┌----------┐      ┌----------┐   |
    # |   |          |   |          |      |          |   |
    # |   |  Task 1  |   |  Task 2  |      |  Task 4  |   |
    # |   |          |   |          |      |          |   |
    # |   └----------┘   └----------┘      └----------┘   |
    # |        |                |               ↓         |
    # |        └→ ┌----------┐ ←┘          ┌----------┐   |
    # |           |          |             |          |   |
    # |           |  Task 3  |             |  Task 5  |   |
    # |           |          |             |          |   |
    # |           └----------┘             └----------┘   |
    # └---------------------------------------------------┘

    @task("task1", namespace="example_4")
    def task1() -> str:
        return "task1"

    @task("task2", namespace="example_4")
    def task2() -> str:
        return "task2"

    @task("task3", namespace="example_4")
    def task3(prev1: Ref[str, F("task1")], prev2: Ref[str, F("task2")]) -> str:
        return "task3" + prev1 + prev2

    @task("task4", namespace="example_4")
    def task4() -> str:
        return "task4"

    @task("task5", namespace="example_4")
    def task5(prev: Ref[str, F("task4")]) -> str:
        return "task5" + prev

    scheduler = build_scheduler(namespace="example_4", config=CONFIG)
    report = await scheduler.execute()
    resul3 = report.task_return_set["task3"]
    resul5 = report.task_return_set["task5"]
    assert resul3 == "task3task1task2"
    assert resul5 == "task5task4"


# --- Example 5 Order Checkout: Fan-out, Fan-in and Field Data Flow ---
async def example_5():
    """
    例子5: 订单结算复合流。

    该例子包含三个并行入口，并在中间形成多个交错分支：
    - 订单、库存、价格规则并行加载
    - 商品校验、金额计算、折扣、税费和库存审计互相穿插
    - 最终收据同时汇聚订单字段、金额字段和审计结果
    """
    # ┌--------------------------------------------------------------------------┐
    # | load_order      load_inventory      load_pricing_rules                   |
    # |     |              |                    |                                |
    # |     ├----------→ validate_cart          ├----------→ compute_subtotal    |
    # |     |                                      |                             |
    # |     ├----------------------------------→ apply_discount                  |
    # |     |                                      |                             |
    # |     └----------------------------------→ add_tax                         |
    # |                    |                    |                                |
    # |                    └→ build_inventory_audit                              |
    # |                         |                                                |
    # | compute_subtotal + apply_discount + add_tax + audit → build_receipt      |
    # |                                               |                          |
    # |                                               └→ finalize_order          |
    # └--------------------------------------------------------------------------┘

    @task("load_order", namespace="example_5")
    def load_order() -> dict:
        return {
            "order_id": "order-1001",
            "customer_id": "customer-42",
            "region": "CN-SH",
            "coupon": "SAVE20",
            "items": [
                {"sku": "mesh-keyboard", "qty": 2},
                {"sku": "flow-mouse", "qty": 1},
                {"sku": "dag-sticker", "qty": 3},
            ],
        }

    @task("load_inventory", namespace="example_5")
    def load_inventory() -> dict:
        return {
            "stock": {
                "mesh-keyboard": 5,
                "flow-mouse": 8,
                "dag-sticker": 99,
            }
        }

    @task("load_pricing_rules", namespace="example_5")
    def load_pricing_rules() -> dict:
        return {
            "prices_cents": {
                "mesh-keyboard": 1200,
                "flow-mouse": 2500,
                "dag-sticker": 800,
            },
            "discounts": {
                "SAVE20": 20,
                "WELCOME10": 10,
            },
            "tax_basis_points": {
                "CN-SH": 800,
                "US-CA": 925,
            },
        }

    @task("validate_cart", namespace="example_5")
    def validate_cart(items: Ref[list, F("load_order", "items")], stock: Ref[dict, F("load_inventory", "stock")]) -> dict:
        missing_skus = [item["sku"] for item in items if stock.get(item["sku"], 0) < item["qty"]]
        reserved_items = [{"sku": item["sku"], "qty": item["qty"]} for item in items if item["sku"] not in missing_skus]
        return {
            "is_valid": len(missing_skus) == 0,
            "missing_skus": missing_skus,
            "reserved_items": reserved_items,
        }

    @task("compute_subtotal", namespace="example_5")
    def compute_subtotal(items: Ref[list, F("load_order", "items")], prices: Ref[dict, F("load_pricing_rules", "prices_cents")]) -> dict:
        line_items = [
            {
                "sku": item["sku"],
                "qty": item["qty"],
                "unit_cents": prices[item["sku"]],
                "line_total_cents": item["qty"] * prices[item["sku"]],
            }
            for item in items
        ]
        return {
            "line_items": line_items,
            "subtotal_cents": sum(item["line_total_cents"] for item in line_items),
        }

    @task("apply_discount", namespace="example_5")
    def apply_discount(
        subtotal_cents: Ref[int, F("compute_subtotal", "subtotal_cents")],
        coupon: Ref[str, F("load_order", "coupon")],
        discounts: Ref[dict, F("load_pricing_rules", "discounts")],
    ) -> dict:
        discount_rate = discounts.get(coupon, 0)
        discount_cents = subtotal_cents * discount_rate // 100
        return {
            "discount_rate": discount_rate,
            "discount_cents": discount_cents,
            "net_subtotal_cents": subtotal_cents - discount_cents,
        }

    @task("add_tax", namespace="example_5")
    def add_tax(
        net_subtotal_cents: Ref[int, F("apply_discount", "net_subtotal_cents")],
        region: Ref[str, F("load_order", "region")],
        tax_basis_points: Ref[dict, F("load_pricing_rules", "tax_basis_points")],
    ) -> dict:
        basis_points = tax_basis_points[region]
        return {
            "tax_basis_points": basis_points,
            "tax_cents": net_subtotal_cents * basis_points // 10000,
        }

    @task("build_inventory_audit", namespace="example_5")
    def build_inventory_audit(
        order_id: Ref[str, F("load_order", "order_id")],
        is_valid: Ref[bool, F("validate_cart", "is_valid")],
        missing_skus: Ref[list, F("validate_cart", "missing_skus")],
        reserved_items: Ref[list, F("validate_cart", "reserved_items")],
    ) -> dict:
        return {
            "audit_id": f"audit-{order_id}",
            "is_valid": is_valid,
            "missing_skus": missing_skus,
            "reserved_count": sum(item["qty"] for item in reserved_items),
        }

    @task("build_receipt", namespace="example_5")
    def build_receipt(
        order_id: Ref[str, F("load_order", "order_id")],
        customer_id: Ref[str, F("load_order", "customer_id")],
        line_items: Ref[list, F("compute_subtotal", "line_items")],
        subtotal_cents: Ref[int, F("compute_subtotal", "subtotal_cents")],
        discount_cents: Ref[int, F("apply_discount", "discount_cents")],
        net_subtotal_cents: Ref[int, F("apply_discount", "net_subtotal_cents")],
        tax_cents: Ref[int, F("add_tax", "tax_cents")],
        audit_id: Ref[str, F("build_inventory_audit", "audit_id")],
        is_valid: Ref[bool, F("build_inventory_audit", "is_valid")],
        missing_skus: Ref[list, F("build_inventory_audit", "missing_skus")],
    ) -> dict:
        payable_cents = net_subtotal_cents + tax_cents
        return {
            "receipt_id": f"receipt-{order_id}",
            "order_id": order_id,
            "customer_id": customer_id,
            "line_items": line_items,
            "subtotal_cents": subtotal_cents,
            "discount_cents": discount_cents,
            "tax_cents": tax_cents,
            "payable_cents": payable_cents,
            "status": "ready_to_pay" if is_valid else "blocked",
            "audit_id": audit_id,
            "missing_skus": missing_skus,
        }

    @task("finalize_order", namespace="example_5")
    def finalize_order(receipt: Ref[dict, F("build_receipt")], audit_id: Ref[str, F("build_inventory_audit", "audit_id")]) -> dict:
        return {
            "order_id": receipt["order_id"],
            "receipt_id": receipt["receipt_id"],
            "audit_id": audit_id,
            "status": "submitted" if receipt["status"] == "ready_to_pay" else "blocked",
            "payable_cents": receipt["payable_cents"],
            "missing_skus": receipt["missing_skus"],
        }

    scheduler = build_scheduler(namespace="example_5", config=CONFIG)
    report = await scheduler.execute()
    receipt = report.task_return_set["build_receipt"]
    final_order = report.task_return_set["finalize_order"]

    assert receipt["subtotal_cents"] == 7300
    assert receipt["discount_cents"] == 1460
    assert receipt["tax_cents"] == 467
    assert final_order == {
        "order_id": "order-1001",
        "receipt_id": "receipt-order-1001",
        "audit_id": "audit-order-1001",
        "status": "submitted",
        "payable_cents": 6307,
        "missing_skus": [],
    }


# --- Example 5 End ---


# --- Example 6 Risk Workflow: Async, Retry, Fan-in and Side Branches ---
async def example_6():
    """
    例子6: 异步风控/通知复合流。

    该例子展示：
    - 多个异步入口并发执行
    - fetch_credit_score 第一次失败后通过 retry 成功
    - 风控评分、动作选择、通知发送和归档形成多阶段汇聚
    """
    # ┌----------------------------------------------------------------------------┐
    # | fetch_profile      fetch_activity      fetch_payments                      |
    # |      |                  |                  |                               |
    # |      ├→ fetch_credit    └→ normalize       └→ detect_payment_flags         |
    # |      └→ build_policy              |                  |                     |
    # |                 |                 └--------→ score_risk ←------------------┘
    # |                 |                                  |                       |
    # |                 └--------------------------→ choose_action                 |
    # |                                                    |                       |
    # |                                  ┌-----------------┴----------------┐      |
    # |                                  ↓                                  ↓      |
    # |                           send_notification               archive_decision |
    # |                                  └-----------------┬----------------┘      |
    # |                                                    ↓                       |
    # |                                             final_summary                  |
    # └----------------------------------------------------------------------------┘

    credit_attempts = {"count": 0}

    @task("fetch_profile", namespace="example_6")
    async def fetch_profile() -> dict:
        await asyncio.sleep(0.01)
        return {
            "customer_id": "customer-42",
            "email": "ops@example.com",
            "region": "EU",
            "segment": "enterprise",
        }

    @task("fetch_activity", namespace="example_6")
    async def fetch_activity() -> dict:
        await asyncio.sleep(0.01)
        return {
            "login_count_7d": 3,
            "failed_login_count_7d": 2,
            "new_device": True,
        }

    @task("fetch_payments", namespace="example_6")
    async def fetch_payments() -> dict:
        await asyncio.sleep(0.01)
        return {
            "failed_payments_30d": 1,
            "chargebacks_90d": 0,
            "average_ticket_cents": 5500,
        }

    @task("fetch_credit_score", namespace="example_6", retry=1)
    async def fetch_credit_score(customer_id: Ref[str, F("fetch_profile", "customer_id")]) -> dict:
        await asyncio.sleep(0.01)
        credit_attempts["count"] += 1
        if credit_attempts["count"] == 1:
            raise RuntimeError(f"temporary credit provider timeout for {customer_id}")
        return {
            "provider": "mock-credit",
            "credit_score": 680,
        }

    @task("normalize_activity", namespace="example_6")
    async def normalize_activity(
        login_count: Ref[int, F("fetch_activity", "login_count_7d")],
        failed_login_count: Ref[int, F("fetch_activity", "failed_login_count_7d")],
        new_device: Ref[bool, F("fetch_activity", "new_device")],
    ) -> dict:
        await asyncio.sleep(0.01)
        activity_points = failed_login_count * 10 + (5 if new_device else 0) - min(login_count, 5)
        return {
            "activity_points": max(activity_points, 0),
            "activity_flags": ["new_device"] if new_device else [],
        }

    @task("detect_payment_flags", namespace="example_6")
    async def detect_payment_flags(
        failed_payments: Ref[int, F("fetch_payments", "failed_payments_30d")],
        chargebacks: Ref[int, F("fetch_payments", "chargebacks_90d")],
        average_ticket_cents: Ref[int, F("fetch_payments", "average_ticket_cents")],
    ) -> dict:
        await asyncio.sleep(0.01)
        flags = []
        if failed_payments:
            flags.append("recent_failed_payment")
        if chargebacks:
            flags.append("recent_chargeback")
        if average_ticket_cents >= 5000:
            flags.append("high_ticket")
        return {
            "payment_points": failed_payments * 25 + chargebacks * 45 + (10 if average_ticket_cents >= 5000 else 0),
            "payment_flags": flags,
        }

    @task("build_region_policy", namespace="example_6")
    async def build_region_policy(region: Ref[str, F("fetch_profile", "region")], segment: Ref[str, F("fetch_profile", "segment")]) -> dict:
        await asyncio.sleep(0.01)
        return {
            "region": region,
            "segment": segment,
            "region_points": 5 if region == "EU" else 0,
            "review_threshold": 60,
            "decline_threshold": 90,
            "notification_channel": "email",
        }

    @task("score_risk", namespace="example_6")
    async def score_risk(
        activity_points: Ref[int, F("normalize_activity", "activity_points")],
        payment_points: Ref[int, F("detect_payment_flags", "payment_points")],
        credit_score: Ref[int, F("fetch_credit_score", "credit_score")],
        region_points: Ref[int, F("build_region_policy", "region_points")],
        review_threshold: Ref[int, F("build_region_policy", "review_threshold")],
        decline_threshold: Ref[int, F("build_region_policy", "decline_threshold")],
    ) -> dict:
        await asyncio.sleep(0.01)
        credit_points = 20 if credit_score < 700 else 0
        total_score = activity_points + payment_points + credit_points + region_points
        return {
            "risk_score": total_score,
            "credit_points": credit_points,
            "review_threshold": review_threshold,
            "decline_threshold": decline_threshold,
        }

    @task("choose_action", namespace="example_6")
    async def choose_action(
        risk_score: Ref[int, F("score_risk", "risk_score")],
        review_threshold: Ref[int, F("score_risk", "review_threshold")],
        decline_threshold: Ref[int, F("score_risk", "decline_threshold")],
        channel: Ref[str, F("build_region_policy", "notification_channel")],
    ) -> dict:
        await asyncio.sleep(0.01)
        if risk_score >= decline_threshold:
            action = "decline"
        elif risk_score >= review_threshold:
            action = "manual_review"
        else:
            action = "approve"
        return {
            "action": action,
            "priority": "high" if action != "approve" else "normal",
            "channel": channel,
        }

    @task("send_notification", namespace="example_6")
    async def send_notification(
        email: Ref[str, F("fetch_profile", "email")],
        action: Ref[str, F("choose_action", "action")],
        channel: Ref[str, F("choose_action", "channel")],
    ) -> dict:
        await asyncio.sleep(0.01)
        return {
            "sent": True,
            "channel": channel,
            "message_id": f"{channel}:{email}:{action}",
        }

    @task("archive_decision", namespace="example_6")
    async def archive_decision(
        customer_id: Ref[str, F("fetch_profile", "customer_id")],
        action: Ref[str, F("choose_action", "action")],
        risk_score: Ref[int, F("score_risk", "risk_score")],
    ) -> dict:
        await asyncio.sleep(0.01)
        return {
            "archive_key": f"risk/{customer_id}/{action}",
            "risk_score": risk_score,
        }

    @task("final_summary", namespace="example_6")
    async def final_summary(
        action: Ref[str, F("choose_action", "action")],
        priority: Ref[str, F("choose_action", "priority")],
        risk_score: Ref[int, F("score_risk", "risk_score")],
        credit_score: Ref[int, F("fetch_credit_score", "credit_score")],
        notification: Ref[dict, F("send_notification")],
        archive_key: Ref[str, F("archive_decision", "archive_key")],
    ) -> dict:
        await asyncio.sleep(0.01)
        return {
            "status": "completed",
            "action": action,
            "priority": priority,
            "risk_score": risk_score,
            "credit_score": credit_score,
            "notified": notification["sent"],
            "archive_key": archive_key,
        }

    scheduler = build_scheduler(namespace="example_6", config=CONFIG)
    report = await scheduler.execute()
    summary = report.task_return_set["final_summary"]
    credit_stat = next(task_stat for task_stat in report.task_statistics if task_stat.task_name == "fetch_credit_score")

    assert report.execution_state == "completed"
    assert credit_attempts["count"] == 2
    assert credit_stat.attempt_count == 2
    assert summary == {
        "status": "completed",
        "action": "manual_review",
        "priority": "high",
        "risk_score": 82,
        "credit_score": 680,
        "notified": True,
        "archive_key": "risk/customer-42/manual_review",
    }


# --- Example 6 End ---


async def main():
    await example_1()
    await example_2()
    await example_3()
    await example_4()
    await example_5()
    await example_6()


if __name__ == "__main__":
    asyncio.run(main())
