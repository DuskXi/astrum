from __future__ import annotations

import asyncio

import pytest

from astrum import DynamicScheduler, TaskOrder


@pytest.mark.asyncio
async def test_scheduler_executes_dependencies_in_order() -> None:
    events: list[str] = []

    async def a() -> None:
        await asyncio.sleep(0.01)
        events.append("a")

    async def b() -> None:
        await asyncio.sleep(0.01)
        events.append("b")

    async def c() -> None:
        events.append("c")

    task_a = TaskOrder(task_name="a")
    task_b = TaskOrder(task_name="b")
    task_c = TaskOrder(task_name="c", dependencies=[task_a, task_b])

    scheduler = DynamicScheduler(
        tasks=[("a", a), ("b", b), ("c", c)],
        task_order=[task_a, task_b, task_c],
    )

    report = await scheduler.execute()

    assert report.execution_state == "completed"
    assert report.successful_tasks == 3
    assert events[-1] == "c"
    assert set(events[:2]) == {"a", "b"}


@pytest.mark.asyncio
async def test_scheduler_cancels_remaining_tasks_after_failure() -> None:
    async def a() -> None:
        raise RuntimeError("boom")

    async def b() -> None:
        await asyncio.sleep(0.2)

    async def c() -> None:
        await asyncio.sleep(0.01)

    task_a = TaskOrder(task_name="a")
    task_b = TaskOrder(task_name="b")
    task_c = TaskOrder(task_name="c", dependencies=[task_a, task_b])

    scheduler = DynamicScheduler(
        tasks=[("a", a), ("b", b), ("c", c)],
        task_order=[task_a, task_b, task_c],
    )

    report = await scheduler.execute()

    assert report.execution_state == "failed"
    assert report.failed_tasks == 1
    assert any("boom" in message for message in report.error_summary)
    statuses = {task.task_name: task.status for task in report.task_statistics}
    assert statuses["a"] == "failed"
    assert statuses["b"] == "cancelled"


def test_scheduler_rejects_already_called_coroutine() -> None:
    async def a() -> None:
        return None

    coro = a()
    try:
        with pytest.raises(TypeError) as excinfo:
            DynamicScheduler(tasks=[("a", coro)], task_order=[TaskOrder("a")])  # type: ignore[list-item]
        assert "已 call 的协程" in str(excinfo.value)
    finally:
        coro.close()


def test_scheduler_rejects_non_callable() -> None:
    with pytest.raises(TypeError):
        DynamicScheduler(tasks=[("a", 123)], task_order=[TaskOrder("a")])  # type: ignore[list-item]


@pytest.mark.asyncio
async def test_scheduler_retry_zero_preserves_failure_behavior() -> None:
    attempts = 0

    async def a() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("boom")

    scheduler = DynamicScheduler(
        tasks=[("a", a)],
        task_order=[TaskOrder("a")],
        task_retries={"a": 0},
    )

    report = await scheduler.execute()

    assert attempts == 1
    assert report.execution_state == "failed"
    assert report.failed_tasks == 1
    task_stat = report.task_statistics[0]
    assert task_stat.retry == 0
    assert task_stat.attempt_count == 1
    assert [attempt.status for attempt in task_stat.attempt_statistics] == ["failed"]
    assert not any("exhausted" in message for message in report.error_summary)


@pytest.mark.asyncio
async def test_scheduler_retries_until_task_succeeds() -> None:
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError(f"boom {attempts}")
        return "ok"

    scheduler = DynamicScheduler(
        tasks=[("flaky", flaky)],
        task_order=[TaskOrder("flaky")],
        task_retries={"flaky": 2},
    )

    report = await scheduler.execute()

    assert report.execution_state == "completed"
    assert attempts == 3
    task_stat = report.task_statistics[0]
    assert task_stat.retry == 2
    assert task_stat.attempt_count == 3
    assert [attempt.status for attempt in task_stat.attempt_statistics] == ["failed", "failed", "completed"]
    assert task_stat.attempt_statistics[0].will_retry is True
    assert task_stat.attempt_statistics[1].will_retry is True
    assert task_stat.attempt_statistics[2].will_retry is False
    assert any("attempt 1/3 failed; retrying with 2 retries left" in message for message in report.error_summary)
    assert any("attempt 2/3 failed; retrying with 1 retries left" in message for message in report.error_summary)


@pytest.mark.asyncio
async def test_scheduler_records_exhausted_retries() -> None:
    attempts = 0

    async def flaky() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError(f"boom {attempts}")

    scheduler = DynamicScheduler(
        tasks=[("flaky", flaky)],
        task_order=[TaskOrder("flaky")],
        task_retries={"flaky": 1},
    )

    report = await scheduler.execute()

    assert report.execution_state == "failed"
    assert attempts == 2
    task_stat = report.task_statistics[0]
    assert task_stat.retry == 1
    assert task_stat.attempt_count == 2
    assert [attempt.status for attempt in task_stat.attempt_statistics] == ["failed", "failed"]
    assert task_stat.attempt_statistics[0].will_retry is True
    assert task_stat.attempt_statistics[1].will_retry is False
    assert any("attempt 1/2 failed; retrying with 1 retries left" in message for message in report.error_summary)
    assert any("exhausted 1 retries after 2 attempts" in message for message in report.error_summary)


@pytest.mark.asyncio
async def test_scheduler_retry_success_does_not_cancel_parallel_task() -> None:
    attempts = 0
    events: list[str] = []

    async def flaky() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary")
        events.append("flaky")

    async def stable() -> None:
        await asyncio.sleep(0.01)
        events.append("stable")

    scheduler = DynamicScheduler(
        tasks=[("flaky", flaky), ("stable", stable)],
        task_order=[TaskOrder("flaky"), TaskOrder("stable")],
        task_retries={"flaky": 1},
    )

    report = await scheduler.execute()

    assert report.execution_state == "completed"
    assert report.successful_tasks == 2
    assert sorted(events) == ["flaky", "stable"]


@pytest.mark.asyncio
async def test_scheduler_does_not_retry_cancelled_task() -> None:
    async def failing() -> None:
        raise RuntimeError("boom")

    async def slow() -> None:
        await asyncio.sleep(1)

    scheduler = DynamicScheduler(
        tasks=[("failing", failing), ("slow", slow)],
        task_order=[TaskOrder("failing"), TaskOrder("slow")],
        task_retries={"slow": 2},
    )

    report = await scheduler.execute()

    statuses = {task.task_name: task for task in report.task_statistics}
    assert statuses["slow"].status == "cancelled"
    assert statuses["slow"].retry == 2
    assert statuses["slow"].attempt_count <= 1
    if statuses["slow"].attempt_statistics:
        assert [attempt.status for attempt in statuses["slow"].attempt_statistics] == ["cancelled"]
        assert statuses["slow"].attempt_statistics[0].will_retry is False
