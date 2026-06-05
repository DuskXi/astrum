from __future__ import annotations

import asyncio
import inspect

import pytest

from astrum import (
    DEFAULT_NAMESPACE,
    SchedulerRegistry,
    TaskRegistrationError,
    active_namespace,
    build_scheduler,
    build_task_orders,
    clear_registry,
    get_registry,
    run,
    task,
    use_namespace,
)


@pytest.fixture(autouse=True)
def _isolate_global_hub():
    clear_registry()
    yield
    clear_registry()


def test_registry_registers_tasks_and_builds_orders() -> None:
    workflow = SchedulerRegistry("unit")

    @workflow.task("load")
    async def load() -> None:
        return None

    @workflow.task("transform", depends_on=["load"])
    async def transform() -> None:
        return None

    task_orders = workflow.build_task_orders(["transform"])
    task_names = [task.task_name for task in task_orders]

    assert task_names == ["load", "transform"]
    assert task_orders[1].dependencies[0].task_name == "load"


def test_registry_rejects_duplicate_tasks() -> None:
    workflow = SchedulerRegistry("unit")

    @workflow.task("same")
    async def first() -> None:
        return None

    with pytest.raises(TaskRegistrationError):

        @workflow.task("same")
        async def second() -> None:
            return None


def test_registry_rejects_missing_dependencies() -> None:
    workflow = SchedulerRegistry("unit")

    @workflow.task("broken", depends_on=["missing"])
    async def broken() -> None:
        return None

    with pytest.raises(TaskRegistrationError):
        workflow.build_task_orders()


@pytest.mark.asyncio
async def test_registry_builds_scheduler_and_executes() -> None:
    workflow = SchedulerRegistry("unit")
    events: list[str] = []

    @workflow.task("extract")
    async def extract() -> None:
        await asyncio.sleep(0.01)
        events.append("extract")

    @workflow.task("load", depends_on=["extract"])
    async def load() -> None:
        events.append("load")

    report = await workflow.run(["load"])

    assert report.execution_state == "completed"
    assert events == ["extract", "load"]


@pytest.mark.asyncio
async def test_registry_allows_sync_tasks_by_wrapping_result() -> None:
    workflow = SchedulerRegistry("unit")
    events: list[str] = []

    @workflow.task("sync_task")
    def sync_task() -> None:
        events.append("sync_task")

    report = await workflow.run()

    assert report.execution_state == "completed"
    assert events == ["sync_task"]


def test_global_task_defaults_to_default_namespace() -> None:
    assert active_namespace() == DEFAULT_NAMESPACE

    @task("solo")
    async def solo() -> None:
        return None

    registry = get_registry()
    assert registry.name == DEFAULT_NAMESPACE
    assert "solo" in registry.get_all_tasks()


def test_global_task_namespace_kwarg_isolates_registries() -> None:
    @task("a", namespace="alpha")
    async def task_a() -> None:
        return None

    @task("b", namespace="beta")
    async def task_b() -> None:
        return None

    alpha = get_registry("alpha")
    beta = get_registry("beta")

    assert "a" in alpha.get_all_tasks() and "b" not in alpha.get_all_tasks()
    assert "b" in beta.get_all_tasks() and "a" not in beta.get_all_tasks()


def test_use_namespace_sets_active_namespace_and_restores_on_exit() -> None:
    assert active_namespace() == DEFAULT_NAMESPACE

    with use_namespace("outer"):
        assert active_namespace() == "outer"

        @task("only_outer")
        async def only_outer() -> None:
            return None

        with use_namespace("inner"):
            assert active_namespace() == "inner"

            @task("only_inner")
            async def only_inner() -> None:
                return None

        assert active_namespace() == "outer"

    assert active_namespace() == DEFAULT_NAMESPACE
    assert "only_outer" in get_registry("outer").get_all_tasks()
    assert "only_inner" in get_registry("inner").get_all_tasks()
    assert "only_outer" not in get_registry("inner").get_all_tasks()


def test_explicit_namespace_overrides_context_manager() -> None:
    with use_namespace("ctx"):

        @task("escaped", namespace="other")
        async def escaped() -> None:
            return None

    assert "escaped" not in get_registry("ctx").get_all_tasks()
    assert "escaped" in get_registry("other").get_all_tasks()


@pytest.mark.asyncio
async def test_build_scheduler_via_namespace_kwarg_and_context_manager_match() -> None:
    events: list[str] = []

    @task("extract", namespace="pipe")
    async def extract() -> None:
        events.append("extract")

    @task("load", depends_on=["extract"], namespace="pipe")
    async def load() -> None:
        events.append("load")

    orders_kwarg = build_task_orders(["load"], namespace="pipe")
    with use_namespace("pipe"):
        orders_ctx = build_task_orders(["load"])

    assert [o.task_name for o in orders_kwarg] == [o.task_name for o in orders_ctx]

    scheduler = build_scheduler(["load"], namespace="pipe")
    report = await scheduler.execute()
    assert report.execution_state == "completed"
    assert events == ["extract", "load"]


@pytest.mark.asyncio
async def test_module_level_run_with_context_manager() -> None:
    events: list[str] = []

    with use_namespace("ctx_run"):

        @task("a")
        async def task_a() -> None:
            events.append("a")

        @task("b", depends_on=["a"])
        async def task_b() -> None:
            events.append("b")

        report = await run(["b"])

    assert report.execution_state == "completed"
    assert events == ["a", "b"]


def test_build_tasks_returns_callables_not_coroutines() -> None:
    """build_tasks 改造后保留函数引用，由调度器在调度期才实际调用。"""

    workflow = SchedulerRegistry("unit")

    @workflow.task("a")
    async def a() -> None:
        return None

    @workflow.task("b", depends_on=["a"])
    def b() -> None:
        return None

    built = workflow.build_tasks()
    assert {name for name, _ in built} == {"a", "b"}
    for _, obj in built:
        assert callable(obj)
        assert not inspect.iscoroutine(obj)


def test_registry_task_retry_is_passed_to_scheduler() -> None:
    workflow = SchedulerRegistry("unit")

    @workflow.task("flaky", retry=2)
    async def flaky() -> None:
        return None

    scheduler = workflow.build_scheduler()

    assert workflow.get_task("flaky").retry == 2
    assert scheduler.task_retries == {"flaky": 2}


def test_global_task_retry_is_passed_to_scheduler() -> None:
    @task("flaky", namespace="retry_ns", retry=2)
    async def flaky() -> None:
        return None

    scheduler = build_scheduler(namespace="retry_ns")

    assert get_registry("retry_ns").get_task("flaky").retry == 2
    assert scheduler.task_retries == {"flaky": 2}


def test_build_task_orders_preserves_retry_metadata() -> None:
    workflow = SchedulerRegistry("unit")

    @workflow.task("source", retry=3)
    async def source() -> None:
        return None

    workflow.build_task_orders()

    assert workflow.get_task("source").retry == 3


def test_task_retry_must_be_non_negative_integer() -> None:
    workflow = SchedulerRegistry("unit")

    with pytest.raises(TaskRegistrationError):

        @workflow.task("negative", retry=-1)
        async def negative() -> None:
            return None

    with pytest.raises(TaskRegistrationError):

        @task("text", retry="one")  # type: ignore[arg-type]
        async def text() -> None:
            return None


def test_clear_registry_scopes() -> None:
    @task("x", namespace="ns1")
    async def x() -> None:
        return None

    @task("y", namespace="ns2")
    async def y() -> None:
        return None

    clear_registry("ns1")
    assert "x" not in get_registry("ns1").get_all_tasks()
    assert "y" in get_registry("ns2").get_all_tasks()

    clear_registry()
    assert get_registry("ns2").get_all_tasks() == {}
