from __future__ import annotations

import pytest

from astrum import (
    ExecutionPlanner,
    TaskDependencyError,
    TaskOrder,
    TaskOrderLoopError,
)


def test_planner_builds_parallel_stages() -> None:
    task_a = TaskOrder(task_name="a")
    task_b = TaskOrder(task_name="b")
    task_c = TaskOrder(task_name="c", dependencies=[task_a, task_b])

    plan = ExecutionPlanner([task_a, task_b, task_c]).get_execute_timeline()

    assert plan.total_tasks == 3
    assert plan.max_parallelism == 2
    assert set(plan.stages[0].start_tasks) == {"a", "b"}
    assert plan.stages[1].wait_for_tasks == ["a", "b"]
    assert plan.stages[1].start_tasks == ["c"]


def test_planner_detects_cycle() -> None:
    task_a = TaskOrder(task_name="a")
    task_b = TaskOrder(task_name="b", dependencies=[task_a])
    task_a.dependencies = [task_b]

    with pytest.raises(TaskOrderLoopError):
        ExecutionPlanner([task_a, task_b]).get_execute_timeline()


def test_planner_rejects_missing_dependency() -> None:
    external = TaskOrder(task_name="external")
    task_a = TaskOrder(task_name="a", dependencies=[external])

    with pytest.raises(TaskDependencyError):
        ExecutionPlanner([task_a]).get_execute_timeline()


def test_planner_rejects_duplicate_task_names() -> None:
    with pytest.raises(TaskDependencyError):
        ExecutionPlanner([TaskOrder("same"), TaskOrder("same")]).get_execute_timeline()
