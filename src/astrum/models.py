from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# --- Core ---


class ExecutionState(str, Enum):
    """Overall scheduler execution state."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    PLANNING = "planning"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NONE = "none"


class StageStatus(str, Enum):
    """Current execution stage state."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskOrder:
    """A task node and the task nodes that must complete before it can run."""

    task_name: str
    dependencies: list[TaskOrder] = field(default_factory=list)

    @property
    def depend(self) -> TaskOrder | None:
        """Backward-compatible single-dependency accessor."""

        return self.dependencies[0] if self.dependencies else None

    @depend.setter
    def depend(self, value: TaskOrder | None) -> None:
        self.dependencies = [] if value is None else [value]


@dataclass(frozen=True)
class ExecutionStage:
    """One planned scheduler stage."""

    stage_id: int
    parallel_tasks: list[str]
    wait_for_tasks: list[str]
    start_tasks: list[str]


@dataclass
class ExecutionPlan:
    """A topologically planned execution timeline."""

    stages: list[ExecutionStage]
    total_tasks: int
    max_parallelism: int
    original_tasks: list[TaskOrder] = field(default_factory=list, repr=False)

    def set_original_tasks(self, tasks: list[TaskOrder]) -> None:
        self.original_tasks = tasks

    def get_dependency_graph_info(self) -> str:
        if not self.original_tasks:
            return "No task dependency information."

        lines = ["Task dependency graph:"]
        for task in self.original_tasks:
            dependencies = ", ".join(dep.task_name for dep in task.dependencies) or "none"
            lines.append(f"- {task.task_name}: depends on [{dependencies}]")
        return "\n".join(lines)

    def get_visualization_table(self) -> str:
        if not self.stages:
            return "No execution stages."

        lines = [f"Execution plan ({len(self.stages)} stages):"]
        for stage in self.stages:
            lines.append(f"- stage {stage.stage_id}: start={stage.start_tasks}, " f"wait={stage.wait_for_tasks}, parallel={stage.parallel_tasks}")
        return "\n".join(lines)

    def get_full_visualization(self) -> str:
        return f"{self.get_dependency_graph_info()}\n\n{self.get_visualization_table()}"


@dataclass(frozen=True)
class TaskStageStatistics:
    stage_id: int
    stage_name: str
    start_time: float
    end_time: float
    duration: float
    parallel_task_count: int
    wait_task_count: int
    parallel_tasks: list[str]
    wait_tasks: list[str]


@dataclass(frozen=True)
class TaskAttemptStatistics:
    task_name: str
    attempt_number: int
    max_retries: int
    start_time: float
    end_time: float
    duration: float
    status: str
    error_message: str | None = None
    will_retry: bool = False


@dataclass(frozen=True)
class TaskExecutionStatistics:
    task_name: str
    stage_id: int
    start_time: float
    end_time: float
    duration: float
    status: str
    error_message: str | None = None
    retry: int = 0
    attempt_count: int = 0
    attempt_statistics: list[TaskAttemptStatistics] = field(default_factory=list)


@dataclass
class ExecutionReport:
    total_start_time: float
    total_end_time: float
    total_duration: float
    planning_duration: float
    execution_duration: float
    total_tasks: int
    total_stages: int
    max_parallelism: int
    successful_tasks: int
    failed_tasks: int
    stage_statistics: list[TaskStageStatistics]
    task_statistics: list[TaskExecutionStatistics]
    execution_state: str
    error_summary: list[str] = field(default_factory=list)
    original_tasks: dict[str, list[str]] = field(default_factory=dict)
    tail_tasks: dict[str, asyncio.Task[Any]] = field(default_factory=dict)
    task_return_set: dict[str, Any] = field(default_factory=list)

    def get_task_statistics_by_stage(self, stage_id: int) -> list[TaskExecutionStatistics]:
        return [task for task in self.task_statistics if task.stage_id == stage_id]

    def get_longest_task(self) -> TaskExecutionStatistics | None:
        if not self.task_statistics:
            return None
        return max(self.task_statistics, key=lambda task: task.duration)

    def get_average_task_duration(self) -> float:
        if not self.task_statistics:
            return 0.0
        return sum(task.duration for task in self.task_statistics) / len(self.task_statistics)


class SchedulerError(Exception):
    """Base class for scheduler errors."""


class TaskOrderLoopError(SchedulerError):
    """Raised when the DAG contains a cycle."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("\n".join(errors))


class TaskOrderNoExitError(SchedulerError):
    """Raised when no task can be used as an entry point."""


class TaskDependencyError(SchedulerError):
    """Raised when a task depends on an unknown task."""


class TaskNotFoundError(SchedulerError):
    """Raised when a planned task has no matching coroutine."""

    def __init__(self, task_name: str) -> None:
        self.task_name = task_name
        super().__init__(f"Task '{task_name}' not found in task list.")


class TaskDuplicateExecutionError(SchedulerError):
    """Raised when a task would be scheduled more than once."""

    def __init__(self, task_name: str) -> None:
        self.task_name = task_name
        super().__init__(f"Task '{task_name}' is already scheduled for execution.")


class TaskRegistrationError(SchedulerError):
    """Raised when decorator-based registration is invalid."""


#
