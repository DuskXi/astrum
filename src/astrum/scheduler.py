from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from typing import Any, Union

from .models import (
    ExecutionReport,
    ExecutionState,
    StageStatus,
    TaskAttemptStatistics,
    TaskDuplicateExecutionError,
    TaskExecutionStatistics,
    TaskNotFoundError,
    TaskOrder,
    TaskStageStatistics,
)
from .planner import ExecutionPlanner
from .data_transport import TaskData

TaskCallable = Callable[..., Union[Awaitable[Any], Any]]


class DynamicScheduler:
    """Async DAG scheduler for predeclared task orders.

    每个任务以 ``(task_id, callable)`` 的形式传入；调度器在调度期才真正调用
    callable，从而支持基于 :class:`TaskData` 的动态参数装配。**不再支持**
    将"已 call 但未 run 的协程对象"作为 task；这种用法会在 ``__init__``
    阶段直接抛 :class:`TypeError`。

    Each task is passed in as ``(task_id, callable)``; the scheduler only actually
    calls the callable during scheduling, which supports dynamic parameter assembly
    based on :class:`TaskData`. Passing an "already called but not yet run coroutine
    object" as a task is **no longer supported**; that usage raises
    :class:`TypeError` directly during ``__init__``.
    """

    def __init__(
        self,
        tasks: list[tuple[str, TaskCallable]],
        task_order: list[TaskOrder],
        task_data_refs: dict[str, TaskData] | None = None,
        has_data_path: bool = False,
        ignore_tail_task: list[str] | None = None,
        concurrency_context: asyncio.Semaphore | None = None,
        *,
        task_retries: dict[str, int] | None = None,
        silence: bool = True,
    ) -> None:
        self.ignore_tail_task = ignore_tail_task or []
        self.task_data_refs = task_data_refs or {}
        self.has_data_path = has_data_path
        self.silence = silence
        self._validate_task_inputs(tasks)
        self.task_retries = task_retries or {}

        self.tasks = tasks
        self.task_order = task_order
        self.concurrency_context = concurrency_context

        self.execution_state = ExecutionState.NONE
        self.stage_status: StageStatus | None = None
        self.current_stage = -1
        self._task_outputs: dict[str, Any] = {}
        self._task_attempts: dict[str, list[TaskAttemptStatistics]] = {}
        self.task_return_set: dict[str, Any] = {}

    def detect_cycle(self) -> None:
        ExecutionPlanner(self.task_order).detect_cycle()

    def get_execute_timeline(self):
        return ExecutionPlanner(self.task_order).get_execute_timeline()

    def find_task_by_name(self, task_name: str) -> TaskCallable | None:
        for name, task in self.tasks:
            if name == task_name:
                return task
        return None

    async def execute(self) -> ExecutionReport:
        total_start_time = time.time()
        planning_start = time.time()
        self.current_stage = -1
        self.execution_state = ExecutionState.PLANNING
        self._task_attempts = {}

        try:
            plan = self.get_execute_timeline()
        except Exception as exc:
            self.execution_state = ExecutionState.FAILED
            return self._create_failed_report(total_start_time, time.time(), planning_start, str(exc))

        planning_end = time.time()
        execution_start = time.time()
        self.execution_state = ExecutionState.RUNNING

        stage_statistics: list[TaskStageStatistics] = []
        task_statistics: list[TaskExecutionStatistics] = []
        error_summary: list[str] = []
        task_map: dict[str, asyncio.Task[Any]] = {}
        task_start_times: dict[str, float] = {}
        completed_tasks_recorded: set[str] = set()

        execution_failed = False
        failed_task_name: str | None = None
        first_error: BaseException | None = None

        try:
            for stage in plan.stages:
                stage_start_time = time.time()
                self.current_stage = stage.stage_id
                self.stage_status = StageStatus.PENDING

                for task_name in stage.wait_for_tasks:
                    if task_name not in task_map:
                        continue

                    try:
                        self._task_outputs[task_name] = await task_map[task_name]
                        self._record_task_stat(
                            task_statistics,
                            completed_tasks_recorded,
                            task_name,
                            stage.stage_id,
                            task_start_times[task_name],
                            "completed",
                        )
                    except Exception as exc:
                        self._record_task_stat(
                            task_statistics,
                            completed_tasks_recorded,
                            task_name,
                            stage.stage_id,
                            task_start_times[task_name],
                            "failed",
                            str(exc),
                        )
                        error_summary.append(f"Task '{task_name}' failed during execution: {exc}")
                        execution_failed = True
                        failed_task_name = task_name
                        first_error = exc
                        self.execution_state = ExecutionState.FAILED
                        self.stage_status = StageStatus.FAILED
                        raise

                self.stage_status = StageStatus.RUNNING
                for task_name in stage.start_tasks:
                    if task_name in task_map:
                        raise TaskDuplicateExecutionError(task_name)

                    task_callable = self.find_task_by_name(task_name)
                    if task_callable is None:
                        raise TaskNotFoundError(task_name)

                    task_start_times[task_name] = time.time()
                    task_map[task_name] = asyncio.create_task(self._run_with_concurrency(self._invoke_task_with_retries(task_name, task_callable, error_summary)))

                self.stage_status = StageStatus.COMPLETED
                stage_end_time = time.time()
                stage_statistics.append(
                    TaskStageStatistics(
                        stage_id=stage.stage_id,
                        stage_name=f"Stage_{stage.stage_id}",
                        start_time=stage_start_time,
                        end_time=stage_end_time,
                        duration=stage_end_time - stage_start_time,
                        parallel_task_count=len(stage.parallel_tasks),
                        wait_task_count=len(stage.wait_for_tasks),
                        parallel_tasks=stage.parallel_tasks.copy(),
                        wait_tasks=stage.wait_for_tasks.copy(),
                    )
                )

            if not execution_failed:
                self.execution_state = ExecutionState.COMPLETED

        except Exception as exc:
            if not execution_failed:
                self.execution_state = ExecutionState.FAILED
                error_summary.append(f"Execution failed: {exc}")
                execution_failed = True
                first_error = exc

        tail_tasks: dict[str, asyncio.Task[Any]] = {}
        for task_name, task in task_map.items():
            if task_name in self.ignore_tail_task:
                tail_tasks[task_name] = task
                continue

            if task_name in completed_tasks_recorded:
                continue

            if execution_failed:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    self._record_task_stat(
                        task_statistics,
                        completed_tasks_recorded,
                        task_name,
                        -1,
                        task_start_times[task_name],
                        "cancelled",
                        "Task cancelled due to previous failure",
                    )
                except Exception as exc:
                    self._record_task_stat(
                        task_statistics,
                        completed_tasks_recorded,
                        task_name,
                        -1,
                        task_start_times[task_name],
                        "failed",
                        str(exc),
                    )
            else:
                try:
                    self._task_outputs[task_name] = await task
                    self._record_task_stat(
                        task_statistics,
                        completed_tasks_recorded,
                        task_name,
                        -1,
                        task_start_times[task_name],
                        "completed",
                    )
                except Exception as exc:
                    error_summary.append(f"Final task '{task_name}' failed: {exc}")
                    self._record_task_stat(
                        task_statistics,
                        completed_tasks_recorded,
                        task_name,
                        -1,
                        task_start_times[task_name],
                        "failed",
                        str(exc),
                    )
                    self.execution_state = ExecutionState.FAILED
                    execution_failed = True
                    if failed_task_name is None:
                        failed_task_name = task_name
                        first_error = exc

        if execution_failed and failed_task_name and first_error:
            error_summary.insert(0, f"Execution interrupted; task '{failed_task_name}' failed: {first_error}")

        total_end_time = time.time()
        successful_tasks = len([task for task in task_statistics if task.status == "completed"])
        failed_tasks = len([task for task in task_statistics if task.status == "failed"])

        return ExecutionReport(
            total_start_time=total_start_time,
            total_end_time=total_end_time,
            total_duration=total_end_time - total_start_time,
            planning_duration=planning_end - planning_start,
            execution_duration=total_end_time - execution_start,
            total_tasks=len(self.task_order),
            total_stages=len(plan.stages),
            max_parallelism=plan.max_parallelism,
            successful_tasks=successful_tasks,
            failed_tasks=failed_tasks,
            stage_statistics=stage_statistics,
            task_statistics=task_statistics,
            execution_state=self.execution_state.value,
            error_summary=error_summary,
            original_tasks=self._serialize_task_orders(),
            tail_tasks=tail_tasks,
            task_return_set=self.task_return_set,
        )

    async def _run_with_concurrency(self, task_coroutine: Awaitable[Any]) -> Any:
        if self.concurrency_context is None:
            return await task_coroutine

        async with self.concurrency_context:
            return await task_coroutine

    async def _invoke_task_with_retries(self, task_name: str, task_callable: TaskCallable, error_summary: list[str]) -> Any:
        retry = self.task_retries.get(task_name, 0)
        max_attempts = retry + 1

        for attempt_number in range(1, max_attempts + 1):
            attempt_start = time.time()
            try:
                result = await self._invoke_task(task_name, task_callable)
            except asyncio.CancelledError:
                self._record_attempt_stat(
                    task_name,
                    attempt_number,
                    retry,
                    attempt_start,
                    "cancelled",
                    "Task cancelled",
                    False,
                )
                raise
            except Exception as exc:
                will_retry = attempt_number < max_attempts
                self._record_attempt_stat(
                    task_name,
                    attempt_number,
                    retry,
                    attempt_start,
                    "failed",
                    str(exc),
                    will_retry,
                )

                if will_retry:
                    retries_left = max_attempts - attempt_number
                    error_summary.append(f"Task '{task_name}' attempt {attempt_number}/{max_attempts} failed; " f"retrying with {retries_left} retries left: {exc}")
                    continue

                if retry > 0:
                    error_summary.append(f"Task '{task_name}' exhausted {retry} retries after {max_attempts} attempts: {exc}")
                raise
            else:
                self._record_attempt_stat(
                    task_name,
                    attempt_number,
                    retry,
                    attempt_start,
                    "completed",
                    None,
                    False,
                )
                return result

        raise RuntimeError(f"Task '{task_name}' retry loop exited unexpectedly.")

    async def _invoke_task(self, task_name: str, task_callable: TaskCallable) -> Any:
        """调度期真正调用任务函数。

        这是把"调用时机"从注册期推迟到调度期的核心入口：所有依赖于调度上下文的
        参数装配/出参校验都应在此处完成。
        """

        task_data = self.task_data_refs.get(task_name)

        kwargs: dict[str, Any] = {}
        args: list[Any] = []

        if task_data:
            index_map = []
            for input_data in task_data.input_data_item:
                value = input_data.data_ref.data if input_data.data_ref else None
                if input_data.from_relation and input_data.from_relation.from_function:
                    func_res = input_data.from_relation.from_function()
                    value = await func_res if inspect.isawaitable(func_res) else func_res

                if input_data.to_relation:
                    if input_data.to_relation.key is not None:
                        kwargs[input_data.to_relation.key] = value
                    elif input_data.to_relation.index is not None:
                        index_map.append((input_data.to_relation.index, value))
                    else:
                        index_map.append((0, value))
            if index_map:
                index_min = min(map(lambda x: x[0], index_map))
                index_max = max(map(lambda x: x[0], index_map))
                args = [None] * (index_max - index_min + 1)
                for index, value in index_map:
                    args[index - index_min] = value

        result = task_callable(*args, **kwargs) if kwargs or args else task_callable()
        if inspect.isawaitable(result):
            result = await result

        self.task_return_set[task_name] = result


        if task_data:
            if len(task_data.output_data_item) == 1 and task_data.output_data_item[0].from_relation.key is None and task_data.output_data_item[0].from_relation.index is None:
                # 单输出且无 key/index 约束，直接把整个 result 作为下游输入
                task_data.output_data_item[0].data_ref.data = result
            else:
                if all([x.from_relation.key is not None for x in task_data.output_data_item]):
                    for output_data in task_data.output_data_item:
                        key = output_data.from_relation.key
                        if isinstance(result, dict):
                            output_data.data_ref.data = result[key]
                        elif hasattr(result, key):
                            output_data.data_ref.data = getattr(result, key)
                        else:
                            raise KeyError(f"Key {key} not found in result of {task_name}")
                elif all([x.from_relation.index is not None for x in task_data.output_data_item]):
                    result_list = list(result)
                    for output_data in task_data.output_data_item:
                        output_data.data_ref.data = result_list[output_data.from_relation.index]
                else:
                    raise ValueError(f"Invalid output_data_item configuration for task '{task_name}': mixed or missing key/index in from_relation")

        return result

    def _validate_task_inputs(self, tasks: list[tuple[str, Any]]) -> None:
        validation_errors: list[str] = []

        task_names = [name for name, _ in tasks]
        duplicate_names = sorted({name for name in task_names if task_names.count(name) > 1})
        if duplicate_names:
            validation_errors.append(f"Duplicate task names in tasks: {duplicate_names}")

        for index, (task_name, task_obj) in enumerate(tasks):
            if not isinstance(task_name, str):
                validation_errors.append(f"Task #{index} name is not a string: {type(task_name)}")
                continue

            if inspect.iscoroutine(task_obj):
                validation_errors.append(f"Task '{task_name}' 是已 call 的协程对象；当前版本不再支持，" "请改为直接传入函数引用（不要加括号）。")
                continue

            if not callable(task_obj):
                validation_errors.append(f"Task '{task_name}' 不是可调用对象，得到 {type(task_obj)}。")

        if validation_errors:
            raise TypeError("\n".join(validation_errors))

    def _record_task_stat(
        self,
        task_statistics: list[TaskExecutionStatistics],
        completed_tasks_recorded: set[str],
        task_name: str,
        stage_id: int,
        start_time: float,
        status: str,
        error_message: str | None = None,
    ) -> None:
        if task_name in completed_tasks_recorded:
            return

        end_time = time.time()
        attempt_statistics = self._task_attempts.get(task_name, [])
        task_statistics.append(
            TaskExecutionStatistics(
                task_name=task_name,
                stage_id=stage_id,
                start_time=start_time,
                end_time=end_time,
                duration=end_time - start_time,
                status=status,
                error_message=error_message,
                retry=self.task_retries.get(task_name, 0),
                attempt_count=len(attempt_statistics),
                attempt_statistics=attempt_statistics.copy(),
            )
        )
        completed_tasks_recorded.add(task_name)

    def _record_attempt_stat(
        self,
        task_name: str,
        attempt_number: int,
        max_retries: int,
        start_time: float,
        status: str,
        error_message: str | None,
        will_retry: bool,
    ) -> None:
        end_time = time.time()
        self._task_attempts.setdefault(task_name, []).append(
            TaskAttemptStatistics(
                task_name=task_name,
                attempt_number=attempt_number,
                max_retries=max_retries,
                start_time=start_time,
                end_time=end_time,
                duration=end_time - start_time,
                status=status,
                error_message=error_message,
                will_retry=will_retry,
            )
        )

    def _create_failed_report(
        self,
        start_time: float,
        end_time: float,
        planning_start: float,
        error_message: str,
    ) -> ExecutionReport:
        return ExecutionReport(
            total_start_time=start_time,
            total_end_time=end_time,
            total_duration=end_time - start_time,
            planning_duration=end_time - planning_start,
            execution_duration=0.0,
            total_tasks=0,
            total_stages=0,
            max_parallelism=0,
            successful_tasks=0,
            failed_tasks=0,
            stage_statistics=[],
            task_statistics=[],
            execution_state=ExecutionState.FAILED.value,
            error_summary=[error_message],
            task_return_set=self.task_return_set,
        )

    def _serialize_task_orders(self) -> dict[str, list[str]]:
        return {task.task_name: [dependency.task_name for dependency in task.dependencies] for task in self.task_order}
