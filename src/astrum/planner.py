from __future__ import annotations

from collections import Counter

from .models import (
    ExecutionPlan,
    ExecutionStage,
    TaskDependencyError,
    TaskOrder,
    TaskOrderLoopError,
    TaskOrderNoExitError,
)


class ExecutionPlanner:
    """Build execution stages from a predeclared DAG."""

    def __init__(self, task_order: list[TaskOrder]) -> None:
        self.task_order = task_order

    def validate(self) -> None:
        self._validate_unique_names()
        self._validate_known_dependencies()
        self.detect_cycle()

    def detect_cycle(self) -> None:
        """Detect dependency cycles by task name."""

        task_map = {task.task_name: task for task in self.task_order}
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def dfs(task: TaskOrder, path: list[str]) -> None:
            task_name = task.task_name
            if task_name in rec_stack:
                loop_start_idx = path.index(task_name) if task_name in path else 0
                loop_chain = path[loop_start_idx:] + [task_name]
                raise TaskOrderLoopError([f"Cycle detected: {' -> '.join(loop_chain)}"])

            if task_name in visited:
                return

            visited.add(task_name)
            rec_stack.add(task_name)

            for dependency in task.dependencies:
                known_dependency = task_map.get(dependency.task_name)
                if known_dependency is not None:
                    dfs(known_dependency, path + [task_name])

            rec_stack.remove(task_name)

        for task in self.task_order:
            if task.task_name not in visited:
                dfs(task, [])

    def get_execute_timeline(self) -> ExecutionPlan:
        """Validate the DAG and return a staged execution plan."""

        self.validate()

        if self.task_order and all(task.dependencies for task in self.task_order):
            raise TaskOrderNoExitError("Task order has no entry point; every task depends on another task.")

        return self._build_execution_stages()

    def _build_execution_stages(self) -> ExecutionPlan:
        task_map = {task.task_name: task for task in self.task_order}
        in_degree = {task.task_name: 0 for task in self.task_order}
        dependency_graph: dict[str, list[str]] = {task.task_name: [] for task in self.task_order}

        for task in self.task_order:
            for dependency in task.dependencies:
                dependency_graph[dependency.task_name].append(task.task_name)
                in_degree[task.task_name] += 1

        stages: list[ExecutionStage] = []
        stage_id = 0
        completed_tasks: set[str] = set()
        all_started_tasks: set[str] = set()
        running_tasks: set[str] = set()

        while len(completed_tasks) < len(self.task_order):
            ready_tasks = [task_name for task_name, degree in in_degree.items() if degree == 0 and task_name not in completed_tasks]

            if not ready_tasks:
                remaining_tasks = [name for name in task_map if name not in completed_tasks]
                raise TaskOrderLoopError([f"Cannot continue scheduling; remaining tasks may be cyclic: {remaining_tasks}"])

            wait_for_tasks: list[str] = []
            for task_name in ready_tasks:
                for dependency in task_map[task_name].dependencies:
                    if dependency.task_name not in wait_for_tasks:
                        wait_for_tasks.append(dependency.task_name)

            start_tasks = [task_name for task_name in ready_tasks if task_name not in all_started_tasks]
            continuing_tasks = [task_name for task_name in running_tasks if task_name not in wait_for_tasks]
            parallel_tasks = continuing_tasks + start_tasks

            stages.append(
                ExecutionStage(
                    stage_id=stage_id,
                    parallel_tasks=parallel_tasks,
                    wait_for_tasks=wait_for_tasks,
                    start_tasks=start_tasks,
                )
            )

            all_started_tasks.update(start_tasks)
            running_tasks.update(start_tasks)
            for task_name in wait_for_tasks:
                running_tasks.discard(task_name)

            for task_name in ready_tasks:
                completed_tasks.add(task_name)
                for dependent in dependency_graph[task_name]:
                    in_degree[dependent] -= 1

            stage_id += 1

        plan = ExecutionPlan(
            stages=stages,
            total_tasks=len(self.task_order),
            max_parallelism=max((len(stage.parallel_tasks) for stage in stages), default=0),
        )
        plan.set_original_tasks(self.task_order)
        return plan

    def _validate_unique_names(self) -> None:
        counts = Counter(task.task_name for task in self.task_order)
        duplicates = sorted(name for name, count in counts.items() if count > 1)
        if duplicates:
            raise TaskDependencyError(f"Duplicate task names in task_order: {duplicates}")

    def _validate_known_dependencies(self) -> None:
        task_names = {task.task_name for task in self.task_order}
        missing: list[str] = []

        for task in self.task_order:
            for dependency in task.dependencies:
                if dependency.task_name not in task_names:
                    missing.append(f"{task.task_name} -> {dependency.task_name}")

        if missing:
            raise TaskDependencyError(f"Task dependencies are not present in task_order: {missing}")
