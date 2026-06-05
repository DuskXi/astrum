from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any

from .config import AstrumConfig
from .data_transport import TaskData, resolve_task_data, autocast_data_transports_path, final_check, auto_generate_data_transports
from .models import TaskOrder, TaskRegistrationError
from .planner import ExecutionPlanner
from .scheduler import DynamicScheduler

DEFAULT_NAMESPACE = "default"


@dataclass(frozen=True)
class RegisteredTask:
    """Metadata captured by the decorator DAG builder."""

    task_id: str
    function: Callable[..., Awaitable[Any] | Any]
    depends_on: tuple[str, ...]
    retry: int
    data: TaskData | None = None


def _merge_task_data(explicit_data: TaskData | None, generated_data: TaskData) -> TaskData:
    if explicit_data is None:
        return generated_data

    explicit_data.task_id = explicit_data.task_id or generated_data.task_id
    if explicit_data.from_tasks is None:
        explicit_data.from_tasks = []
    if explicit_data.to_tasks is None:
        explicit_data.to_tasks = []

    for source_task in generated_data.from_tasks or []:
        if source_task not in explicit_data.from_tasks:
            explicit_data.from_tasks.append(source_task)
    for target_task in generated_data.to_tasks or []:
        if target_task not in explicit_data.to_tasks:
            explicit_data.to_tasks.append(target_task)

    for generated_item in generated_data.input_data_item:
        if not any(_same_input_slot(existing_item, generated_item) for existing_item in explicit_data.input_data_item):
            explicit_data.input_data_item.append(generated_item)

    for generated_item in generated_data.output_data_item:
        if not any(_same_output_slot(existing_item, generated_item) for existing_item in explicit_data.output_data_item):
            explicit_data.output_data_item.append(generated_item)

    return explicit_data


def _same_input_slot(left: Any, right: Any) -> bool:
    return _same_relation(left.to_relation, right.to_relation) or _same_data_item(left, right)


def _same_output_slot(left: Any, right: Any) -> bool:
    return _same_relation(left.from_relation, right.from_relation) or _same_relation(left.to_relation, right.to_relation) or _same_data_item(left, right)


def _same_data_item(left: Any, right: Any) -> bool:
    return _same_relation(left.from_relation, right.from_relation) and _same_relation(left.to_relation, right.to_relation)


def _same_relation(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    return left.related_task == right.related_task and left.key == right.key and left.index == right.index and left.single_item == right.single_item and left.from_function == right.from_function


class SchedulerRegistry:
    """Decorator-based DAG builder.

    This module is intentionally isolated from the core scheduler: it only
    converts decorated functions into the same inputs used by manual DAGs.
    """

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._tasks: dict[str, RegisteredTask] = {}

    def task(
        self, task_id: str | None = None, *, depends_on: list[str] | tuple[str, ...] | None = None, data: TaskData | None = None, retry: int = 0
    ) -> Callable[[Callable[..., Awaitable[Any] | Any]], Callable[..., Awaitable[Any] | Any]]:
        """Register a function as a DAG task in this registry."""

        return _make_task_decorator(self, task_id=task_id, depends_on=depends_on, data=data, retry=retry)

    def get_task(self, task_id: str) -> RegisteredTask:
        try:
            return self._tasks[task_id]
        except KeyError as exc:
            raise TaskRegistrationError(f"Task '{task_id}' is not registered in '{self.name}'.") from exc

    def get_all_tasks(self) -> dict[str, RegisteredTask]:
        return self._tasks.copy()

    def clear(self) -> None:
        self._tasks.clear()

    def build_task_orders(self, target_tasks: list[str] | None = None, config: AstrumConfig | None = None) -> list[TaskOrder]:
        cfg = config or AstrumConfig()

        all_task_orders = [TaskOrder(task_name=task_id) for task_id in self._tasks]
        all_task_func_map = {task_id: registered.function for task_id, registered in self._tasks.items()}
        generated_transports = auto_generate_data_transports(all_task_orders, all_task_func_map)
        for generated in generated_transports:
            registered_task = self._tasks[generated.task_id]
            merged_data = _merge_task_data(registered_task.data, generated)
            self._tasks[generated.task_id] = RegisteredTask(
                task_id=registered_task.task_id,
                function=registered_task.function,
                depends_on=registered_task.depends_on,
                retry=registered_task.retry,
                data=merged_data,
            )
            setattr(registered_task.function, "_scheduler_task_data", merged_data)

        selected_task_ids = self._collect_task_ids(target_tasks)
        order_map = {task_id: TaskOrder(task_name=task_id) for task_id in selected_task_ids}

        for task_id in selected_task_ids:
            registered_task = self._tasks[task_id]
            order_map[task_id].dependencies = [order_map[dependency] for dependency in registered_task.depends_on if dependency in order_map]

        task_orders = [order_map[task_id] for task_id in selected_task_ids]
        task_func_map = {tid: self._tasks[tid].function for tid in selected_task_ids}

        # 校验并补全数据传输关系
        task_transports: list[TaskData] = [self._tasks[tid].data for tid in selected_task_ids if self._tasks[tid].data is not None]
        resolve_task_data(
            task_orders,
            task_transports,
            allow_no_dir_definition=cfg.allow_no_dir_definition,
            infer_via_ast=cfg.infer_via_ast,
            silence_warnings=cfg.silence_warnings,
        )
        autocast_data_transports_path(task_transports, task_orders)

        # 自动将 data transport 推导出的 from_tasks 同步回任务图的 dependencies
        if cfg.auto_sync_dependencies:
            for task_id, task_order in order_map.items():
                dt = next((d for d in task_transports if d.task_id == task_id), None)
                if dt and dt.from_tasks:
                    for from_id in dt.from_tasks:
                        if from_id in order_map and order_map[from_id] not in task_order.dependencies:
                            task_order.dependencies.append(order_map[from_id])

        # 类型安全校验 (final_check)
        if not cfg.skip_type_check:
            errors = final_check(
                task_transports,
                task_orders,
                task_func_map,
                skip_type_check=cfg.skip_type_check,
                infer_via_ast=cfg.infer_via_ast,
                strict_topology=cfg.strict_topology,
            )
            if errors:
                raise TaskRegistrationError(f"Data transport validation failed with the following errors:\n- " + "\n- ".join(errors))

        ExecutionPlanner(task_orders).validate()

        if cfg.visualize:
            from .data_transport import visualize_data_transport

            visualize_data_transport(task_transports, task_orders)

        return task_orders

    def build_tasks(self, target_tasks: list[str] | None = None) -> list[tuple[str, Callable[..., Awaitable[Any] | Any]]]:
        """返回 (task_id, callable) 列表。

        改造前这里会立刻 ``func()`` 把任务定格成无参协程；改造后保留函数引用，
        由 :class:`DynamicScheduler` 在调度期决定何时调用以及如何注入参数。

        Return a list of (task_id, callable) pairs.

        Before the refactor, this would immediately call ``func()`` and freeze the
        task as a no-argument coroutine; after the refactor, it keeps the function
        reference and lets :class:`DynamicScheduler` decide when to call it and how
        to inject parameters during scheduling.
        """

        selected_task_ids = self._collect_task_ids(target_tasks)
        return [(task_id, self._tasks[task_id].function) for task_id in selected_task_ids]

    def build_scheduler(
        self,
        target_tasks: list[str] | None = None,
        *,
        config: AstrumConfig | None = None,
        # 以下为向后兼容的遗留参数，优先使用 config
        ignore_tail_task: list[str] | None = None,
        concurrency_context: asyncio.Semaphore | None = None,
        silence: bool = True,
        visualize: bool = False,
    ) -> DynamicScheduler:
        # 如果用户传了 config 就用 config，否则从散装参数构建
        if config is None:
            config = AstrumConfig(
                visualize=visualize,
                silence=silence,
                ignore_tail_task=ignore_tail_task or [],
            )

        task_orders = self.build_task_orders(target_tasks, config=config)
        tasks = self.build_tasks(target_tasks)

        selected_task_ids = self._collect_task_ids(target_tasks)
        task_data_refs: dict[str, TaskData] = {task_id: self._tasks[task_id].data for task_id in selected_task_ids if self._tasks[task_id].data is not None}
        task_retries = {task_id: self._tasks[task_id].retry for task_id in selected_task_ids if self._tasks[task_id].retry > 0}

        sem = concurrency_context or config.build_semaphore()

        return DynamicScheduler(
            tasks=tasks,
            task_order=task_orders,
            task_data_refs=task_data_refs or None,
            has_data_path=bool(task_data_refs),
            ignore_tail_task=config.ignore_tail_task or ignore_tail_task,
            concurrency_context=sem,
            task_retries=task_retries or None,
            silence=config.silence,
        )

    async def run(self, target_tasks: list[str] | None = None, config: AstrumConfig | None = None):
        scheduler = self.build_scheduler(target_tasks, config=config)
        return await scheduler.execute()

    def _collect_task_ids(self, target_tasks: list[str] | None) -> list[str]:
        if target_tasks is None:
            selected = list(self._tasks.keys())
        else:
            selected_set: set[str] = set()

            def collect(task_id: str) -> None:
                if task_id in selected_set:
                    return
                if task_id not in self._tasks:
                    raise TaskRegistrationError(f"Task '{task_id}' is not registered in '{self.name}'.")

                selected_set.add(task_id)
                for dependency in self._tasks[task_id].depends_on:
                    collect(dependency)
                task_data = self._tasks[task_id].data
                if task_data and task_data.from_tasks:
                    for dependency in task_data.from_tasks:
                        collect(dependency)

            for task_id in target_tasks:
                collect(task_id)

            selected = [task_id for task_id in self._tasks if task_id in selected_set]

        missing_dependencies = sorted({dependency for task_id in selected for dependency in self._tasks[task_id].depends_on if dependency not in self._tasks})
        missing_dependencies.extend(
            dependency
            for task_id in selected
            for dependency in (self._tasks[task_id].data.from_tasks if self._tasks[task_id].data and self._tasks[task_id].data.from_tasks else [])
            if dependency not in self._tasks and dependency not in missing_dependencies
        )
        if missing_dependencies:
            raise TaskRegistrationError(f"Registry '{self.name}' has unknown dependencies: {missing_dependencies}")

        return selected


def _register_function(
    registry: SchedulerRegistry, func: Callable[..., Awaitable[Any] | Any], *, task_id: str | None, depends_on: tuple[str, ...], data: TaskData | None = None, retry: int = 0
) -> Callable[..., Awaitable[Any] | Any]:
    """共享的底层注册逻辑：把 ``func`` 写入 ``registry._tasks`` 并打上元数据。

    ``SchedulerRegistry.task`` 和模块级 ``task`` 装饰器都通过这里落地，
    任何关于"如何注册一个任务"的改动只需要修改这一个函数。
    """

    resolved_task_id = task_id or func.__name__
    if resolved_task_id in registry._tasks:
        raise TaskRegistrationError(f"Task '{resolved_task_id}' already exists in registry '{registry.name}'.")
    _validate_retry(retry, resolved_task_id)

    if data is not None:
        # 自动处理 data 部分字段，使其与装饰器声明保持一致。
        data.task_id = resolved_task_id
        if data.from_tasks is None:
            data.from_tasks = []
        for dependency in depends_on:
            if dependency not in data.from_tasks:
                data.from_tasks.append(dependency)
        setattr(func, "_scheduler_task_data", data)

    registry._tasks[resolved_task_id] = RegisteredTask(task_id=resolved_task_id, function=func, depends_on=depends_on, data=data, retry=retry)

    setattr(func, "_scheduler_task_id", resolved_task_id)
    setattr(func, "_scheduler_registry", registry.name)
    setattr(func, "_scheduler_depends_on", depends_on)
    setattr(func, "_scheduler_retry", retry)
    return func


def _validate_retry(retry: int, task_id: str) -> None:
    if isinstance(retry, bool) or not isinstance(retry, int) or retry < 0:
        raise TaskRegistrationError(f"Task '{task_id}' retry must be a non-negative integer.")


def _make_task_decorator(
    registry: SchedulerRegistry, *, task_id: str | None, depends_on: list[str] | tuple[str, ...] | None, data: TaskData | None = None, retry: int = 0
) -> Callable[[Callable[..., Awaitable[Any] | Any]], Callable[..., Awaitable[Any] | Any]]:
    """构造一个绑定到指定 ``registry`` 的 ``@task`` 装饰器。"""

    dependencies = tuple(depends_on or ())

    def decorator(func: Callable[..., Awaitable[Any] | Any]) -> Callable[..., Awaitable[Any] | Any]:
        return _register_function(registry, func, task_id=task_id, depends_on=dependencies, data=data, retry=retry)

    return decorator


class _RegistryHub:
    """全局命名空间注册中心。

    通过 ContextVar 维护激活命名空间栈，对 async / 多线程调用安全，
    且 ``use_namespace`` 可以嵌套使用。
    """

    def __init__(self) -> None:
        self._registries: dict[str, SchedulerRegistry] = {}
        self._stack: ContextVar[tuple[str, ...]] = ContextVar("_astrum_namespace_stack", default=())

    def active(self) -> str:
        stack = self._stack.get()
        return stack[-1] if stack else DEFAULT_NAMESPACE

    def resolve(self, namespace: str | None) -> str:
        return namespace if namespace is not None else self.active()

    def get(self, namespace: str | None = None) -> SchedulerRegistry:
        ns = self.resolve(namespace)
        registry = self._registries.get(ns)
        if registry is None:
            registry = SchedulerRegistry(ns)
            self._registries[ns] = registry
        return registry

    def push(self, namespace: str) -> Token[tuple[str, ...]]:
        return self._stack.set(self._stack.get() + (namespace,))

    def reset(self, token: Token[tuple[str, ...]]) -> None:
        self._stack.reset(token)

    def clear(self, namespace: str | None = None) -> None:
        if namespace is None:
            self._registries.clear()
        else:
            self._registries.pop(namespace, None)

    def namespaces(self) -> list[str]:
        return list(self._registries.keys())


_HUB = _RegistryHub()


@contextmanager
def use_namespace(namespace: str) -> Iterator[SchedulerRegistry]:
    """临时把激活命名空间切换为 ``namespace``。

    ``with use_namespace("analytics"):`` 块内的 ``@task`` 装饰器与
    ``build_scheduler`` / ``run`` 等模块级函数将默认作用于该命名空间。
    支持嵌套，退出 ``with`` 后会自动恢复上一层命名空间。

    Temporarily switch the active namespace to ``namespace``.

    Inside a ``with use_namespace("analytics"):`` block, the ``@task`` decorator and
    module-level functions such as ``build_scheduler`` / ``run`` default to that
    namespace. Nesting is supported; after leaving the ``with`` block, the previous
    namespace is restored automatically.
    """

    token = _HUB.push(namespace)
    try:
        yield _HUB.get(namespace)
    finally:
        _HUB.reset(token)


def task(
    task_id: str | None = None, *, depends_on: list[str] | tuple[str, ...] | None = None, data: TaskData | None = None, namespace: str | None = None, retry: int = 0
) -> Callable[[Callable[..., Awaitable[Any] | Any]], Callable[..., Awaitable[Any] | Any]]:
    """模块级任务装饰器。

    除了多出的 ``namespace`` 参数外，签名和行为与 :meth:`SchedulerRegistry.task`
    完全一致——两者都委托到 :func:`_register_function`，未来改进底层注册行为
    只需要修改那一个函数。

    解析命名空间的优先级：显式 ``namespace=`` 参数 >
    ``use_namespace`` 上下文栈顶 > :data:`DEFAULT_NAMESPACE`。注意命名空间
    会在 ``@`` 装饰发生时即时解析，因此把 ``task(...)`` 调用放在
    ``with use_namespace(...)`` 块内即可获得上下文绑定的效果。

    Module-level task decorator.

    Except for the extra ``namespace`` parameter, its signature and behavior are
    exactly the same as :meth:`SchedulerRegistry.task`: both delegate to
    :func:`_register_function`, so future changes to the underlying registration
    behavior only need to modify that one function.

    Namespace resolution priority: explicit ``namespace=`` parameter >
    top of the ``use_namespace`` context stack > :data:`DEFAULT_NAMESPACE`. Note
    that the namespace is resolved immediately when the ``@`` decoration happens,
    so placing the ``task(...)`` call inside a ``with use_namespace(...)`` block is
    enough to get the context-bound effect.
    """

    dependencies = tuple(depends_on or ())

    def decorator(func: Callable[..., Awaitable[Any] | Any]) -> Callable[..., Awaitable[Any] | Any]:
        registry = _HUB.get(namespace)
        return _register_function(registry, func, task_id=task_id, depends_on=dependencies, data=data, retry=retry)

    return decorator


def get_registry(namespace: str | None = None) -> SchedulerRegistry:
    """返回指定命名空间对应的 :class:`SchedulerRegistry`（不存在则按需创建）。

    Return the :class:`SchedulerRegistry` for the specified namespace, creating it
    on demand if it does not exist.
    """

    return _HUB.get(namespace)


def clear_registry(namespace: str | None = None) -> None:
    """清除全局 hub 中的注册表。``namespace=None`` 时清空所有命名空间。

    Clear registries in the global hub. When ``namespace=None``, clear all namespaces.
    """

    _HUB.clear(namespace)


def active_namespace() -> str:
    """返回当前激活的命名空间名。

    Return the name of the currently active namespace.
    """

    return _HUB.active()


def build_task_orders(
    target_tasks: list[str] | None = None,
    *,
    namespace: str | None = None,
    config: AstrumConfig | None = None,
    visualize: bool = False,
) -> list[TaskOrder]:
    if config is None:
        config = AstrumConfig(visualize=visualize)
    return _HUB.get(namespace).build_task_orders(target_tasks, config=config)


def build_scheduler(
    target_tasks: list[str] | None = None,
    *,
    namespace: str | None = None,
    config: AstrumConfig | None = None,
    # 以下为向后兼容的遗留参数
    ignore_tail_task: list[str] | None = None,
    concurrency_context: asyncio.Semaphore | None = None,
    silence: bool = True,
    visualize: bool = False,
) -> DynamicScheduler:
    if config is None:
        config = AstrumConfig(
            visualize=visualize,
            silence=silence,
            ignore_tail_task=ignore_tail_task or [],
        )
    return _HUB.get(namespace).build_scheduler(
        target_tasks,
        config=config,
        concurrency_context=concurrency_context,
    )


async def run(
    target_tasks: list[str] | None = None,
    *,
    namespace: str | None = None,
    config: AstrumConfig | None = None,
    visualize: bool = False,
):
    if config is None:
        config = AstrumConfig(visualize=visualize)
    return await _HUB.get(namespace).run(target_tasks, config=config)
