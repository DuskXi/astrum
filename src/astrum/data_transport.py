from __future__ import annotations
import asyncio

import pydantic
import types
import gc
import typing
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Optional, Dict, Any, Union, Callable, Literal, Awaitable, Annotated, get_type_hints, get_origin, get_args
from typing import Annotated as Ref

from pydantic import BaseModel, Field, field_validator
from loguru import logger
import inspect
import ast
import textwrap
import re


from .models import TaskOrder

CommonDataModelType = type[dict] | type[list] | type[tuple] | type[set] | type[str] | type[int] | type[float] | type[bool] | type[bytes]
DataModelType = type[BaseModel] | CommonDataModelType | type
COMMON_DATA_MODEL_TYPES = (dict, list, tuple, set, str, int, float, bool, bytes)


@dataclass(frozen=True)
class From:
    task: Any
    locator: str | int | None = None


@dataclass(frozen=True)
class To:
    task: Any
    local: str | int | None = None
    target: str | int | None = None


F = From
T = To


@dataclass(frozen=True)
class _UnknownTaskReference:
    name: str

    def __getattr__(self, item: str) -> _UnknownTaskReference:
        return _UnknownTaskReference(f"{self.name}.{item}")


def normalize_data_model(data_model: Any, field_name: str = "allow_data_model") -> Optional[DataModelType]:
    if data_model is None:
        return None
    if data_model is Any or data_model is typing.Any:
        raise ValueError(f"{field_name} cannot be Any; use a concrete data model type")
    if get_origin(data_model) is Annotated:
        return normalize_data_model(get_args(data_model)[0], field_name)

    origin_model = get_origin(data_model)
    if origin_model is not None:
        data_model = origin_model

    if inspect.isclass(data_model):
        return data_model

    raise ValueError(f"{field_name} must be a class/type, got {data_model!r}")


class DTRela(BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
    key: Optional[str] = Field(default=None, description="Data key(data key and data index cannot empty at the same time)")
    index: Optional[int] = Field(default=None, description="Data Index(data key and data index cannot empty at the same time)")
    single_item: bool = Field(default=False, description="Task single item, if your function only accept one item or ont output item")
    related_task: str = Field(description="Related task id, if data key is not empty, related task id cannot be empty")
    from_function: Optional[Callable[..., Any] | Any] = Field(default=None, description="From function, if it is not None, related_task will be ignored when parent container is from_relation")

    # def __eq__(self, other: DTRela) -> bool:
    #     if isinstance(other, DTRela):
    #         return self.key == other.key and self.index == other.index and self.single_item == other.single_item and self.related_task == other.related_task
    #     else:
    #         return False


class DataRef(BaseModel):
    data: Optional[Any] = None


class DataItem(BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
    allow_data_model: Optional[DataModelType] = Field(default=None, description="Task data")
    data_ref: Optional[DataRef] = Field(default=None, description="Data reference")
    data_lock: asyncio.Lock = Field(default_factory=asyncio.Lock, description="Data lock for concurrent access")
    from_relation: Optional[DTRela] = Field(default=None, description="From data relation")
    to_relation: Optional[DTRela] = Field(default=None, description="To data relation")

    @field_validator("allow_data_model", mode="before")
    @classmethod
    def _normalize_allow_data_model(cls, value: Any) -> Optional[DataModelType]:
        return normalize_data_model(value)


class TaskData(BaseModel):
    task_id: str = Field(default_factory=str, description="Unique identifier for the task")
    input_data_item: list[DataItem] = Field(default_factory=list, description="Input data format for the task")
    output_data_item: list[DataItem] = Field(default_factory=list, description="Output data format for the task")
    from_tasks: Optional[list[str]] = Field(default_factory=list, description="From task id(from task and to task cannot empty at the same time)")
    to_tasks: Optional[list[str]] = Field(default_factory=list, description="To task id(from task and to task cannot empty at the same time)")


async def write_data(item: DataItem, result: Any) -> None:
    data_ref = item.data_ref
    lock = item.data_lock
    relation = item.from_relation
    if relation is not None:
        async with lock:
            if relation.single_item:
                data_ref.data = result
            elif relation.index is not None:
                if isinstance(result, list) or isinstance(result, tuple):
                    data_ref.data = result[relation.index]
                else:
                    raise ValueError(f"Expected a list or tuple result for indexed data relation, got {type(result).__name__}")
            elif relation.key:
                if isinstance(result, dict):
                    if relation.key in result:
                        data_ref.data = result[relation.key]
                    else:
                        raise KeyError(f"Key '{relation.key}' not found in result for keyed data relation")
                else:
                    raise ValueError(f"Expected a dict result for keyed data relation, got {type(result).__name__}")
    else:
        raise ValueError(f"Expected a dict result for keyed data relation, got {type(result).__name__}")


def autocast_data_transports_path(data_transports: list[TaskData], task_orders: list[TaskOrder]) -> list[TaskData]:
    # 构建task order的缓存
    task_order_map: dict[str, TaskOrder] = {}  # 任务依赖映射
    task_order_short_path: dict[str, list[str]] = {}  # 所有的任务路径
    task_queue = [x for x in task_orders]  # 队列
    while len(task_queue) > 0:
        task_order = task_queue.pop(0)
        task_order_map[task_order.task_name] = task_order
        for dependency in task_order.dependencies:
            if dependency.task_name not in task_order_map:
                task_queue.append(dependency)
            if dependency.task_name not in task_order_short_path:
                task_order_short_path[dependency.task_name] = []
            task_order_short_path[dependency.task_name].append(task_order.task_name)

    def auto_get(_from_id: str) -> list[str]:
        if _from_id in task_order_short_path:
            return task_order_short_path[_from_id]
        else:
            task_order_short_path[_from_id] = []
            return task_order_short_path[_from_id]

    # 双向路径表
    forward_data_map: dict[str, list[tuple[str, DataItem, TaskData, DTRela, Literal["from", "to"]]]] = {}
    backward_data_map: dict[str, list[tuple[str, DataItem, TaskData, DTRela, Literal["from", "to"]]]] = {}
    data_transports_map: dict[str, TaskData] = {x.task_id: x for x in data_transports if x.task_id is not None}
    # ref key 命名规则: f"{from_task}.{to_task}.{K:key/I:index/S:single(empty)}"
    autokey = lambda from_task, to_task, relation: (f"{from_task}.{to_task}." + ("S:" if relation is None else (f"K:{relation}" if isinstance(relation, str) else f"I:{relation}")))

    errors: list[str] = []
    for data_transport in data_transports:
        if data_transport.task_id and data_transport.task_id not in forward_data_map:
            forward_data_map[data_transport.task_id] = []
        if data_transport.task_id and data_transport.task_id not in backward_data_map:
            backward_data_map[data_transport.task_id] = []
        for item in data_transport.input_data_item:
            if item.from_relation is None and item.to_relation is None:
                errors.append(f"Data item in task {data_transport.task_id} has no relation definition, at least one of from_relation or to_relation must be defined.")
            if item.from_relation and not item.from_relation.single_item and item.from_relation.key is None and item.from_relation.index is None:
                errors.append(f"Data item in task {data_transport.task_id} has invalid from_relation definition, for non-single_item relation, either key or index must be defined.")
            if item.to_relation and not item.to_relation.single_item and item.to_relation.key is None and item.to_relation.index is None:
                errors.append(f"Data item in task {data_transport.task_id} has invalid to_relation definition, for non-single_item relation, either key or index must be defined.")

        for item in data_transport.output_data_item:
            if item.from_relation is None and item.to_relation is None:
                errors.append(f"Data item in task {data_transport.task_id} has no relation definition, at least one of from_relation or to_relation must be defined.")
            if item.from_relation and not item.from_relation.single_item and item.from_relation.key is None and item.from_relation.index is None:
                errors.append(f"Data item in task {data_transport.task_id} has invalid from_relation definition, for non-single_item relation, either key or index must be defined.")
            if item.to_relation and not item.to_relation.single_item and item.to_relation.key is None and item.to_relation.index is None:
                errors.append(f"Data item in task {data_transport.task_id} has invalid to_relation definition, for non-single_item relation, either key or index must be defined.")

    # 检查是否积攒了错误，并一次性抛出
    if errors:
        # 将列表中的错误信息用换行符连接起来，方便阅读
        error_msg = "Data transport validation failed with the following errors:\n- " + "\n- ".join(errors)
        raise ValueError(error_msg)
        # TODO: 将这里积攒起来一次性抛出，防止改完一个才发现另一个有问题
        # 已完成

    # task_orders 自动传播无related_task的情况
    # for data_transport in data_transports:
    #     if data_transport.task_id:
    #         for i, from_data in enumerate(data_transport.input_data_item):
    #             if from_data.from_relation and from_data.from_relation.related_task is None:
    #                 to_task: str = data_transport.task_id
    #                 # 在task order中寻找是否有映射, 在潜在的映射中寻找所属的data_transport中
    # TODO: 未来实现，支持在前后都设置了同名同类型的情况下，支持依赖taskorder的自动传播，当前还是强制related_task不得为空

    data_ref_map: dict[str, DataRef] = {}
    # 自动双向传播
    for data_transport in data_transports:
        if data_transport.task_id:
            for i, from_data in enumerate(data_transport.input_data_item):
                # from_data.data_ref = DataRef()
                if from_data.from_relation and from_data.from_relation.from_function is None:
                    backward_data_map[data_transport.task_id].append((from_data.from_relation.related_task, from_data, data_transport, from_data.from_relation, "from"))
                    forward_data_map[from_data.from_relation.related_task].append((data_transport.task_id, from_data, data_transport, from_data.from_relation, "from"))
            for i, to_data in enumerate(data_transport.output_data_item):
                # to_data.data_ref = DataRef()
                if to_data.to_relation:
                    forward_data_map[data_transport.task_id].append((to_data.to_relation.related_task, to_data, data_transport, to_data.to_relation, "to"))
                    forward_data_map[to_data.to_relation.related_task].append((data_transport.task_id, to_data, data_transport, to_data.to_relation, "to"))

    # TODO: from_relation 和 to_relation 也需要自动传播
    # 传播数据引用
    for from_id, forward_data in forward_data_map.items():
        for data in forward_data:
            to_id, data_item, task_data, relation, dir_type = data
            key = autokey(from_id, to_id, None if relation.single_item else (relation.key if relation.key is not None else relation.index))
            if key not in data_ref_map:
                data_ref_map[key] = DataRef()
            data_item.data_ref = data_ref_map[key]
            # TODO: 这里需要额外在forward_data_map记录到底是from relation还是 to relation 以推断这里需要找的是input还是output
            # 已完成传播，包括(input/output)_data_item级和(from/to)_relation级自动传播，等待审查
            if dir_type == "from":
                # 在前向映射中，记录的时候为from_relation，那么就要将数据映射到目标task的to_relation
                # 1. 找到目标对象
                target_task = data_transports_map[relation.related_task]
                has_item = False
                for d in target_task.output_data_item:
                    target_relation = d.to_relation
                    if (
                        target_relation
                        and target_relation.key == relation.key
                        and target_relation.index == relation.index
                        and target_relation.single_item == relation.single_item
                        and target_relation.related_task == to_id
                    ):
                        d.data_ref = data_ref_map[key]
                        has_item = True
                if not has_item:
                    target_task.output_data_item.append(
                        DataItem(
                            allow_data_model=data_item.allow_data_model,
                            data_ref=data_ref_map[key],
                            from_relation=DTRela(key=relation.key, index=relation.index, single_item=relation.single_item, related_task=from_id, from_function=relation.from_function),
                            to_relation=DTRela(key=relation.key, index=relation.index, single_item=relation.single_item, related_task=to_id),
                        )
                    )
                if target_task.to_tasks is None:
                    target_task.to_tasks = []
                if to_id not in target_task.to_tasks:
                    target_task.to_tasks.append(to_id)
            else:
                # 在前向映射中，记录的时候为to_relation，那么就要将数据映射到目标task的from_relation
                # 1. 找到目标对象
                target_task = data_transports_map[relation.related_task]
                has_item = False
                for d in target_task.input_data_item:
                    target_relation = d.from_relation
                    if (
                        target_relation
                        and target_relation.key == relation.key
                        and target_relation.index == relation.index
                        and target_relation.single_item == relation.single_item
                        and target_relation.related_task == from_id
                    ):
                        d.data_ref = data_ref_map[key]
                        has_item = True
                if not has_item:
                    target_task.input_data_item.append(
                        DataItem(
                            allow_data_model=data_item.allow_data_model,
                            data_ref=data_ref_map[key],
                            from_relation=DTRela(key=relation.key, index=relation.index, single_item=relation.single_item, related_task=from_id, from_function=relation.from_function),
                            to_relation=DTRela(key=relation.key, index=relation.index, single_item=relation.single_item, related_task=to_id),
                        )
                    )
                if target_task.from_tasks is None:
                    target_task.from_tasks = []
                if from_id not in target_task.from_tasks:
                    target_task.from_tasks.append(from_id)


def auto_generate_data_transports(task_orders: list[TaskOrder], task_func_map: dict[str, Callable | Awaitable]) -> list[TaskData]:
    """Generate TaskData from ``Annotated``/``Ref`` function annotations."""
    task_ids = _collect_task_ids(task_orders)
    task_id_set = set(task_ids)
    callable_task_lookup = _build_callable_task_lookup(task_func_map)
    data_transports = [TaskData(task_id=task_id) for task_id in task_ids]

    for data_transport in data_transports:
        task_id = data_transport.task_id
        if task_id not in task_func_map:
            continue

        func = unwrap_to_callable(task_func_map[task_id])
        type_hints = _get_type_hints_with_task_refs(func, task_func_map)

        for param_name, annotation in type_hints.items():
            if param_name == "return":
                continue

            parsed = _parse_ref_annotation(annotation, "input")
            if parsed is None:
                continue

            allow_data_model, relation = parsed
            source_task, source_locator = _parse_input_relation(task_id, param_name, relation, callable_task_lookup)
            _ensure_known_task(task_id, "input", source_task, task_id_set)

            data_transport.input_data_item.append(
                DataItem(
                    allow_data_model=allow_data_model,
                    from_relation=_build_relation(source_task, source_locator),
                    to_relation=DTRela(key=param_name, related_task=task_id),
                )
            )
            _append_unique(data_transport.from_tasks, source_task)

        return_annotation = type_hints.get("return")
        parsed_return = _parse_ref_annotation(return_annotation, "return")
        if parsed_return is None:
            continue

        allow_data_model, relation = parsed_return
        target_task, local_locator, target_locator = _parse_return_relation(task_id, relation, callable_task_lookup)
        _ensure_known_task(task_id, "return", target_task, task_id_set)

        data_transport.output_data_item.append(
            DataItem(
                allow_data_model=allow_data_model,
                from_relation=_build_relation(task_id, local_locator),
                to_relation=_build_relation(target_task, target_locator),
            )
        )
        _append_unique(data_transport.to_tasks, target_task)

    return data_transports


def _collect_task_ids(task_orders: list[TaskOrder]) -> list[str]:
    task_ids: list[str] = []
    task_queue = [x for x in task_orders]
    while task_queue:
        task_order = task_queue.pop(0)
        if task_order.task_name in task_ids:
            continue
        task_ids.append(task_order.task_name)
        task_queue.extend(task_order.dependencies)
    return task_ids


def _build_type_hint_localns(task_func_map: dict[str, Callable | Awaitable]) -> dict[str, Any]:
    localns: dict[str, Any] = {"Annotated": Annotated, "Ref": Ref, "From": From, "To": To, "F": F, "T": T}

    def ensure_namespace(parent: dict[str, Any] | SimpleNamespace, name: str) -> SimpleNamespace:
        if isinstance(parent, dict):
            existing = parent.get(name)
            if not isinstance(existing, SimpleNamespace):
                existing = SimpleNamespace()
                parent[name] = existing
            return existing

        existing = getattr(parent, name, None)
        if not isinstance(existing, SimpleNamespace):
            existing = SimpleNamespace()
            setattr(parent, name, existing)
        return existing

    for task_id, target in task_func_map.items():
        try:
            func = unwrap_to_callable(target)
        except Exception:
            func = target

        localns.setdefault(task_id, func)
        func_name = getattr(func, "__name__", None)
        if func_name:
            localns.setdefault(func_name, func)

        qualname = getattr(func, "__qualname__", None)
        if not qualname:
            continue

        raw_parts = qualname.split(".")
        paths = [[part for part in raw_parts if part != "<locals>"]]
        if "<locals>" in raw_parts:
            local_index = len(raw_parts) - 1 - raw_parts[::-1].index("<locals>")
            paths.append(raw_parts[local_index + 1 :])

        for parts in paths:
            if len(parts) < 2:
                continue
            parent: dict[str, Any] | SimpleNamespace = localns
            for part in parts[:-1]:
                parent = ensure_namespace(parent, part)
            setattr(parent, parts[-1], func)

    return localns


def _get_type_hints_with_task_refs(func: Callable, task_func_map: dict[str, Callable | Awaitable]) -> dict[str, Any]:
    localns = _build_type_hint_localns(task_func_map)
    while True:
        try:
            return get_type_hints(func, globalns=func.__globals__, localns=localns, include_extras=True)
        except TypeError:
            return get_type_hints(func, globalns=func.__globals__, localns=localns)
        except NameError as exc:
            match = re.search(r"name '([^']+)' is not defined", str(exc))
            if not match:
                raise
            missing_name = match.group(1)
            if missing_name in localns:
                raise
            localns[missing_name] = _UnknownTaskReference(missing_name)


def _parse_ref_annotation(annotation: Any, direction: Literal["input", "return"]) -> Optional[tuple[Any, tuple[Any, ...]]]:
    if annotation is None or get_origin(annotation) is not Annotated:
        return None

    args = get_args(annotation)
    if len(args) < 2:
        return None

    relation_metadata = [metadata for metadata in args[1:] if isinstance(metadata, (From, To, tuple))]
    if not relation_metadata:
        return None
    if len(relation_metadata) > 1:
        raise ValueError(f"Annotated relation metadata is ambiguous: {annotation!r}")

    metadata = relation_metadata[0]
    if isinstance(metadata, To):
        if direction == "input":
            raise ValueError("To(...) metadata is only valid on return annotations")
        return args[0], (metadata.task, metadata.local, metadata.target)
    if isinstance(metadata, From):
        if direction == "return":
            raise ValueError("From(...) metadata is only valid on parameter annotations")
        return args[0], (metadata.task, metadata.locator)

    return args[0], metadata


def _build_callable_task_lookup(task_func_map: dict[str, Callable | Awaitable]) -> dict[tuple[str, Any], str]:
    lookup: dict[tuple[str, Any], str] = {}

    def add_key(key: tuple[str, Any], task_id: str) -> None:
        existing = lookup.get(key)
        if existing is not None and existing != task_id:
            raise ValueError(f"Callable task reference is ambiguous: task ids '{existing}' and '{task_id}' refer to the same callable")
        lookup[key] = task_id

    for task_id, target in task_func_map.items():
        try:
            func = unwrap_to_callable(target)
        except Exception:
            func = target

        for candidate in _callable_lookup_candidates(func):
            add_key(candidate, task_id)

    return lookup


def _callable_lookup_candidates(target: Any) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = [("id", id(target))]

    func = getattr(target, "__func__", None)
    if func is not None:
        candidates.append(("id", id(func)))
        target = func

    module = getattr(target, "__module__", None)
    qualname = getattr(target, "__qualname__", None)
    if module is not None and qualname is not None:
        candidates.append(("qualname", (module, qualname)))

    return candidates


def _resolve_task_reference(owner_task_id: str, relation_kind: str, task_ref: Any, callable_task_lookup: dict[tuple[str, Any], str]) -> str:
    if isinstance(task_ref, str):
        if task_ref:
            return task_ref
        raise ValueError(f"Invalid {relation_kind} relation at task '{owner_task_id}': task id must be a non-empty string")

    matches = {callable_task_lookup[key] for key in _callable_lookup_candidates(task_ref) if key in callable_task_lookup}
    if len(matches) == 1:
        return next(iter(matches))
    if len(matches) > 1:
        raise ValueError(f"Callable task reference in {relation_kind} relation at task '{owner_task_id}' is ambiguous: {sorted(matches)}")

    name = task_ref.name if isinstance(task_ref, _UnknownTaskReference) else getattr(task_ref, "__qualname__", repr(task_ref))
    raise ValueError(f"Unknown callable task reference '{name}' in {relation_kind} relation at task '{owner_task_id}'")


def _parse_input_relation(task_id: str, param_name: str, relation: tuple[Any, ...], callable_task_lookup: dict[tuple[str, Any], str]) -> tuple[str, str | int | None]:
    if not 1 <= len(relation) <= 2:
        raise ValueError(f"Invalid input relation at task '{task_id}' parameter '{param_name}': expected (source_task_id, locator?)")

    source_task = _resolve_task_reference(task_id, f"input parameter '{param_name}'", relation[0], callable_task_lookup)
    locator = relation[1] if len(relation) == 2 else None
    _validate_locator(task_id, param_name, locator)
    return source_task, locator


def _parse_return_relation(task_id: str, relation: tuple[Any, ...], callable_task_lookup: dict[tuple[str, Any], str]) -> tuple[str, str | int | None, str | int | None]:
    if not 1 <= len(relation) <= 3:
        raise ValueError(f"Invalid return relation at task '{task_id}': expected (target_task_id, local_locator?, target_locator?)")

    target_task = _resolve_task_reference(task_id, "return", relation[0], callable_task_lookup)
    local_locator = relation[1] if len(relation) >= 2 else None
    target_locator = relation[2] if len(relation) == 3 else None
    _validate_locator(task_id, "return local", local_locator)
    _validate_locator(task_id, "return target", target_locator)
    return target_task, local_locator, target_locator


def _validate_locator(task_id: str, field_name: str, locator: Any) -> None:
    if locator is None or isinstance(locator, (str, int)):
        return
    raise ValueError(f"Invalid relation locator at task '{task_id}' field '{field_name}': expected str, int, or None")


def _build_relation(related_task: str, locator: str | int | None) -> DTRela:
    if locator is None:
        return DTRela(single_item=True, related_task=related_task)
    if isinstance(locator, int):
        return DTRela(index=locator, related_task=related_task)
    return DTRela(key=locator, related_task=related_task)


def _ensure_known_task(task_id: str, direction: str, related_task: str, task_ids: set[str]) -> None:
    if related_task not in task_ids:
        raise ValueError(f"Unknown task id '{related_task}' referenced by {direction} relation at task '{task_id}'")


def _append_unique(items: Optional[list[str]], value: str) -> None:
    if items is not None and value not in items:
        items.append(value)


def unwrap_to_callable(target: Callable | Awaitable) -> Callable:
    """
    将 Callable 或 Awaitable (协程对象) 统一转为原始的 Callable 函数对象。
    """
    # 1. 如果传入的已经是 Callable (例如直接传入了 func)
    if callable(target):
        return target

    # 2. 如果传入的是执行后的协程对象 (例如传入了 func())
    if inspect.iscoroutine(target):
        # 提取协程底层的代码对象 (CodeType)
        code_obj = getattr(target, "cr_code", None)
        if not code_obj:
            raise ValueError("无法获取该协程的代码对象")

        # 核心黑魔法：通过垃圾回收器反向查找引用了该代码对象的函数
        for referrer in gc.get_referrers(code_obj):
            # 确认引用者是一个函数，并且它的 __code__ 正是我们提取出的代码对象
            if isinstance(referrer, types.FunctionType) and referrer.__code__ is code_obj:
                return referrer

        raise RuntimeError("无法从协程对象中找回原始函数，它可能未被定义为常规函数或已被销毁。")

    raise TypeError(f"不支持的类型: {type(target)}，需要 Callable 或 Awaitable")


def final_check(
    data_transports: list[TaskData],
    task_orders: list[TaskOrder],
    task_func_map: dict[str, Callable | Awaitable],
    *,
    skip_type_check: bool = False,
    infer_via_ast: bool = True,
    strict_topology: bool = False,
) -> list[str]:
    # 构建task order的缓存
    task_order_map: dict[str, TaskOrder] = {}  # 任务依赖映射
    task_queue = [x for x in task_orders]  # 队列
    while len(task_queue) > 0:
        task_order = task_queue.pop(0)
        task_order_map[task_order.task_name] = task_order
        for dependency in task_order.dependencies:
            if dependency.task_name not in task_order_map:
                task_queue.append(dependency)

    errors: list[str] = []
    jump_map: dict[str, dict[str, list[str]]] = {}

    # 提前构建 Task ID 到 TaskData 的映射字典，用于后续校验引用的合法性
    task_dict: dict[str, TaskData] = {dt.task_id: dt for dt in data_transports if dt.task_id}

    for data_transport in data_transports:
        # 校验模式冲突，index/key/single模式只能有一个
        for i, input_data_item in enumerate(data_transport.input_data_item):
            err = False
            # 校验空关系
            if input_data_item.from_relation is None:
                errors.append(f"From Relation Loss at {data_transport.task_id}.input[{i}]")
                err = True  # 防止后续获取 related_task 时抛出 NoneType 错误
            if input_data_item.to_relation is None:
                errors.append(f"To Relation Loss at {data_transport.task_id}.input[{i}]")
                err = True  # 防止后续抛出 NoneType 错误
            if err:
                continue

            related_task = input_data_item.from_relation.related_task
            if input_data_item.from_relation.from_function is not None:
                # 校验来源函数是否匹配类型定义
                if not skip_type_check:
                    try:
                        check_from_function_type(input_data_item.from_relation.from_function, input_data_item.allow_data_model, data_transport.task_id, infer_via_ast=infer_via_ast)
                    except Exception as e:
                        errors.append(f"Relation Model mismatch at {data_transport.task_id}.input[{i}]: {e}")
                continue

            # 校验来源任务函数是否匹配类型定义
            if not skip_type_check and input_data_item.from_relation.single_item:
                try:
                    check_from_function_type(unwrap_to_callable(task_func_map[related_task]), input_data_item.allow_data_model, related_task, infer_via_ast=infer_via_ast)
                except Exception as e:
                    errors.append(f"Relation Model mismatch at {data_transport.task_id}.input[{i}]: {e}")
            # 写入关系表
            if related_task not in jump_map:
                jump_map[related_task] = {"from": [], "to": []}
            # 这个意思是，当前的这个task下属的输入数据中，我们有from和to，to肯定就指的是本体，而from就是数据来源，我们需要确保这个from的task的对象，拥有到这个task的映射地址，也就是to属性
            jump_map[related_task]["to"].append(data_transport.task_id)

            if data_transport.task_id not in jump_map:
                jump_map[data_transport.task_id] = {"from": [], "to": []}
            # 这里就是写入固有的本体的from属性，以供后续核验使用
            jump_map[data_transport.task_id]["from"].append(related_task)

        for i, output_data_item in enumerate(data_transport.output_data_item):
            err = False
            if output_data_item.from_relation is None:
                errors.append(f"From Relation Loss at {data_transport.task_id}.output[{i}]")
                err = True
            if output_data_item.to_relation is None:
                errors.append(f"To Relation Loss at {data_transport.task_id}.output[{i}]")
                err = True
            if err:
                continue

            related_task = output_data_item.to_relation.related_task
            # 校验来源任务本身函数是否匹配类型定义
            if not skip_type_check and output_data_item.from_relation.single_item:
                try:
                    check_from_function_type(unwrap_to_callable(task_func_map[data_transport.task_id]), output_data_item.allow_data_model, data_transport.task_id, infer_via_ast=infer_via_ast)
                except Exception as e:
                    errors.append(f"Relation Model mismatch at {data_transport.task_id}.output[{i}]: {e}")
            # 写入关系表
            if related_task not in jump_map:
                jump_map[related_task] = {"from": [], "to": []}
            # 这里是输出数据的去向，去向的task应该在jump map中有一个from属性指向当前task
            jump_map[related_task]["from"].append(data_transport.task_id)

            if data_transport.task_id not in jump_map:
                jump_map[data_transport.task_id] = {"from": [], "to": []}
            # 这里就是写入固有的本体的to属性，以供后续核验使用
            jump_map[data_transport.task_id]["to"].append(related_task)

    # 全部写入完成，校验jump情况
    for task_id, relation in jump_map.items():
        # 1. 校验推导出的相关节点是否存在于当前定义的任务列表中
        if task_id not in task_dict:
            errors.append(f"Unknown Task ID '{task_id}' referenced in data relations")
            continue

        current_task = task_dict[task_id]

        # 获取当前 Task 显式声明的来源和去向（兼容 None 和 Pydantic 默认 factory 产生的异常值）
        declared_from_tasks = set(current_task.from_tasks or [])
        declared_to_tasks = set(current_task.to_tasks or [])

        # 获取根据 DataItem 数据流向推导出的来源和去向（使用集合进行去重）
        inferred_from_tasks = set(relation["from"])
        inferred_to_tasks = set(relation["to"])

        # 2. 校验数据推导的来源是否在声明的 from_tasks 列表中
        for inferred_from in inferred_from_tasks:
            if inferred_from not in declared_from_tasks:
                msg = f"Topology mismatch: Task '{task_id}' receives data from '{inferred_from}', but it is missing in from_tasks"
                if strict_topology:
                    errors.append(msg)
                else:
                    logger.debug(msg)

        # 3. 校验数据推导的去向是否在声明的 to_tasks 列表中
        for inferred_to in inferred_to_tasks:
            if inferred_to not in declared_to_tasks:
                msg = f"Topology mismatch: Task '{task_id}' sends data to '{inferred_to}', but it is missing in to_tasks"
                if strict_topology:
                    errors.append(msg)
                else:
                    logger.debug(msg)

    return errors


def check_from_function_type(from_function: Any, allow_data_model: Optional[DataModelType], task_id: Optional[str], infer_via_ast: bool = True) -> None:
    """检查 from_function 或其默认值的类型是否符合 allow_data_model 定义"""
    if from_function is None:
        return
    allow_data_model = normalize_data_model(allow_data_model)

    if isinstance(from_function, Callable):
        is_aligned = check_return_type_alignment(from_function=from_function, allow_data_model=allow_data_model, infer_via_ast=infer_via_ast)
        if not is_aligned:
            model_name = getattr(allow_data_model, "__name__", str(allow_data_model))
            func_name = getattr(from_function, "__name__", str(from_function))
            raise ValueError(f"Type error for task '{task_id}' input data item: from_function '{func_name}' return type does not match expected {model_name}")
    else:
        if allow_data_model is None:
            return

        if inspect.isclass(allow_data_model) and issubclass(allow_data_model, BaseModel):
            try:
                allow_data_model.model_validate(from_function)
            except pydantic.ValidationError as e:
                raise ValueError(f"Validation error for task '{task_id}' input data item with from_function default value: {e}")
        else:
            if not isinstance(from_function, allow_data_model):
                model_name = getattr(allow_data_model, "__name__", str(allow_data_model))
                raise ValueError(f"Type error for task '{task_id}' input data item with from_function default value: expected {model_name}, got {type(from_function).__name__}")


def resolve_task_data(task_orders: list[TaskOrder], data_transports: list[TaskData], allow_no_dir_definition: bool = False, infer_via_ast: bool = False, silence_warnings: bool = False):
    """解析数据流并自动补依赖，同时校验引用关系与默认数据/自定义数据源的类型匹配。

    注意：本函数会就地变异传入的对象——

    - ``task_orders``：当 ``DataItem.from_relation`` / ``to_relation`` 引用了未声明
      的依赖关系时，会自动把对应的 ``TaskOrder`` 追加到目标任务的 ``dependencies``。
    - ``data_transports``：会把推断出的来源/去向 task_id 自动写入对应 ``TaskData``
      的 ``from_tasks`` / ``to_tasks``。

    应在 DAG 进入调度器之前调用一次，并据此结果决定是否阻断构建（致命逻辑错误
    会抛 ``ValueError``）。
    """
    task_order_map: dict[str, TaskOrder] = {}  # 任务依赖映射
    task_order_short_path: dict[str, list[str]] = {}  # 所有的任务路径
    task_queue = [x for x in task_orders]  # 队列
    while len(task_queue) > 0:
        task_order = task_queue.pop(0)
        task_order_map[task_order.task_name] = task_order
        for dependency in task_order.dependencies:
            if dependency.task_name not in task_order_map:
                task_queue.append(dependency)
            if dependency.task_name not in task_order_short_path:
                task_order_short_path[dependency.task_name] = []
            task_order_short_path[dependency.task_name].append(task_order.task_name)

    def auto_get(_from_id: str) -> list[str]:
        if _from_id in task_order_short_path:
            return task_order_short_path[_from_id]
        else:
            task_order_short_path[_from_id] = []
            return task_order_short_path[_from_id]

    no_source_definition: int = 0
    no_target_definition: int = 0

    for data_transport in data_transports:
        if not data_transport.from_tasks:
            data_transport.from_tasks = []
        if not data_transport.to_tasks:
            data_transport.to_tasks = []
        # 检查数据方向定义
        for i, item in enumerate(data_transport.input_data_item):  # 检查输入方向数据定义
            if item.from_relation:
                # 检查来源定义是否被声明了依赖
                relation = item.from_relation
                from_id = relation.related_task
                if from_id:
                    if data_transport.task_id and relation.from_function is None and data_transport.task_id not in auto_get(from_id):  # 代表没有声明依赖, 或者声明了数据注入函数
                        if not silence_warnings:
                            logger.debug(f"Warning with undeclared relation at: Task[{data_transport.task_id}] | Input Data[{i}] | From Task[{from_id}], has been automatically declared")
                        task_order_map[data_transport.task_id].dependencies.append(task_order_map[from_id])  # 在当前的task的依赖关系中加入对该未声明依赖的依赖
                        auto_get(from_id).append(data_transport.task_id)  # 在该未声明依赖的短路径中加入当前task
                    if from_id not in data_transport.from_tasks and relation.from_function is None:
                        data_transport.from_tasks.append(from_id)  # 在当前task的数据定义中加入来源task的声明
                    if relation.from_function is not None:  # 检查可执行对象的结果是否符合allow_data_model定义
                        check_from_function_type(from_function=relation.from_function, allow_data_model=item.allow_data_model, task_id=data_transport.task_id, infer_via_ast=infer_via_ast)
                else:
                    pass
            else:
                if not silence_warnings:
                    logger.warning(f"Warning with empty relation at: Task[{data_transport.task_id}] has no source definition, it will break the full progress!")
                no_source_definition += 1

            if item.to_relation:
                relation = item.to_relation  # 检查去往方向定义是否错误的被定义到了非本任务，因为这个是输入方向的数据定义，去向必须是本体
                to_id = relation.related_task
                if to_id != data_transport.task_id:  # 代表去向定义错误
                    if not silence_warnings:
                        logger.debug(f"Warning with wrong relation at: Task[{data_transport.task_id}] | Input Data[{i}] | To Task[{to_id}] is not the same as current task, system has been ignored")

        for i, item in enumerate(data_transport.output_data_item):  # 检查输出方向数据定义
            if item.from_relation:
                relation = item.from_relation  # 检查来源定义是否错误的被定义到了非本任务，因为这个是输出方向的数据定义，来源必须是本体
                from_id = relation.related_task
                if from_id != data_transport.task_id:  # 代表来源定义错误
                    if not silence_warnings:
                        logger.debug(
                            f"Warning with wrong relation at: Task[{data_transport.task_id}] | Output Data[{i}] | From Task[{from_id}] is not the same as current task, system has been ignored"
                        )

            if item.to_relation:
                relation = item.to_relation  # 检查去往方向定义是否被声明了依赖
                to_id = relation.related_task
                if to_id:
                    if data_transport.task_id and to_id not in auto_get(data_transport.task_id):  # 代表没有声明依赖
                        if not silence_warnings:
                            logger.debug(f"Warning with undeclared relation at: Task[{data_transport.task_id}] | Output Data[{i}] | To Task[{to_id}], has been automatically declared")
                        task_order_map[to_id].dependencies.append(task_order_map[data_transport.task_id])  # 在去向task的依赖关系中加入对当前task的依赖
                        auto_get(data_transport.task_id).append(to_id)  # 在当前task的短路径中加入去向task
                    if to_id not in data_transport.to_tasks:
                        data_transport.to_tasks.append(to_id)  # 在当前task的数据定义中加入去向task的声明
                    # to relation是用于本任务出口的类型定义情况的，不存在出口设置固定数字或者自定义数据提取方式的
                else:
                    pass
            else:
                if not silence_warnings:
                    logger.warning(f"Warning with empty relation at: Task[{data_transport.task_id}] has no target definition, it will break the full progress!")
                no_target_definition += 1

    if not silence_warnings:
        if no_source_definition > 0:
            logger.warning(f"Total {no_source_definition} data items have no source definition, which may break the full progress!")
        if no_target_definition > 0:
            logger.warning(f"Total {no_target_definition} data items have no target definition, which may break the full progress!")
    if not allow_no_dir_definition:
        logger.error(f"Current system does not allow data items with no source or target definition, please check the warnings above and fix the issues!")
        raise ValueError(f"Data items with no source or target definition are not allowed when allow_no_dir_definition is set to False.")
    elif not silence_warnings:
        logger.warning(f"Current system allows data items with no source or target definition, but it may break the full progress, please check the warnings above and fix the issues if possible!")


def _get_return_type_from_ast(func: Callable) -> Optional[Any]:
    """通过 AST 深度静态分析推断返回类型，支持局部变量标注"""
    try:
        source = inspect.getsource(func)
        source = textwrap.dedent(source)
        tree = ast.parse(source)

        # 提取函数定义节点 (可能是 AsyncFunctionDef 或 FunctionDef)
        func_def = next(node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Expr)))

        # 1. 如果是 Lambda
        if isinstance(func_def, ast.Expr) and isinstance(func_def.value, ast.Lambda):
            return _guess_type_from_node(func_def.value.body, func)

        # 2. 搜索 Return 语句
        target_var_name = None
        for node in ast.walk(func_def):
            if isinstance(node, ast.Return) and isinstance(node.value, ast.Name):
                target_var_name = node.value.id
                break
            elif isinstance(node, ast.Return):
                # 如果直接返回字面量，如 return {"a": 1}
                return _guess_type_from_node(node.value, func)

        # 3. 如果返回的是变量名，在函数体内追溯变量传递和类型标注
        if target_var_name:
            var_types = {}
            var_aliases = {}
            for node in ast.walk(func_def):
                if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                    var_types[node.target.id] = _resolve_annotation_node(node.annotation, func)
                elif isinstance(node, ast.Assign):
                    for t in node.targets:
                        if isinstance(t, ast.Name):
                            if isinstance(node.value, ast.Name):
                                var_aliases[t.id] = node.value.id
                            else:
                                var_types[t.id] = _guess_type_from_node(node.value, func)

            curr = target_var_name
            seen = set()
            while curr and curr not in seen:
                seen.add(curr)
                if curr in var_types and var_types[curr] is not None:
                    return var_types[curr]
                curr = var_aliases.get(curr)

        return None
    except Exception:
        # AST 解析任何环节出错，均返回 None 触发回退机制
        return None


def _resolve_annotation_node(node: ast.AST, func: Callable) -> Any:
    """将 AST 的标注节点解析为实际的 Python 类型对象"""
    # 处理简单类型如 int, str, MyModel
    if isinstance(node, ast.Name):
        name = node.id
        if name in func.__globals__:
            return func.__globals__[name]
        import builtins

        if hasattr(builtins, name):
            return getattr(builtins, name)
        return name
    # 处理嵌套类型如 list[int] (Subscript)
    elif isinstance(node, ast.Subscript):
        return _resolve_annotation_node(node.value, func)
    return None


def _guess_type_from_node(node: ast.AST, func: Callable) -> Any:
    """根据字面量猜测类型"""
    if isinstance(node, ast.Dict):
        return dict
    if isinstance(node, ast.List):
        return list
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        return func.__globals__.get(node.func.id)
    return None


def check_return_type_alignment(from_function: Optional[Callable[..., Any]], allow_data_model: Optional[DataModelType], infer_via_ast: bool = True) -> bool:
    if from_function is None or allow_data_model is None:
        return True
    allow_data_model = normalize_data_model(allow_data_model)

    # --- 步骤 1: 运行时注解获取 ---
    try:
        type_hints = get_type_hints(from_function)
        return_type = type_hints.get("return", inspect._empty)
    except Exception:
        return_type = inspect._empty

    # --- 步骤 2: AST 推断 (如果开启且注解缺失) ---
    if (return_type is inspect._empty or return_type is Any or return_type is typing.Any) and infer_via_ast:
        ast_type = _get_return_type_from_ast(from_function)
        if ast_type:
            return_type = ast_type

    # --- 步骤 3: 容错与 Any 处理 ---
    # 如果此时还是无法确定类型（inspect._empty），则视为 Any 并给警告
    if return_type is inspect._empty or return_type is Any or return_type is typing.Any:
        logger.warning(f"Could not reliably determine return type for '{from_function.__name__}'. " f"Falling back to 'Any' validation (Pass with warning).", UserWarning)
        return True

    if get_origin(return_type) is Annotated:
        return_type = get_args(return_type)[0]
    # --- 步骤 4: 类型匹配逻辑 ---
    # 处理异步
    awaitable_origin = get_origin(return_type)
    if awaitable_origin is not None and getattr(awaitable_origin, "__name__", "") == "Awaitable" and hasattr(return_type, "__args__"):
        # 简单解包 Awaitable
        if hasattr(return_type, "__args__"):
            return_type = return_type.__args__[-1]

    # 获取原始类型 (处理 list[int] -> list)
    if return_type is Any or return_type is typing.Any:
        return True
    origin_return = get_origin(return_type) or return_type

    # 1. 匹配 BaseModel
    if inspect.isclass(allow_data_model) and issubclass(allow_data_model, BaseModel):
        if inspect.isclass(origin_return) and issubclass(origin_return, allow_data_model):
            return True

    # 2. 匹配容器类型
    if allow_data_model in COMMON_DATA_MODEL_TYPES:
        if origin_return is allow_data_model:
            return True

    if inspect.isclass(allow_data_model) and inspect.isclass(origin_return):
        return issubclass(origin_return, allow_data_model)

    return False


def visualize_data_transport(task_transports: list[TaskData], task_orders: list[TaskOrder]) -> None:
    try:
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel
        from rich.tree import Tree
        from rich.text import Text
    except ImportError:
        logger.warning("Rich is not installed, skipping visualization.")
        return

    console = Console()
    transport_map: dict[str, TaskData] = {t.task_id: t for t in task_transports if t.task_id}

    # ── 构建邻接表：parent -> [children]（正向依赖展开） ──
    # 即：如果 child 依赖 parent，那么 parent 的 children 列表里有 child
    children_map: dict[str, list[str]] = {}
    parents_map: dict[str, list[str]] = {}
    all_task_names: list[str] = []
    for order in task_orders:
        name = order.task_name
        all_task_names.append(name)
        parents_map.setdefault(name, [])
        children_map.setdefault(name, [])
        for dep in order.dependencies:
            children_map.setdefault(dep.task_name, [])
            if name not in children_map[dep.task_name]:
                children_map[dep.task_name].append(name)
            if dep.task_name not in parents_map[name]:
                parents_map[name].append(dep.task_name)

    # 找到根节点（无前置依赖）
    roots = [name for name in all_task_names if not parents_map.get(name)]

    # ── 辅助：为单个任务节点生成数据流描述文本 ──
    def _format_data_flow(task_id: str) -> list[str]:
        transport = transport_map.get(task_id)
        if not transport:
            return []
        lines: list[str] = []
        # 输入
        if transport.input_data_item:
            for item in transport.input_data_item:
                rel = item.from_relation
                if not rel:
                    continue
                model_name = getattr(item.allow_data_model, "__name__", "Any") if item.allow_data_model else "Any"
                key_str = f"'{rel.key}'" if rel.key is not None else (f"[{rel.index}]" if rel.index is not None else "•")
                if rel.from_function:
                    func_name = getattr(rel.from_function, "__name__", "λ")
                    lines.append(f"[dim]⬅ {key_str} ← ƒ({func_name}) : {model_name}[/dim]")
                else:
                    lines.append(f"[dim]⬅ {key_str} ← {rel.related_task} : {model_name}[/dim]")
        # 输出
        if transport.output_data_item:
            for item in transport.output_data_item:
                rel = item.to_relation
                if not rel:
                    continue
                model_name = getattr(item.allow_data_model, "__name__", "Any") if item.allow_data_model else "Any"
                key_str = f"'{rel.key}'" if rel.key is not None else (f"[{rel.index}]" if rel.index is not None else "•")
                lines.append(f"[dim]➡ {key_str} → {rel.related_task} : {model_name}[/dim]")
        return lines

    # ── 递归构建 DAG 树（处理多引用节点） ──
    expanded: set[str] = set()

    def _build_dag_node(task_id: str, parent_tree: Tree) -> None:
        is_first = task_id not in expanded
        expanded.add(task_id)

        # 节点标签
        fan_in = len(parents_map.get(task_id, []))
        fan_out = len(children_map.get(task_id, []))
        badge = ""
        if fan_in > 1:
            badge += f" [yellow]⇠×{fan_in}[/yellow]"
        if fan_out > 1:
            badge += f" [cyan]⇢×{fan_out}[/cyan]"

        if is_first:
            node = parent_tree.add(f"[bold green]{task_id}[/bold green]{badge}")
            # 打印数据流注释
            for line in _format_data_flow(task_id):
                node.add(line)
            # 递归展开子节点
            for child in children_map.get(task_id, []):
                _build_dag_node(child, node)
        else:
            # 已展开过的节点，用回引标记避免重复递归
            parent_tree.add(f"[dim italic]↩ {task_id}[/dim italic] [dim](已展开)[/dim]{badge}")

    # ── 构建主树 ──
    main_tree = Tree("[bold blue]📋 Astrum DAG 依赖树[/bold blue]")

    if not roots:
        main_tree.add("[red]⚠ 未发现根节点（可能存在循环依赖）[/red]")
    else:
        for root in roots:
            _build_dag_node(root, main_tree)

    # ── 构建 Data Transport 明细表 ──
    table = Table(
        title="[bold magenta]📊 Data Transport Matrix[/bold magenta]",
        show_header=True,
        header_style="bold yellow",
        border_style="bright_black",
        pad_edge=True,
        expand=False,
    )
    table.add_column("Task", style="bold cyan", no_wrap=True)
    table.add_column("Input ⬅", style="green")
    table.add_column("Output ➡", style="blue")

    for task_id in all_task_names:
        transport = transport_map.get(task_id)

        # inputs
        input_parts: list[str] = []
        if transport and transport.input_data_item:
            for idx, item in enumerate(transport.input_data_item):
                rel = item.from_relation
                if not rel:
                    continue
                model = getattr(item.allow_data_model, "__name__", "Any") if item.allow_data_model else "Any"
                key_str = f"'{rel.key}'" if rel.key is not None else (f"[{rel.index}]" if rel.index is not None else "•")
                if rel.from_function:
                    func_name = getattr(rel.from_function, "__name__", "λ")
                    input_parts.append(f"{idx}. ƒ({func_name}).{key_str} → [yellow]{model}[/yellow]")
                else:
                    input_parts.append(f"{idx}. {rel.related_task}.{key_str} → [yellow]{model}[/yellow]")
        input_str = "\n".join(input_parts) if input_parts else "[dim]—[/dim]"

        # outputs
        output_parts: list[str] = []
        if transport and transport.output_data_item:
            for idx, item in enumerate(transport.output_data_item):
                rel = item.to_relation
                if not rel:
                    continue
                model = getattr(item.allow_data_model, "__name__", "Any") if item.allow_data_model else "Any"
                key_str = f"'{rel.key}'" if rel.key is not None else (f"[{rel.index}]" if rel.index is not None else "•")
                output_parts.append(f"{idx}. {key_str} → {rel.related_task} [yellow]{model}[/yellow]")
        output_str = "\n".join(output_parts) if output_parts else "[dim]—[/dim]"

        table.add_row(task_id, input_str, output_str)

    # ── 统计摘要 ──
    total_tasks = len(all_task_names)
    total_edges = sum(len(v) for v in children_map.values())
    total_data_items = sum(len(t.input_data_item) + len(t.output_data_item) for t in task_transports)
    func_injections = sum(1 for t in task_transports for item in t.input_data_item if item.from_relation and item.from_relation.from_function)

    summary_tree = Tree("[bold blue]📈 统计摘要[/bold blue]")
    summary_tree.add(f"任务总数: [bold]{total_tasks}[/bold]")
    summary_tree.add(f"依赖边数: [bold]{total_edges}[/bold]")
    summary_tree.add(f"数据传输项: [bold]{total_data_items}[/bold]")
    if func_injections:
        summary_tree.add(f"外部函数注入: [bold yellow]{func_injections}[/bold yellow]")
    multi_ref = [name for name, parents in parents_map.items() if len(parents) > 1]
    if multi_ref:
        summary_tree.add(f"多源汇聚节点: [bold cyan]{', '.join(multi_ref)}[/bold cyan]")
    fan_out_nodes = [name for name, kids in children_map.items() if len(kids) > 1]
    if fan_out_nodes:
        summary_tree.add(f"多路扇出节点: [bold cyan]{', '.join(fan_out_nodes)}[/bold cyan]")

    # ── 最终输出 ──
    console.print()
    console.print(Panel(main_tree, title="[bold]Astrum Execution Plan[/bold]", border_style="cyan", padding=(1, 2)))
    console.print()
    console.print(table)
    console.print()
    console.print(Panel(summary_tree, border_style="bright_black", padding=(0, 2)))
