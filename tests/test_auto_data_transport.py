from __future__ import annotations

from typing import Annotated, Any

import pytest
import pydantic

from astrum.config import AstrumConfig
from astrum.data_transport import DataItem, F, From, Ref, T, To, auto_generate_data_transports
from astrum.decorators import SchedulerRegistry
from astrum.models import TaskOrder


class Payload:
    pass


def _orders(*task_ids: str) -> list[TaskOrder]:
    return [TaskOrder(task_name=task_id) for task_id in task_ids]


def test_auto_generate_ref_input_key() -> None:
    def source() -> dict:
        return {"account": Payload()}

    def target(account: Ref[Payload, ("source", "account")]) -> None:
        return None

    transports = auto_generate_data_transports(_orders("source", "target"), {"source": source, "target": target})
    target_data = next(item for item in transports if item.task_id == "target")

    assert target_data.from_tasks == ["source"]
    assert len(target_data.input_data_item) == 1
    data_item = target_data.input_data_item[0]
    assert data_item.allow_data_model is Payload
    assert data_item.from_relation.related_task == "source"
    assert data_item.from_relation.key == "account"
    assert data_item.to_relation.related_task == "target"
    assert data_item.to_relation.key == "account"


def test_auto_generate_from_input_key() -> None:
    def source() -> dict:
        return {"beans_ready": True}

    def target(beans: Annotated[bool, From("source", "beans_ready")]) -> None:
        return None

    transports = auto_generate_data_transports(_orders("source", "target"), {"source": source, "target": target})
    target_data = next(item for item in transports if item.task_id == "target")
    data_item = target_data.input_data_item[0]

    assert data_item.allow_data_model is bool
    assert data_item.from_relation.related_task == "source"
    assert data_item.from_relation.key == "beans_ready"
    assert data_item.to_relation.key == "beans"


def test_auto_generate_from_input_callable_reference() -> None:
    def source() -> dict:
        return {"beans_ready": True}

    def target(beans: Annotated[bool, F(source, "beans_ready")]) -> None:
        return None

    transports = auto_generate_data_transports(_orders("source", "target"), {"source": source, "target": target})
    target_data = next(item for item in transports if item.task_id == "target")

    assert target_data.input_data_item[0].from_relation.related_task == "source"


def test_auto_generate_to_return_callable_reference() -> None:
    def target() -> None:
        return None

    def source() -> Annotated[dict, T(target, "local", "remote")]:
        return {"local": True}

    transports = auto_generate_data_transports(_orders("source", "target"), {"source": source, "target": target})
    source_data = next(item for item in transports if item.task_id == "source")
    data_item = source_data.output_data_item[0]

    assert data_item.to_relation.related_task == "target"
    assert data_item.from_relation.key == "local"
    assert data_item.to_relation.key == "remote"


def test_auto_generate_legacy_tuple_callable_reference() -> None:
    def source() -> dict:
        return {"value": 1}

    def target(value: Ref[int, (source, "value")]) -> None:
        return None

    transports = auto_generate_data_transports(_orders("source", "target"), {"source": source, "target": target})
    target_data = next(item for item in transports if item.task_id == "target")

    assert target_data.input_data_item[0].from_relation.related_task == "source"


def test_auto_generate_class_scoped_callable_references() -> None:
    class Flow:
        def load(self) -> dict:
            return {"value": 1}

        @staticmethod
        def load_static() -> dict:
            return {"value": 2}

        @classmethod
        def load_class(cls) -> dict:
            return {"value": 3}

    def use_method(value: Annotated[int, F(Flow.load, "value")]) -> None:
        return None

    def use_static(value: Annotated[int, F(Flow.load_static, "value")]) -> None:
        return None

    def use_class(value: Annotated[int, F(Flow.load_class, "value")]) -> None:
        return None

    transports = auto_generate_data_transports(
        _orders("load", "load_static", "load_class", "use_method", "use_static", "use_class"),
        {
            "load": Flow.load,
            "load_static": Flow.load_static,
            "load_class": Flow.load_class,
            "use_method": use_method,
            "use_static": use_static,
            "use_class": use_class,
        },
    )

    for task_id, source_id in [("use_method", "load"), ("use_static", "load_static"), ("use_class", "load_class")]:
        task_data = next(item for item in transports if item.task_id == task_id)
        assert task_data.input_data_item[0].from_relation.related_task == source_id


def test_auto_generate_short_from_input_index_and_whole_item() -> None:
    def source() -> tuple[str, Payload]:
        return "ready", Payload()

    def target(status: Annotated[str, F("source", 0)], payload: Annotated[Payload, F("source")]) -> None:
        return None

    transports = auto_generate_data_transports(_orders("source", "target"), {"source": source, "target": target})
    target_data = next(item for item in transports if item.task_id == "target")

    status, payload = target_data.input_data_item
    assert status.from_relation.index == 0
    assert payload.from_relation.single_item is True


def test_auto_generate_ref_input_index_and_whole_item() -> None:
    def source() -> tuple[int, str]:
        return 1, "ready"

    def target(first: Ref[int, ("source", 0)], whole: Ref[tuple, ("source",)]) -> None:
        return None

    transports = auto_generate_data_transports(_orders("source", "target"), {"source": source, "target": target})
    target_data = next(item for item in transports if item.task_id == "target")

    first, whole = target_data.input_data_item
    assert first.from_relation.index == 0
    assert first.from_relation.single_item is False
    assert whole.from_relation.key is None
    assert whole.from_relation.index is None
    assert whole.from_relation.single_item is True


def test_auto_generate_normalizes_builtin_generic_models() -> None:
    def source() -> dict:
        return {"items": [1, 2], "title": "ready"}

    def target(items: Ref[list[int], ("source", "items")], title: Ref[str, ("source", "title")]) -> None:
        return None

    transports = auto_generate_data_transports(_orders("source", "target"), {"source": source, "target": target})
    target_data = next(item for item in transports if item.task_id == "target")

    assert target_data.input_data_item[0].allow_data_model is list
    assert target_data.input_data_item[1].allow_data_model is str


def test_data_item_rejects_any_and_instances_as_data_models() -> None:
    with pytest.raises(pydantic.ValidationError, match="cannot be Any"):
        DataItem(allow_data_model=Any)

    with pytest.raises(pydantic.ValidationError, match="must be a class/type"):
        DataItem(allow_data_model=Payload())


def test_data_item_accepts_custom_class_objects() -> None:
    data_item = DataItem(allow_data_model=Payload)

    assert data_item.allow_data_model is Payload


def test_auto_generate_ref_return_key_and_index() -> None:
    def source_key() -> Ref[bool, ("target", "local_key", "target_key")]:
        return True

    def source_index() -> Ref[str, ("target", 0, 1)]:
        return "ready"

    def target() -> None:
        return None

    transports = auto_generate_data_transports(
        _orders("source_key", "source_index", "target"),
        {"source_key": source_key, "source_index": source_index, "target": target},
    )

    key_data = next(item for item in transports if item.task_id == "source_key")
    index_data = next(item for item in transports if item.task_id == "source_index")

    key_item = key_data.output_data_item[0]
    assert key_item.from_relation.key == "local_key"
    assert key_item.to_relation.key == "target_key"

    index_item = index_data.output_data_item[0]
    assert index_item.from_relation.index == 0
    assert index_item.to_relation.index == 1


def test_auto_generate_to_return_key_and_index() -> None:
    def source_key() -> Annotated[dict, To("target", "local_key", "target_key")]:
        return {"local_key": True}

    def source_index() -> Annotated[tuple, T("target", 0, 1)]:
        return "ready", "done"

    def target() -> None:
        return None

    transports = auto_generate_data_transports(
        _orders("source_key", "source_index", "target"),
        {"source_key": source_key, "source_index": source_index, "target": target},
    )

    key_data = next(item for item in transports if item.task_id == "source_key")
    index_data = next(item for item in transports if item.task_id == "source_index")

    key_item = key_data.output_data_item[0]
    assert key_item.from_relation.key == "local_key"
    assert key_item.to_relation.key == "target_key"

    index_item = index_data.output_data_item[0]
    assert index_item.from_relation.index == 0
    assert index_item.to_relation.index == 1


def test_auto_generate_rejects_from_to_direction_misuse() -> None:
    def source() -> None:
        return None

    def to_on_param(value: Annotated[int, To("source", "x", "y")]) -> None:
        return None

    def from_on_return() -> Annotated[int, From("source", "x")]:
        return 1

    with pytest.raises(ValueError, match=r"To\(\.\.\.\) metadata is only valid on return annotations"):
        auto_generate_data_transports(_orders("source", "to_on_param"), {"source": source, "to_on_param": to_on_param})

    with pytest.raises(ValueError, match=r"From\(\.\.\.\) metadata is only valid on parameter annotations"):
        auto_generate_data_transports(_orders("source", "from_on_return"), {"source": source, "from_on_return": from_on_return})


def test_auto_generate_rejects_multiple_relation_metadata() -> None:
    def source() -> None:
        return None

    def target(value: Annotated[int, From("source", "a"), ("source", "b")]) -> None:
        return None

    with pytest.raises(ValueError, match="Annotated relation metadata is ambiguous"):
        auto_generate_data_transports(_orders("source", "target"), {"source": source, "target": target})


def test_auto_generate_rejects_invalid_metadata() -> None:
    def source() -> None:
        return None

    def bad_shape(value: Ref[int, ("source", "key", "extra")]) -> None:
        return None

    def bad_locator(value: Ref[int, ("source", object())]) -> None:
        return None

    with pytest.raises(ValueError, match="Invalid input relation"):
        auto_generate_data_transports(_orders("source", "bad_shape"), {"source": source, "bad_shape": bad_shape})

    with pytest.raises(ValueError, match="Invalid relation locator"):
        auto_generate_data_transports(_orders("source", "bad_locator"), {"source": source, "bad_locator": bad_locator})


def test_auto_generate_rejects_unknown_task_id() -> None:
    def target(value: Ref[int, ("missing", "key")]) -> None:
        return None

    with pytest.raises(ValueError, match="Unknown task id 'missing'"):
        auto_generate_data_transports(_orders("target"), {"target": target})


def test_auto_generate_rejects_unknown_callable_reference() -> None:
    def missing() -> None:
        return None

    def target(value: Annotated[int, F(missing, "value")]) -> None:
        return None

    with pytest.raises(ValueError, match="Unknown callable task reference"):
        auto_generate_data_transports(_orders("target"), {"target": target})


def test_auto_generate_rejects_duplicate_callable_reference() -> None:
    def shared() -> dict:
        return {"value": 1}

    with pytest.raises(ValueError, match="Callable task reference is ambiguous"):
        auto_generate_data_transports(_orders("a", "b"), {"a": shared, "b": shared})


def test_auto_generate_future_annotations_callable_reference() -> None:
    namespace: dict[str, Any] = {}
    exec(
        "from __future__ import annotations\n"
        "from typing import Annotated\n"
        "from astrum.data_transport import F\n"
        "def source() -> dict:\n"
        "    return {'value': 1}\n"
        "def target(value: Annotated[int, F(source, 'value')]) -> None:\n"
        "    return None\n",
        namespace,
    )

    transports = auto_generate_data_transports(
        _orders("source", "target"),
        {"source": namespace["source"], "target": namespace["target"]},
    )
    target_data = next(item for item in transports if item.task_id == "target")

    assert target_data.input_data_item[0].from_relation.related_task == "source"


@pytest.mark.asyncio
async def test_decorator_integration_runs_with_generated_data() -> None:
    workflow = SchedulerRegistry("auto-data")
    received: list[bool] = []

    @workflow.task("source")
    async def source() -> dict:
        return {"beans_ready": True}

    @workflow.task("target")
    async def target(beans: Ref[bool, ("source", "beans_ready")]) -> None:
        received.append(beans)

    report = await workflow.run(["target"], config=AstrumConfig(skip_type_check=True))

    assert report.execution_state == "completed"
    assert received == [True]
