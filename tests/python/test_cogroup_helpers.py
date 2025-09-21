from __future__ import annotations

import io
import sys
from pathlib import Path

import pyarrow as pa
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER_DIR = REPO_ROOT / "examples" / "python"
if str(HELPER_DIR) not in sys.path:
    sys.path.insert(0, str(HELPER_DIR))

import cogroup_helpers as helpers  # noqa: E402  (added to path above)


def test_omni_rank_prefers_valid_env(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {helpers.ENV_RANK_VARS[0]: "7"}
    assert helpers.omni_rank(env) == 7

    env_bad = {helpers.ENV_RANK_VARS[0]: "abc"}
    assert helpers.omni_rank(env_bad) == 0

    monkeypatch.setenv(helpers.ENV_RANK_VARS[0], "12")
    try:
        assert helpers.omni_rank() == 12
    finally:
        monkeypatch.delenv(helpers.ENV_RANK_VARS[0], raising=False)


def test_omni_world_prefers_valid_env(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {helpers.ENV_WORLD_VARS[0]: "4"}
    assert helpers.omni_world(env) == 4

    env_bad = {helpers.ENV_WORLD_VARS[0]: ""}
    assert helpers.omni_world(env_bad) == 1

    monkeypatch.setenv(helpers.ENV_WORLD_VARS[0], "8")
    try:
        assert helpers.omni_world() == 8
    finally:
        monkeypatch.delenv(helpers.ENV_WORLD_VARS[0], raising=False)


def test_make_random_batch_is_deterministic() -> None:
    batch_a = helpers.make_random_batch(rows=4, seed=123, value_name="vx")
    batch_b = helpers.make_random_batch(rows=4, seed=123, value_name="vx")
    assert batch_a.equals(batch_b)


def test_make_nested_struct_batch_has_expected_schema() -> None:
    batch = helpers.make_nested_struct_batch(rows=3, seed=321)
    assert batch.schema.names == ["id", "metric", "nested_list", "nested_map"]

    list_field = batch.schema.field("nested_list")
    assert pa.types.is_list(list_field.type)
    struct_type = list_field.type.value_type
    assert struct_type.names == ["item_key", "item_value"]

    map_field = batch.schema.field("nested_map")
    assert pa.types.is_map(map_field.type)
    value_type = map_field.type.item_type
    assert value_type.names == ["count", "flag"]

    # Ensure the contents are converted to Python objects correctly.
    first_row = batch.column(2)[0].as_py()
    assert isinstance(first_row, list)
    assert first_row[0]["item_key"] != first_row[1]["item_key"]


def test_sort_record_batch_sorts_when_field_present() -> None:
    batch = pa.record_batch(
        [pa.array([3, 1, 2], type=pa.int64()), pa.array([30, 10, 20], type=pa.int32())],
        names=["key", "val"],
    )
    sorted_batch = helpers.sort_record_batch(batch, "val")
    assert sorted_batch.column(1).to_pylist() == [10, 20, 30]
    assert sorted_batch.column(0).to_pylist() == [1, 2, 3]


def test_sort_record_batch_returns_input_when_missing_field() -> None:
    batch = helpers.make_random_batch(rows=2, seed=99, value_name="vx")
    assert helpers.sort_record_batch(batch, "missing") is batch


def test_dump_sample_writes_expected_output() -> None:
    batch = pa.record_batch(
        [pa.array([1, 2], type=pa.int64()), pa.array([10, 20], type=pa.int32())],
        names=["key", "val"],
    )
    buffer = io.StringIO()
    helpers.dump_sample(batch, "left", buffer, limit=1)
    text = buffer.getvalue()
    assert "sample_left" in text
    assert "key=1" in text
    assert "val=10" in text


def test_dump_sample_handles_nested_values() -> None:
    batch = helpers.make_nested_struct_batch(rows=1, seed=999)
    buffer = io.StringIO()
    helpers.dump_sample(batch, "nested", buffer, limit=1)
    text = buffer.getvalue()
    assert "nested" in text
    assert "nested_list" in text
    assert "nested_map" in text
