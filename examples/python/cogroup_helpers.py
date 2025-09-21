"""Utility helpers for the Python cogroup benchmark.

These helpers are imported by ``cogroup_benchmark.py`` but are also kept free of
``surfingprocessorspy`` so they can be unit tested without building the native
extension.  The small test suite under ``tests/python`` validates the behaviours
we rely on in the benchmark script (environment detection, Arrow batch
generation, and optional sorting).
"""

from __future__ import annotations

import os
import random
from typing import Iterable, Mapping, MutableMapping

import pyarrow as pa
import pyarrow.compute as pc

ENV_RANK_VARS = ("OMPI_COMM_WORLD_RANK", "PMI_RANK")
ENV_WORLD_VARS = ("OMPI_COMM_WORLD_SIZE", "PMI_SIZE")


def omni_rank(env: Mapping[str, str] | MutableMapping[str, str] | None = None) -> int:
    """Return the rank from common MPI environment variables.

    A ``Mapping`` can be passed explicitly for tests; otherwise ``os.environ`` is
    consulted.  Invalid values are treated as missing and we fall back to rank 0.
    """

    env = os.environ if env is None else env
    for var in ENV_RANK_VARS:
        value = env.get(var)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0
    return 0


def omni_world(env: Mapping[str, str] | MutableMapping[str, str] | None = None) -> int:
    """Return the world size from common MPI environment variables."""

    env = os.environ if env is None else env
    for var in ENV_WORLD_VARS:
        value = env.get(var)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return 1
    return 1


def make_random_batch(rows: int, seed: int, value_name: str) -> pa.RecordBatch:
    """Deterministically generate a two-column Arrow ``RecordBatch``.

    ``value_name`` controls the name of the second column so the benchmark can
    synthesise left/right payloads with unique field names.
    """

    rng = random.Random(seed)
    keys = [rng.randint(-(1 << 63), (1 << 63) - 1) for _ in range(rows)]
    values = list(range(rows))
    arrays = [pa.array(keys, type=pa.int64()), pa.array(values, type=pa.int32())]
    return pa.record_batch(arrays, names=["key", value_name])


def make_nested_struct_batch(rows: int, seed: int) -> pa.RecordBatch:
    """Generate a batch containing nested list/map-of-struct columns.

    The shape mimics the data structures exercised in the cogroup benchmark so
    the Python tests stress Arrow conversions similar to the production flow.
    """

    rng = random.Random(seed)
    keys = [rng.randint(-(1 << 31), (1 << 31) - 1) for _ in range(rows)]
    base_values = [rng.random() for _ in range(rows)]

    nested_list_type = pa.list_(
        pa.struct([
            pa.field("item_key", pa.int64()),
            pa.field("item_value", pa.float64()),
        ])
    )
    nested_map_type = pa.map_(pa.string(), pa.struct([
        pa.field("count", pa.int32()),
        pa.field("flag", pa.bool_()),
    ]))

    list_entries = []
    map_entries = []
    for idx, (k, v) in enumerate(zip(keys, base_values)):
        list_entries.append([
            {"item_key": k, "item_value": v},
            {"item_key": k + 1, "item_value": v + 0.5},
        ])
        map_entries.append({
            f"k{idx}": {"count": idx, "flag": idx % 2 == 0},
            f"k{idx}_alt": {"count": idx * 2, "flag": idx % 3 == 0},
        })

    batch = pa.record_batch(
        [
            pa.array(keys, type=pa.int64()),
            pa.array(base_values, type=pa.float64()),
            pa.array(list_entries, type=nested_list_type),
            pa.array(map_entries, type=nested_map_type),
        ],
        names=["id", "metric", "nested_list", "nested_map"],
    )
    return batch


def sort_record_batch(batch: pa.RecordBatch, field: str) -> pa.RecordBatch:
    """Return a copy of ``batch`` sorted by ``field`` if it exists.

    When ``field`` is not present the input batch is returned unchanged.  Arrow's
    compute kernels are used so the code mirrors the benchmark path.
    """

    if field not in batch.schema.names:
        return batch
    table = pa.Table.from_batches([batch])
    indices = pc.sort_indices(table, sort_keys=[(field, "ascending")])
    sorted_table = table.take(indices)
    return sorted_table.to_batches(max_chunksize=batch.num_rows)[0]


def dump_sample(batch: pa.RecordBatch, label: str, handle, limit: int = 5) -> None:
    """Write a small textual sample of ``batch`` to ``handle``.

    Keeping this helper here avoids having to import ``pyarrow`` at module scope
    inside the benchmark when users only need the CLI parser (e.g. ``--help``).
    """

    rows = min(limit, batch.num_rows)
    handle.write(f"sample_{label} (first {rows})\n")
    names = batch.schema.names
    for i in range(rows):
        parts = []
        for col_idx, name in enumerate(names):
            column = batch.column(col_idx)
            value = column[i].as_py()
            parts.append(f"{name}={value}")
        handle.write(f"  {i}: {', '.join(parts)}\n")


__all__ = [
    "ENV_RANK_VARS",
    "ENV_WORLD_VARS",
    "omni_rank",
    "omni_world",
    "make_random_batch",
    "make_nested_struct_batch",
    "sort_record_batch",
    "dump_sample",
]
