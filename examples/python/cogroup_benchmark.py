#!/usr/bin/env python3
"""MPI cogroup benchmark using the surfingprocessorspy module."""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Iterable

# Ensure the build directory (which houses surfingprocessorspy) is importable.
REPO_ROOT = Path(__file__).resolve().parents[2]
BUILD_DIR = REPO_ROOT / "build"
if str(BUILD_DIR) not in sys.path:
    sys.path.insert(0, str(BUILD_DIR))

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import cogroup_helpers as helpers

try:
    import pyarrow as pa
except ImportError as exc:  # pragma: no cover - dependency warning path
    raise SystemExit("pyarrow is required to run cogroup_benchmark.py: " + str(exc)) from exc

import surfingprocessorspy as spp


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Python cogroup benchmark using surfingprocessorspy")
    parser.add_argument("--mode", choices=("one", "two"), default="one", help="one-sided or two-sided shuffle (default: one)")
    parser.add_argument("--rows", type=int, default=100_000, help="Rows per rank (default: 100000)")
    parser.add_argument("--iters", type=int, default=1, help="Iterations (default: 1)")
    parser.add_argument("--out", default=str(REPO_ROOT / "build" / "examples" / "fake-cogroup-out"), help="Output directory for samples")
    parser.add_argument("--sort-by", default="", help="Optional field name to sort outputs by before sampling")
    parser.add_argument("--seed", type=int, default=int(time.time()), help="Base RNG seed (default: current time)")
    return parser.parse_args(list(argv))


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)
    # Initialise MPI through the bindings and pick rank/world.
    rank = spp.mpi_rank()
    world = spp.mpi_world()

    # Fallback for schedulers that do not set OMPI/PMI env vars before MPI_Init_thread.
    if world <= 1:
        env_world = helpers.omni_world()
        env_rank = helpers.omni_rank()
        if env_world > 1:
            world = env_world
            rank = env_rank

    one_sided = args.mode != "two"
    out_dir = Path(args.out)
    if rank == 0:
        out_dir.mkdir(parents=True, exist_ok=True)
    spp.barrier()
    if rank != 0:
        out_dir.mkdir(parents=True, exist_ok=True)

    # Prepare inputs (either random batches or future payload readers).
    left = helpers.make_random_batch(args.rows, args.seed + rank, "la")
    right = helpers.make_random_batch(args.rows, args.seed + rank + 17, "rb")
    rows_per_rank = left.num_rows

    if rank == 0:
        print(f"[python-cogroup] world={world} rows_per_rank={rows_per_rank} iters={args.iters} mode={'one' if one_sided else 'two'} out={out_dir}")

    # Warmup call.
    warm_left, warm_right = spp.cogroup(left, right, "key", one_sided, rank, world)
    del warm_left, warm_right

    best = float("inf")
    total = 0.0

    for iteration in range(args.iters):
        t0 = time.perf_counter()
        out_left, out_right = spp.cogroup(left, right, "key", one_sided, rank, world)
        t1 = time.perf_counter()
        elapsed = t1 - t0
        best = min(best, elapsed)
        total += elapsed

        if args.sort_by:
            if args.sort_by in out_left.schema.names:
                out_left = helpers.sort_record_batch(out_left, args.sort_by)
            if args.sort_by in out_right.schema.names:
                out_right = helpers.sort_record_batch(out_right, args.sort_by)

        sample_path = out_dir / f"rank-{rank}-iter-{iteration}-sample.txt"
        with sample_path.open("w", encoding="utf-8") as handle:
            handle.write(f"rank={rank} world={world} rowsL={out_left.num_rows} rowsR={out_right.num_rows} time_s={elapsed:.6f} mode={'one' if one_sided else 'two'}\n")
            if args.sort_by:
                handle.write(f"sorted_by={args.sort_by}\n")
            helpers.dump_sample(out_left, "left", handle)
            helpers.dump_sample(out_right, "right", handle)

        if rank == 0:
            total_rows = rows_per_rank * world * 2
            rows_per_second = total_rows / elapsed if elapsed > 0 else float("inf")
            print(f"[python-cogroup] iter={iteration} rows={total_rows} time_s={elapsed:.6f} rows_per_sec={rows_per_second:.0f}")

    if rank == 0:
        avg = total / max(args.iters, 1)
        print(f"[python-cogroup] best_s={best:.6f} avg_s={avg:.6f}")

    spp.barrier()
    spp.finalize_mpi()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
