#!/usr/bin/env python3
# Minimal MCP server implemented with fastmcp using StreamableHTTP transport.
# Accepts shuffle/cogroup tasks, executes them sequentially across all hosts
# via mpiexec, and streams async progress updates during execution.

import asyncio
import base64
import json
import os
import shlex
import sys
from pathlib import Path

from fastmcp.server.server import FastMCP
from fastmcp.server import server as srv
from fastmcp import Context


app = FastMCP(
    name="surfing-mcp",
    instructions="Surfing DB task coordinator (MPI shuffle/cogroup)",
)

_seq_lock = asyncio.Lock()  # ensures FIFO execution


def _default_paths():
    root = Path(__file__).resolve().parents[1]
    jar = root / "drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar"
    lib = root / "build"
    return str(jar), str(lib)


@app.tool(
    name="submit_task",
    description="Submit a shuffle/cogroup task. Executes sequentially across hosts and streams progress.",
)
async def submit_task(
    mode: str,
    keyField: str,
    outputS3: str,
    leftS3: list[str] | None = None,
    rightS3: list[str] | None = None,
    oneSided: bool = True,
    thriftDir: str | None = None,
    thriftStruct: str | None = None,
    thriftFile: str | None = None,
    np: int = 2,
    hostfile: str | None = None,
    iface: str | None = None,
    javaJar: str | None = None,
    javaLibPath: str | None = None,
    ctx: Context | None = None,
) -> dict:
    """
    Args:
      mode: 'shuffle' or 'cogroup'
      keyField: name of key column
      outputS3: output S3 prefix
      leftS3/rightS3: input prefixes (base64 lines of thrift payloads)
      oneSided: use one-sided MPI (true) or two-sided
      thrift* : location and struct for decoding payloads
      np: total MPI ranks
      hostfile: optional path to hostfile
      iface: optional network interface for OpenMPI include
      javaJar/javaLibPath: overrides for jar/lib path
    Returns: {taskId, status}
    """

    assert mode in ("shuffle", "cogroup"), "mode must be shuffle or cogroup"
    assert outputS3, "outputS3 required"
    if mode == "shuffle":
        assert leftS3 and len(leftS3) > 0, "leftS3 required for shuffle"
    else:
        assert leftS3 and rightS3 and len(leftS3) > 0 and len(rightS3) > 0, "leftS3/rightS3 required for cogroup"
    assert thriftDir and thriftStruct, "thriftDir and thriftStruct required"

    task = {
        "taskId": os.urandom(8).hex(),
        "mode": mode,
        "oneSided": oneSided,
        "keyField": keyField,
        "leftS3": leftS3,
        "rightS3": rightS3,
        "thrift": {"dir": thriftDir, "struct": thriftStruct, "file": thriftFile or ""},
        "outputS3": outputS3,
    }

    jar, lib = _default_paths()
    if javaJar: jar = javaJar
    if javaLibPath: lib = javaLibPath

    # Build mpiexec command
    java = "java"
    cp = jar
    runner = "org.surfing.drsquirrel.jni.McpWorkerRunner"
    encoded = base64.b64encode(json.dumps(task).encode()).decode()
    json_arg = f"json:{encoded}"

    iface_opt = f"--mca btl_tcp_if_include {shlex.quote(iface)}" if iface else "--mca btl_tcp_if_exclude lo,docker0"
    hostfile_opt = f"--hostfile {shlex.quote(hostfile)}" if hostfile else ""

    if np <= 1:
        cmd = f"{shlex.quote(java)} -Djava.library.path={shlex.quote(lib)} -cp {shlex.quote(cp)} {runner} {shlex.quote(json_arg)}"
    else:
        cmd = (
            f"mpiexec -np {np} {hostfile_opt} --use-hwthread-cpus --oversubscribe --map-by core --bind-to core "
            f"{iface_opt} "
            f"{shlex.quote(java)} -Djava.library.path={shlex.quote(lib)} -cp {shlex.quote(cp)} {runner} {shlex.quote(json_arg)}"
        )

    # Enforce FIFO execution
    await ctx.report_progress(0, 100, "Queued")
    async with _seq_lock:
        await ctx.report_progress(5, 100, "Launching mpiexec")
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={**os.environ},
        )

        # Stream stdout while reporting coarse progress
        progress = 10
        try:
            assert proc.stdout is not None
            async for line in proc.stdout:
                txt = line.decode(errors="ignore").rstrip()
                # Heuristic progress bumps
                if "[fake-cogroup] iter=" in txt or "rows_per_sec=" in txt:
                    progress = min(progress + 10, 90)
                    await ctx.report_progress(progress, 100, txt)
                elif "Post-shuffle spill" in txt:
                    progress = min(progress + 5, 95)
                    await ctx.report_progress(progress, 100, txt)
                elif txt:
                    # forward occasional output for visibility
                    await ctx.report_progress(progress, 100, txt[:200])
        finally:
            rc = await proc.wait()
        if rc != 0:
            await ctx.report_progress(100, 100, f"Failed (rc={rc})")
            return {"taskId": task["taskId"], "status": f"failed:{rc}"}
        await ctx.report_progress(100, 100, "Completed")
        return {"taskId": task["taskId"], "status": "done"}


@app.tool(name="ping", description="Ping the server")
async def ping() -> str:
    return "pong"


def main():
    import uvicorn
    from fastmcp.server.server import create_streamable_http_app

    # Build ASGI app for StreamableHTTP
    asgi_app = create_streamable_http_app(app, streamable_http_path="/mcp")
    uvicorn.run(asgi_app, host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8081")))


if __name__ == "__main__":
    main()
