#!/usr/bin/env python3
import argparse
import asyncio
from datetime import timedelta

from fastmcp.client import Client, transports


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--url', default='http://localhost:8081/mcp')
    ap.add_argument('--mode', default='shuffle')
    ap.add_argument('--key', default='key')
    ap.add_argument('--out', required=True, help='file:// or s3:// output prefix')
    ap.add_argument('--left', action='append', required=True, help='file:// or s3:// left input (repeatable)')
    ap.add_argument('--right', action='append', help='file:// or s3:// right input (cogroup)')
    ap.add_argument('--one-sided', action='store_true')
    ap.add_argument('--thrift-dir', required=True)
    ap.add_argument('--thrift-struct', required=True)
    ap.add_argument('--thrift-file', default='')
    ap.add_argument('--np', type=int, default=1)
    ap.add_argument('--hostfile', default=None)
    ap.add_argument('--iface', default=None)
    ap.add_argument('--jar', default=None)
    ap.add_argument('--lib', default=None)
    args = ap.parse_args()

    transport = transports.infer_transport(args.url)
    client = Client(transport)

    def on_prog(p):
        print(f"progress: {p.progress}/{p.total} {p.message}")

    req = {
        'mode': args.mode,
        'keyField': args.key,
        'outputS3': args.out,
        'leftS3': args.left,
        'rightS3': args.right,
        'oneSided': bool(args.one_sided),
        'thriftDir': args.thrift_dir,
        'thriftStruct': args.thrift_struct,
        'thriftFile': args.thrift_file,
        'np': args.np,
        'hostfile': args.hostfile,
        'iface': args.iface,
        'javaJar': args.jar,
        'javaLibPath': args.lib,
    }
    res = await client.call_tool('submit_task', req, progress_handler=on_prog, timeout=timedelta(hours=2))
    print('result:', res.content)


if __name__ == '__main__':
    asyncio.run(main())

