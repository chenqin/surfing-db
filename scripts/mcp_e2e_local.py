#!/usr/bin/env python3
import os, sys, base64, time, subprocess, json, socket, tempfile, shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def find_free_port(start=18080):
    p = start
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('127.0.0.1', p))
                return p
            except OSError:
                p += 1

def write_thrift(dirp: Path):
    dirp.mkdir(parents=True, exist_ok=True)
    content = """
namespace cpp test

struct KV {
  1: i64 key
  2: i32 val
}
""".strip()
    (dirp / 'kv.thrift').write_text(content)

def thrift_struct_payload(key: int, val: int) -> bytes:
    # Thrift Binary encoding for struct KV {1:i64 key, 2:i32 val}
    # field type I64 (0x0A), id=1 (0x00 0x01), 8 bytes; then I32 (0x08), id=2, 4 bytes; then STOP (0x00)
    b = bytearray()
    b.append(0x0A); b.extend((0x00, 0x01)); b.extend(key.to_bytes(8, 'big', signed=True))
    b.append(0x08); b.extend((0x00, 0x02)); b.extend(val.to_bytes(4, 'big', signed=True))
    b.append(0x00)
    return bytes(b)

def prepare_inputs(base: Path):
    left = base / 'in_left'; left.mkdir(parents=True, exist_ok=True)
    lines = []
    for i in range(10):
        payload = thrift_struct_payload(i, i*10)
        lines.append(base64.b64encode(payload).decode())
    (left / 'part-000').write_text('\n'.join(lines) + '\n')
    return left

def main():
    build = ROOT / 'build'
    jar = ROOT / 'drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar'
    if not jar.exists():
        print('[e2e] Building drsquirrel-java shaded jar...')
        subprocess.check_call(['mvn','-q','-f', str(ROOT/'drsquirrel-java/pom.xml'), '-Darrow.version=12.0.0','-DskipTests','package'])
    # Prepare local workspace
    work = ROOT / 'build' / 'e2e'
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    thrift_dir = work / 'thrift'
    write_thrift(thrift_dir)
    left_dir = prepare_inputs(work)
    out_dir = work / 'out'
    out_dir.mkdir(exist_ok=True)

    # Start fastmcp server
    port = find_free_port(19000)
    env = os.environ.copy()
    # Prefer venv python if available
    py = sys.executable
    server = subprocess.Popen([py, str(ROOT/'scripts/mcp_fastmcp_server.py')], env=env)
    try:
        # Wait for server
        time.sleep(1.5)
        # Submit client request (np=1 local)
        client_cmd = [
            py, str(ROOT/'scripts/mcp_client_submit.py'),
            '--url', f'http://localhost:8081/mcp',
            '--mode','shuffle', '--key','key',
            '--out', f'file://{out_dir}',
            '--left', f'file://{left_dir}',
            '--thrift-dir', f'file://{thrift_dir}', '--thrift-struct','KV',
            '--np','1', '--jar', str(jar), '--lib', str(build)
        ]
        print('[e2e] Submitting task...')
        subprocess.check_call(client_cmd)
        # Validate output
        outs = list(out_dir.glob('rank-*.arrow'))
        assert len(outs) >= 1, 'no output files'
        print('[e2e] OK, outputs:', [p.name for p in outs])
    finally:
        server.terminate()
        try: server.wait(timeout=2)
        except Exception: server.kill()

if __name__ == '__main__':
    main()

