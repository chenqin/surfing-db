#!/usr/bin/env python3
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

queue_lock = threading.Lock()
task_queue = []  # FIFO list of dicts
lease_active = False

class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype='application/json'):
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.end_headers()
        if isinstance(body, (dict, list)):
            body = json.dumps(body)
        if isinstance(body, str):
            body = body.encode('utf-8')
        self.wfile.write(body)

    def do_GET(self):
        global lease_active
        if self.path.startswith('/lease'):
            with queue_lock:
                if lease_active or not task_queue:
                    self._send(200, 'NONE', 'text/plain')
                    return
                task = task_queue[0]
                lease_active = True
                self._send(200, task)
        elif self.path.startswith('/queue'):
            with queue_lock:
                self._send(200, { 'size': len(task_queue), 'tasks': task_queue })
        else:
            self._send(404, { 'error': 'not found' })

    def do_POST(self):
        global lease_active
        length = int(self.headers.get('Content-Length', '0') or '0')
        data = self.rfile.read(length).decode('utf-8') if length > 0 else '{}'
        try:
            body = json.loads(data)
        except Exception:
            body = {}
        if self.path.startswith('/enqueue'):
            with queue_lock:
                task_queue.append(body)
                self._send(200, { 'enqueued': True, 'size': len(task_queue) })
        elif self.path.startswith('/complete'):
            with queue_lock:
                if lease_active and task_queue:
                    task_queue.pop(0)
                    lease_active = False
                    self._send(200, { 'ok': True, 'remaining': len(task_queue) })
                else:
                    self._send(409, { 'ok': False, 'error': 'no active lease' })
        else:
            self._send(404, { 'error': 'not found' })

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default='0.0.0.0')
    ap.add_argument('--port', type=int, default=8080)
    args = ap.parse_args()
    httpd = HTTPServer((args.host, args.port), Handler)
    print(f"[MCP] listening on {args.host}:{args.port}")
    httpd.serve_forever()

if __name__ == '__main__':
    main()

