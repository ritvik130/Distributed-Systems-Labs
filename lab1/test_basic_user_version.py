#!/usr/bin/env python3
"""Integration tests for Lab 1's Basic HTTP Server (stdlib only)."""
import concurrent.futures
import http.client
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

PROJECT = Path.cwd()  # Run this script from the folder with main.go
BINARY = PROJECT / 'http_server'

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def http_request(port, method, route, data=None, headers=None):
    c = http.client.HTTPConnection('127.0.0.1', port, timeout=8)
    try:
        c.request(method, route, body=data, headers=headers or {})
        r = c.getresponse()
        result = (r.status, r.getheader('Content-Type'), r.read())
        return result
    finally:
        c.close()

def raw_request(port, data, half_close=False):
    with socket.create_connection(('127.0.0.1', port), timeout=5) as s:
        s.settimeout(8)
        s.sendall(data)
        if half_close:
            s.shutdown(socket.SHUT_WR)
        parts = []
        while True:
            part = s.recv(65536)
            if not part:
                break
            parts.append(part)
        response = b''.join(parts)
    return response

def check(cond, desc):
    if not cond:
        raise AssertionError(desc)
    print('PASS',desc)

def main():
    subprocess.run(['go', 'build', '-o', str(BINARY), 'main.go'], cwd=PROJECT, check=True)
    with tempfile.TemporaryDirectory() as work:
        public = Path(work)/'public'
        public.mkdir()
        (public/'hello.txt').write_bytes(b'old version\n')
        (public/'index.html').write_bytes(b'<h1>hello</h1>')
        (public/'a.css').write_bytes(b'body{}')
        (public/'a.gif').write_bytes(b'GIF89a')
        (public/'a.jpeg').write_bytes(b'jpeg')
        (public/'a.jpg').write_bytes(b'jpg')
        port = free_port()
        server_log_path = Path(work) / 'server.log'
        server_log = open(server_log_path, 'wb')
        server = subprocess.Popen([str(BINARY), str(port)], cwd=work,
                                  stdout=subprocess.DEVNULL, stderr=server_log)
        try:
            for _ in range(100):
                if server.poll() is not None:
                    raise RuntimeError('server exited early')
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=.05):
                        break
                except OSError:
                    time.sleep(.025)
            else:
                raise RuntimeError('server did not listen')

            for filename, mime in [('hello.txt','text/plain'),('index.html','text/html'),('a.css','text/css'),('a.gif','image/gif'),('a.jpeg','image/jpeg'),('a.jpg','image/jpeg')]:
                status, contenttype, body = http_request(port,'GET','/'+filename)
                check((status,contenttype,body)==(200,mime,(public/filename).read_bytes()),'GET '+filename)

            check(http_request(port,'GET','/missing.txt')[0] == 404,'404 nonexistent file')
            check(http_request(port,'GET','/forbidden.exe')[0] == 400,'400 unsupported extension')
            check(http_request(port,'GET','/%2e%2e/secrets.txt')[0] == 400,'400 path traversal')
            check(http_request(port,'DELETE','/hello.txt')[0] == 501,'501 unsupported method')
            check(b'400 Bad Request' in raw_request(port,b'GET /hello.txt HTTP/1.1\r\nBrokenHeader\r\n\r\n'),'400 malformed headers')

            check(b'400 Bad Request' in raw_request(
                port, b'POST /big.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 67108865\r\n\r\n'),
                '400 oversized upload')
            check(b'200 OK' in raw_request(
                port, b'POST /chunked.txt HTTP/1.1\r\nHost: localhost\r\n'
                      b'Transfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n0\r\n\r\n'),
                'POST chunked body')
            check(http_request(port,'GET','/chunked.txt')[2] == b'Hello',
                  'GET sees decoded chunked POST')

            check(http_request(port,'POST','/hello.txt',b'new version')[0] == 200,'POST overwrite')
            check(http_request(port,'GET','/hello.txt')[2] == b'new version','GET sees POST data')
            check(http_request(port,'POST','/new/sub/directory/cat.jpg',b'\x00\x01binary\xff')[0]==200,'POST creates directories and binary file')
            check(http_request(port,'GET','/new/sub/directory/cat.jpg')[2] == b'\x00\x01binary\xff','GET binary upload round-trip')
            check(http_request(port,'POST','/empty.txt',b'')[0] == 200,'empty POST accepted')
            check(http_request(port,'GET','/empty.txt')[2] == b'','empty GET response')

            # A truncated upload must not replace the existing complete version.
            resp = raw_request(port,b'POST /hello.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\nXYZ',half_close=True)
            check(b'400 Bad Request' in resp, '400 truncated upload')
            check(http_request(port,'GET','/hello.txt')[2] == b'new version', 'failed POST leaves original intact')

            # Multiple writers publish complete versions; readers must never see a mixture.
            a = b'A'*32768
            b = b'B'*32768
            check(http_request(port,'POST','/racing.txt',a)[0] == 200,'seed atomicity test')
            def writer(payload):
                for _ in range(15):
                    if http_request(port,'POST','/racing.txt',payload)[0]!=200:
                        return False
                return True
            def reader():
                for _ in range(50):
                    status, _, content = http_request(port,'GET','/racing.txt')
                    if status!=200 or content not in (a,b):
                        return False
                return True
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                futures=[pool.submit(writer, a),pool.submit(writer,b),pool.submit(writer,a),pool.submit(writer,b),pool.submit(reader),pool.submit(reader),pool.submit(reader),pool.submit(reader)]
                check(all(f.result() for f in futures),'concurrent GET/POST never mixed')

            # Occupy 10 goroutines with unfinished headers; an 11th must wait.
            blockers=[]
            try:
                for _ in range(10):
                    s=socket.create_connection(('127.0.0.1',port),timeout=5)
                    s.sendall(b'GET /hello.txt HTTP/1.1\r\nHost: localhost\r\n')
                    blockers.append(s)
                time.sleep(.4)
                probe=socket.create_connection(('127.0.0.1',port),timeout=5)
                try:
                    probe.settimeout(.5)
                    probe.sendall(b'GET /hello.txt HTTP/1.1\r\nHost: localhost\r\n\r\n')
                    try:
                        got=probe.recv(100)
                        raise AssertionError(f'11th request processed too soon: {got!r}')
                    except socket.timeout:
                        pass
                    blockers[0].close()
                    probe.settimeout(5)
                    got=probe.recv(4096)
                    check(b'HTTP/1.1 200 OK' in got, '11th waits for one of 10 slots')
                finally:
                    probe.close()
            finally:
                for s in blockers:
                    s.close()

            print('\nALL BASIC TESTS PASSED')
        finally:
            server.terminate()
            try:
                server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
            server_log.close()
            if b'WARNING: DATA RACE' in server_log_path.read_bytes():
                raise AssertionError('Go race detector reported a DATA RACE')

if __name__=='__main__':
    main()
