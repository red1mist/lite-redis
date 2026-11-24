#!/usr/bin/env python3
"""Stage 2 tests: the epoll echo server must handle things the blocking
Stage 1 server could not. Run against a live server:

    ./dev make run                       # terminal 1
    ./dev python3 test/test_echo.py      # terminal 2
"""
import os
import socket
import sys
import threading
import time

HOST, PORT = "127.0.0.1", int(os.environ.get("PORT", 6379))


def connect():
    s = socket.create_connection((HOST, PORT), timeout=5)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def recv_exact(s, n):
    out = bytearray()
    while len(out) < n:
        chunk = s.recv(min(1 << 16, n - len(out)))
        if not chunk:
            raise ConnectionError(f"EOF after {len(out)}/{n} bytes")
        out += chunk
    return bytes(out)


def test_basic():
    with connect() as s:
        s.sendall(b"hello")
        assert recv_exact(s, 5) == b"hello"


def test_concurrent_clients_interleaved():
    # Stage 1 would hang here: client B is stuck in the backlog until A leaves.
    a, b = connect(), connect()
    try:
        b.sendall(b"from-b")
        assert recv_exact(b, 6) == b"from-b"
        a.sendall(b"from-a")
        assert recv_exact(a, 6) == b"from-a"
    finally:
        a.close()
        b.close()


def test_many_clients():
    socks = [connect() for _ in range(200)]
    try:
        for i, s in enumerate(socks):
            s.sendall(f"msg-{i:04d}".encode())
        for i, s in enumerate(socks):
            assert recv_exact(s, 8) == f"msg-{i:04d}".encode()
    finally:
        for s in socks:
            s.close()


def test_large_payload_partial_writes():
    # 32 MB is far bigger than the kernel socket buffers, so the server's
    # write() returns short counts / EAGAIN and it must resume via EPOLLOUT.
    # It also exceeds MAX_WBUF, exercising backpressure. Send and receive
    # concurrently, otherwise both sides' buffers fill and we deadlock.
    payload = os.urandom(32 * 1024 * 1024)
    with connect() as s:
        s.settimeout(30)
        sender = threading.Thread(target=s.sendall, args=(payload,))
        sender.start()
        got = recv_exact(s, len(payload))
        sender.join()
        assert got == payload, "payload corrupted"


def test_slow_reader_backpressure():
    # Client sends a lot but doesn't read for a while. Server must not block
    # other clients meanwhile.
    hog = connect()
    try:
        hog.setblocking(False)
        blob = b"x" * (8 * 1024 * 1024)
        sent = 0
        deadline = time.time() + 2
        while sent < len(blob) and time.time() < deadline:
            try:
                sent += hog.send(blob[sent:])
            except BlockingIOError:
                time.sleep(0.01)
        # Server is now holding a backed-up output buffer for `hog`.
        with connect() as other:
            other.sendall(b"still-alive")
            assert recv_exact(other, 11) == b"still-alive"
    finally:
        hog.close()


def test_abrupt_disconnects():
    # RST instead of FIN: SO_LINGER with timeout 0 makes close() reset the
    # connection. Server must clean up without crashing or leaking.
    import struct
    for _ in range(50):
        s = connect()
        s.sendall(b"x" * 100000)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        s.close()
    with connect() as s:  # server still healthy
        s.sendall(b"ok")
        assert recv_exact(s, 2) == b"ok"


def test_half_close_still_gets_reply():
    # Client sends then shuts down its write side (FIN). Server sees EOF.
    # Currently the server closes on EOF; just make sure it doesn't crash.
    s = connect()
    s.sendall(b"bye")
    s.shutdown(socket.SHUT_WR)
    s.close()
    with connect() as s2:
        s2.sendall(b"ok")
        assert recv_exact(s2, 2) == b"ok"


if __name__ == "__main__":
    tests = [(k, v) for k, v in globals().items() if k.startswith("test_")]
    failed = 0
    for name, fn in tests:
        t0 = time.time()
        try:
            fn()
            print(f"PASS {name} ({time.time() - t0:.2f}s)")
        except Exception as e:
            failed += 1
            print(f"FAIL {name}: {e!r}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
