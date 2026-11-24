#!/usr/bin/env python3
"""Integration tests for lite-redis, speaking raw RESP over TCP (no client
library, so we control exactly how bytes are split across packets).

    ./dev make run                         # terminal 1
    ./dev python3 test/test_server.py      # terminal 2

Run a subset by name:  ./dev python3 test/test_server.py pipelin partial
"""
import os
import socket
import struct
import sys
import threading
import time

HOST, PORT = "127.0.0.1", int(os.environ.get("PORT", 6379))


# --------------------------------------------------------------------------
# Minimal RESP client
# --------------------------------------------------------------------------

def encode(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Err(Exception):
    """A RESP error reply (-ERR ...)."""


class Client:
    def __init__(self, timeout=5):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def close(self):
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def send_raw(self, data):
        self.sock.sendall(data)

    def _fill(self):
        chunk = self.sock.recv(1 << 16)
        if not chunk:
            raise ConnectionError("server closed connection")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def read_reply(self):
        """Returns str for +simple, bytes for $bulk, int, list, None for null.
        Error replies are returned as Err instances (not raised)."""
        line = self._line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return Err(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self._fill()
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.read_reply() for _ in range(n)]
        raise ValueError(f"bad reply type: {line!r}")

    def cmd(self, *args):
        self.send_raw(encode(*args))
        return self.read_reply()

    def is_closed(self, wait=1.0):
        """True if the server closed the connection (EOF) within `wait`."""
        self.sock.settimeout(wait)
        try:
            return self.sock.recv(1) == b""
        except socket.timeout:
            return False
        except ConnectionResetError:
            return True


def check(got, want):
    assert got == want, f"expected {want!r}, got {got!r}"


def check_err(got, prefix):
    assert isinstance(got, Err) and str(got).startswith(prefix), \
        f"expected error starting {prefix!r}, got {got!r}"


# --------------------------------------------------------------------------
# Basic commands
# --------------------------------------------------------------------------

def test_ping():
    with Client() as c:
        check(c.cmd("PING"), "PONG")
        check(c.cmd("PING", "hi"), b"hi")
        check_err(c.cmd("PING", "a", "b"), "ERR wrong number of arguments")


def test_echo():
    with Client() as c:
        check(c.cmd("ECHO", "hello world"), b"hello world")
        check(c.cmd("ECHO", ""), b"")
        check_err(c.cmd("ECHO"), "ERR wrong number of arguments for 'echo'")


def test_case_insensitive():
    with Client() as c:
        check(c.cmd("ping"), "PONG")
        check(c.cmd("PiNg"), "PONG")


def test_binary_safe():
    # Bulk strings are length-prefixed: CRLF and NUL inside a value are data.
    payload = b"a\r\nb\x00c\r\n*3\r\n$3\r\n"
    with Client() as c:
        check(c.cmd("ECHO", payload), payload)


def test_unknown_command():
    with Client() as c:
        check_err(c.cmd("NOPE", "x", "y"),
                  "ERR unknown command 'NOPE', with args beginning with: 'x' 'y'")
        check(c.cmd("PING"), "PONG")  # connection still usable


def test_command_docs_for_redis_cli():
    with Client() as c:
        check(c.cmd("COMMAND", "DOCS"), [])


def test_quit():
    with Client() as c:
        check(c.cmd("QUIT"), "OK")
        assert c.is_closed(), "server should close after QUIT"


def test_inline_commands():
    # Plain-text commands, as typed into telnet / nc.
    with Client() as c:
        c.send_raw(b"PING\r\n")
        check(c.read_reply(), "PONG")
        c.send_raw(b"ECHO   spaced\n")  # bare \n and extra spaces are fine
        check(c.read_reply(), b"spaced")
        c.send_raw(b"\r\n\r\nPING\r\n")  # blank lines are skipped
        check(c.read_reply(), "PONG")


# --------------------------------------------------------------------------
# TCP framing: partial reads and pipelining
# --------------------------------------------------------------------------

def test_byte_by_byte():
    # Worst-case fragmentation: every byte arrives in its own packet. The
    # server must buffer and only execute once the command is complete.
    with Client() as c:
        for b in encode("ECHO", "fragmented"):
            c.send_raw(bytes([b]))
            time.sleep(0.001)
        check(c.read_reply(), b"fragmented")


def test_split_inside_length_header():
    with Client() as c:
        msg = encode("ECHO", "x" * 12345)
        c.send_raw(msg[:9])   # "*2\r\n$4\r\nE": split mid-header
        time.sleep(0.05)
        c.send_raw(msg[9:16])
        time.sleep(0.05)
        c.send_raw(msg[16:])
        check(c.read_reply(), b"x" * 12345)


def test_pipelining():
    # 1000 commands in ONE send: one read() on the server sees many commands.
    n = 1000
    with Client() as c:
        c.send_raw(b"".join(encode("ECHO", str(i)) for i in range(n)))
        for i in range(n):
            check(c.read_reply(), str(i).encode())


def test_pipeline_with_partial_tail():
    with Client() as c:
        data = encode("PING") + encode("ECHO", "two") + encode("ECHO", "three")
        cut = len(data) - 4  # last command incomplete
        c.send_raw(data[:cut])
        check(c.read_reply(), "PONG")
        check(c.read_reply(), b"two")
        c.send_raw(data[cut:])
        check(c.read_reply(), b"three")


def test_half_close_gets_reply():
    # `printf 'PING\r\n' | nc host 6379` sends, then shuts down its write
    # side. The server must still deliver the reply before closing.
    with Client() as c:
        c.send_raw(encode("PING") + encode("ECHO", "last"))
        c.sock.shutdown(socket.SHUT_WR)
        check(c.read_reply(), "PONG")
        check(c.read_reply(), b"last")
        assert c.is_closed()


# --------------------------------------------------------------------------
# Protocol errors
# --------------------------------------------------------------------------

def test_protocol_error_bad_bulk_prefix():
    with Client() as c:
        c.send_raw(b"*1\r\n+PING\r\n")
        check_err(c.read_reply(), "ERR Protocol error: expected '$', got '+'")
        assert c.is_closed()


def test_protocol_error_bad_lengths():
    for bad in [b"*abc\r\n", b"*1\r\n$-5\r\n", b"*1\r\n$3\r\nabcXY",
                b"*99999999\r\n", b"*1\r\n$999999999999\r\n"]:
        with Client() as c:
            c.send_raw(bad)
            check_err(c.read_reply(), "ERR Protocol error")
            assert c.is_closed(), f"not closed after {bad!r}"


def test_huge_header_without_newline():
    with Client() as c:
        c.send_raw(b"*" + b"1" * (70 * 1024))
        check_err(c.read_reply(), "ERR Protocol error")


def test_other_clients_unaffected_by_protocol_error():
    with Client() as good, Client() as bad:
        bad.send_raw(b"*1\r\n!oops\r\n")
        check_err(bad.read_reply(), "ERR Protocol error")
        check(good.cmd("PING"), "PONG")


# --------------------------------------------------------------------------
# Event loop under stress (ported from the Stage 2 echo tests)
# --------------------------------------------------------------------------

def test_concurrent_clients_interleaved():
    with Client() as a, Client() as b:
        check(b.cmd("ECHO", "from-b"), b"from-b")
        check(a.cmd("ECHO", "from-a"), b"from-a")


def test_many_clients():
    clients = [Client() for _ in range(200)]
    try:
        for i, c in enumerate(clients):
            c.send_raw(encode("ECHO", f"msg-{i}"))
        for i, c in enumerate(clients):
            check(c.read_reply(), f"msg-{i}".encode())
    finally:
        for c in clients:
            c.close()


def test_large_value_partial_writes():
    # 32 MB reply >> kernel socket buffers: server write() returns short
    # counts / EAGAIN and must resume on EPOLLOUT. Send in a thread, since
    # the server starts replying only after the whole request has arrived.
    payload = os.urandom(32 * 1024 * 1024)
    with Client(timeout=30) as c:
        t = threading.Thread(target=c.send_raw, args=(encode("ECHO", payload),))
        t.start()
        got = c.read_reply()
        t.join()
        assert got == payload, "payload corrupted"


def test_slow_reader_backpressure():
    # Pipeline lots of commands without reading replies: the server's output
    # buffer fills, it stops reading this client, and must keep serving others.
    hog = Client()
    try:
        hog.sock.setblocking(False)
        blob = encode("ECHO", "y" * 1000) * 20000  # ~20 MB of replies
        sent, deadline = 0, time.time() + 2
        while sent < len(blob) and time.time() < deadline:
            try:
                sent += hog.sock.send(blob[sent:sent + (1 << 20)])
            except BlockingIOError:
                time.sleep(0.01)
        with Client() as other:
            check(other.cmd("PING"), "PONG")
    finally:
        hog.close()


def test_abrupt_disconnects():
    # SO_LINGER(0) makes close() send RST instead of FIN.
    for _ in range(50):
        c = Client()
        c.send_raw(encode("ECHO", "z" * 100000) * 3)
        c.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        c.close()
    with Client() as c:
        check(c.cmd("PING"), "PONG")


# --------------------------------------------------------------------------

if __name__ == "__main__":
    filters = sys.argv[1:]
    tests = [(k, v) for k, v in list(globals().items())
             if k.startswith("test_") and (not filters or any(f in k for f in filters))]
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
