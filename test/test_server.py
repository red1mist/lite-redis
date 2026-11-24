#!/usr/bin/env python3
"""Integration tests for lite-redis, speaking raw RESP over TCP (no client
library, so we control exactly how bytes are split across packets).

    ./dev make run                         # terminal 1
    ./dev python3 test/test_server.py      # terminal 2

Run a subset by name:  ./dev python3 test/test_server.py pipelin partial

Differential mode: TARGET=redis runs the same tests against a REAL
redis-server, which checks that our expected replies are what Redis itself
returns. Tests marked @lite_only check behaviour that is specific to
lite-redis (e.g. our COMMAND stub) and are skipped there.
"""
import os
import socket
import struct
import sys
import threading
import time

HOST, PORT = "127.0.0.1", int(os.environ.get("PORT", 6379))
TARGET = os.environ.get("TARGET", "lite")
# Unique per run, so tests never collide with keys from earlier runs (we
# have no FLUSHALL, and a real redis-server may hold other data).
K = f"t{os.getpid()}:"

LITE_ONLY = set()
TIMING = set()
# SLOW_SERVER=1 (used by test/valgrind.sh): the server runs 20-50x slower
# under Valgrind, so tests that assert latencies/deadlines are skipped there.
# Everything else (correctness, error paths, disconnects) still runs.
SLOW_SERVER = os.environ.get("SLOW_SERVER") == "1"


def lite_only(fn):
    LITE_ONLY.add(fn.__name__)
    return fn


def timing_sensitive(fn):
    TIMING.add(fn.__name__)
    return fn


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


@lite_only  # real Redis returns its full command docs here
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
    for bad in [b"*abc\r\n", b"*1\r\n$-5\r\n", b"*1\r\n$999999999999\r\n"]:
        with Client() as c:
            c.send_raw(bad)
            check_err(c.read_reply(), "ERR Protocol error")
            assert c.is_closed(), f"not closed after {bad!r}"


@lite_only
def test_protocol_error_missing_crlf_after_bulk():
    # Stricter than real Redis: Redis just skips the 2 bytes after a bulk
    # payload without checking they are CRLF, so "abcXY" would run "abc".
    # We treat it as a corrupt stream.
    with Client() as c:
        c.send_raw(b"*1\r\n$3\r\nabcXY")
        check_err(c.read_reply(), "ERR Protocol error: expected CRLF")
        assert c.is_closed()


@lite_only
def test_protocol_error_too_many_args():
    # Stricter than real Redis 7, which accepts up to INT_MAX array elements
    # and just waits for them. We cap a command at 1M arguments.
    with Client() as c:
        c.send_raw(b"*99999999\r\n")
        check_err(c.read_reply(), "ERR Protocol error: invalid multibulk length")
        assert c.is_closed()


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
# Key-value store: SET / GET / DEL / EXISTS / DBSIZE (Stage 4)
# --------------------------------------------------------------------------

def test_set_get_basic():
    with Client() as c:
        check(c.cmd("SET", K + "a", "1"), "OK")
        check(c.cmd("GET", K + "a"), b"1")


def test_get_missing_is_null():
    with Client() as c:
        check(c.cmd("GET", K + "never-set"), None)


def test_set_overwrite():
    with Client() as c:
        c.cmd("SET", K + "ow", "first")
        check(c.cmd("SET", K + "ow", "second"), "OK")
        check(c.cmd("GET", K + "ow"), b"second")


def test_overwrite_big_then_small():
    # Exercises the shrink-on-overwrite path in Store::set.
    with Client(timeout=30) as c:
        check(c.cmd("SET", K + "shrink", b"B" * (4 * 1024 * 1024)), "OK")
        check(c.cmd("SET", K + "shrink", "tiny"), "OK")
        check(c.cmd("GET", K + "shrink"), b"tiny")
        check(c.cmd("SET", K + "shrink", b"C" * 100000), "OK")
        check(c.cmd("GET", K + "shrink"), b"C" * 100000)
        c.cmd("DEL", K + "shrink")


def test_binary_keys_and_values():
    key = K.encode() + b"bin\r\n\x00key"
    val = b"\x00\xff\r\n$3\r\nfoo\r\n" * 10
    with Client() as c:
        check(c.cmd("SET", key, val), "OK")
        check(c.cmd("GET", key), val)
        check(c.cmd("EXISTS", key), 1)
        check(c.cmd("DEL", key), 1)


def test_empty_value_vs_missing():
    with Client() as c:
        check(c.cmd("SET", K + "empty", ""), "OK")
        check(c.cmd("GET", K + "empty"), b"")    # $0: exists, empty
        check(c.cmd("GET", K + "nope"), None)   # $-1: missing
        check(c.cmd("EXISTS", K + "empty"), 1)


def test_empty_key():
    # "" is a valid key in Redis (it can't carry our per-run prefix, so this
    # test cleans up after itself).
    with Client() as c:
        check(c.cmd("SET", "", "empty-key-value"), "OK")
        check(c.cmd("GET", ""), b"empty-key-value")
        check(c.cmd("DEL", ""), 1)
        check(c.cmd("GET", ""), None)


def test_del():
    with Client() as c:
        c.cmd("SET", K + "d1", "x")
        c.cmd("SET", K + "d2", "x")
        check(c.cmd("DEL", K + "d1", K + "d2", K + "missing"), 2)
        check(c.cmd("GET", K + "d1"), None)
        check(c.cmd("DEL", K + "d1"), 0)


def test_del_duplicate_key_counts_once():
    with Client() as c:
        c.cmd("SET", K + "dup", "x")
        check(c.cmd("DEL", K + "dup", K + "dup"), 1)


def test_exists_counts_duplicates():
    with Client() as c:
        c.cmd("SET", K + "ex", "x")
        check(c.cmd("EXISTS", K + "ex", K + "ex", K + "missing"), 2)


def test_set_nx_xx():
    with Client() as c:
        k = K + "nxxx"
        check(c.cmd("SET", k, "v1", "XX"), None)   # missing + XX -> not set
        check(c.cmd("GET", k), None)
        check(c.cmd("SET", k, "v1", "NX"), "OK")   # missing + NX -> set
        check(c.cmd("SET", k, "v2", "nx"), None)   # exists + NX -> not set
        check(c.cmd("GET", k), b"v1")
        check(c.cmd("SET", k, "v3", "xX"), "OK")   # exists + XX -> set
        check(c.cmd("GET", k), b"v3")


def test_set_syntax_errors():
    with Client() as c:
        check_err(c.cmd("SET", K + "s", "v", "NX", "XX"), "ERR syntax error")
        check_err(c.cmd("SET", K + "s", "v", "BOGUS"), "ERR syntax error")
        check(c.cmd("GET", K + "s"), None)  # failed SET must not write


def test_arity_errors():
    with Client() as c:
        check_err(c.cmd("GET"), "ERR wrong number of arguments for 'get' command")
        check_err(c.cmd("GET", "a", "b"), "ERR wrong number of arguments for 'get' command")
        check_err(c.cmd("SET", "a"), "ERR wrong number of arguments for 'set' command")
        check_err(c.cmd("DEL"), "ERR wrong number of arguments for 'del' command")
        check_err(c.cmd("EXISTS"), "ERR wrong number of arguments for 'exists' command")


def test_command_names_case_insensitive():
    with Client() as c:
        check(c.cmd("set", K + "case", "v"), "OK")
        check(c.cmd("gEt", K + "case"), b"v")
        check(c.cmd("Del", K + "case"), 1)


def test_large_value_set_get():
    val = os.urandom(16 * 1024 * 1024)
    with Client(timeout=30) as c:
        t = threading.Thread(target=c.send_raw, args=(encode("SET", K + "big", val),))
        t.start()
        check(c.read_reply(), "OK")
        t.join()
        assert c.cmd("GET", K + "big") == val, "16 MB value corrupted"
        check(c.cmd("DEL", K + "big"), 1)


def test_keys_visible_across_clients():
    with Client() as a, Client() as b:
        check(a.cmd("SET", K + "shared", "from-a"), "OK")
        check(b.cmd("GET", K + "shared"), b"from-a")
        check(b.cmd("DEL", K + "shared"), 1)
        check(a.cmd("GET", K + "shared"), None)


def test_data_outlives_connection():
    with Client() as a:
        a.cmd("SET", K + "persist", "still-here")
    with Client() as b:
        check(b.cmd("GET", K + "persist"), b"still-here")


def test_many_keys_pipelined_and_dbsize():
    n = 50000
    keys = [f"{K}many:{i}" for i in range(n)]
    with Client(timeout=30) as c:
        before = c.cmd("DBSIZE")
        c.send_raw(b"".join(encode("SET", k, f"v{i}") for i, k in enumerate(keys)))
        for _ in range(n):
            check(c.read_reply(), "OK")
        check(c.cmd("DBSIZE"), before + n)
        c.send_raw(b"".join(encode("GET", k) for k in keys))
        for i in range(n):
            check(c.read_reply(), f"v{i}".encode())
        check(c.cmd("DEL", *keys), n)
        check(c.cmd("DBSIZE"), before)


def test_reply_order_preserved_in_pipeline():
    # Mixed commands in one packet: replies must come back in request order.
    with Client() as c:
        k = K + "order"
        c.send_raw(encode("SET", k, "1") + encode("GET", k) + encode("DEL", k)
                   + encode("GET", k) + encode("EXISTS", k) + encode("PING"))
        for want in ["OK", b"1", 1, None, 0, "PONG"]:
            check(c.read_reply(), want)


# --------------------------------------------------------------------------
# Regressions from the Stage 4 multi-agent review. Expected replies were
# checked against real Redis 7.0.15 (these run in the TARGET=redis pass too,
# except @lite_only ones).
# --------------------------------------------------------------------------

def raw_exchange(data, nreplies):
    with Client() as c:
        c.send_raw(data)
        return [c.read_reply() for _ in range(nreplies)]


def test_inline_double_quotes_and_escapes():
    k = K + "q"
    check(raw_exchange(f'SET {k} "hello world"\r\nGET {k}\r\n'.encode(), 2), ["OK", b"hello world"])
    check(raw_exchange(b'ECHO "a\\x41\\n\\t\\"q\\""\r\n', 1), [b'aA\n\t"q"'])
    check(raw_exchange(b'ECHO ""\r\n', 1), [b""])


def test_inline_single_quotes():
    k = K + "sq"
    check(raw_exchange(f"SET {k} 'a b'\r\nGET {k}\r\n".encode(), 2), ["OK", b"a b"])
    check(raw_exchange(b"ECHO 'it\\'s'\r\n", 1), [b"it's"])


def test_inline_quote_mid_token():
    # sdssplitargs quirk: a quote can open in the middle of a token.
    check(raw_exchange(b'ECHO ab"c d"\r\n', 1), [b"abc d"])


def test_inline_unbalanced_quotes_is_protocol_error():
    for bad in [b'ECHO "abc\r\n', b"ECHO 'abc\r\n", b'ECHO "a"b\r\n']:
        with Client() as c:
            c.send_raw(bad)
            check_err(c.read_reply(), "ERR Protocol error: unbalanced quotes in request")
            assert c.is_closed(), f"not closed after {bad!r}"


def test_inline_carriage_returns_are_whitespace():
    check(raw_exchange(b"PING\r\r\n", 1), ["PONG"])
    check(raw_exchange(b"\r\r\nPING\r\n", 1), ["PONG"])
    check_err(raw_exchange(b"ECHO a\rb\r\n", 1)[0],
              "ERR wrong number of arguments for 'echo' command")


def test_unknown_command_arg_budget():
    with Client() as c:
        got = c.cmd("FOO", *[str(i) for i in range(20)])
        want = "ERR unknown command 'FOO', with args beginning with: " + \
               "".join(f"'{i}' " for i in range(20))
        check(str(got), want)
        got = c.cmd("FOO", "a" * 100, "b" * 100, "c" * 100)
        check(str(got), "ERR unknown command 'FOO', with args beginning with: '"
              + "a" * 100 + "' '" + "b" * 25 + "' ")


def test_quit_ignores_arguments():
    with Client() as c:
        c.send_raw(encode("QUIT", "x") + encode("PING"))
        check(c.read_reply(), "OK")
        assert c.is_closed(), "QUIT x must close and not run the pipelined PING"


def test_command_unknown_subcommand():
    with Client() as c:
        check_err(c.cmd("COMMAND", "FOOBAR"),
                  "ERR unknown subcommand 'FOOBAR'. Try COMMAND HELP.")


@lite_only  # real Redis has ~240 commands
def test_command_count_and_info_types():
    with Client() as c:
        n = c.cmd("COMMAND", "COUNT")
        assert isinstance(n, int) and n >= 9, n
        info = c.cmd("COMMAND", "INFO", "get", "nosuchcmd", "del")
        assert len(info) == 3 and info[1] is None, info
        check(info[0][:6], [b"get", 2, [], 1, 1, 1])
        check(info[2][:6], [b"del", -2, [], 1, -1, 1])


def test_config_get_for_redis_benchmark():
    # run_tests.sh starts the real redis with --save "" --appendonly no, so
    # both servers must answer identically.
    with Client() as c:
        check(c.cmd("CONFIG", "GET", "save"), [b"save", b""])
        check(c.cmd("CONFIG", "GET", "appendonly"), [b"appendonly", b"no"])
        check(c.cmd("CONFIG", "GET", "no-such-param"), [])
        check_err(c.cmd("CONFIG", "GET"), "ERR wrong number of arguments for 'config|get' command")


def test_protocol_error_messages_exact():
    for data, msg in [
        (b"*" + b"1" * 70000, "ERR Protocol error: too big mbulk count string"),
        (b"*1\r\n$" + b"1" * 70000, "ERR Protocol error: too big bulk count string"),
        (b"*01\r\n", "ERR Protocol error: invalid multibulk length"),
        (b"*1\r\n$04\r\nPING\r\n", "ERR Protocol error: invalid bulk length"),
        (b"*-0\r\n", "ERR Protocol error: invalid multibulk length"),
    ]:
        with Client() as c:
            c.send_raw(data)
            check(str(c.read_reply()), msg)
            assert c.is_closed()


def test_protocol_error_reply_never_contains_raw_crlf():
    # The offending byte is echoed in the error; a raw \r there would end
    # the error line early and forge a second reply.
    with Client() as c:
        c.send_raw(b"*1\r\n\r\nPING\r\n")
        check(str(c.read_reply()), "ERR Protocol error: expected '$', got ' '")
        assert c.is_closed()


@lite_only  # real Redis waits for a '\r' before validating, so it just hangs
def test_protocol_error_bare_lf_is_sanitized():
    with Client() as c:
        c.send_raw(b"*1\r\n\n")
        check(str(c.read_reply()), "ERR Protocol error: expected '$', got ' '")
        assert c.is_closed()


@timing_sensitive
def test_many_args_trickled_is_not_quadratic():
    # 200k one-byte args arriving in ~1 KB pieces = ~1400 reads. A parser
    # that restarts from byte 0 on every read does ~1400 x 200k arg parses
    # (seconds of CPU, and every other client stalls meanwhile). Resumable
    # parsing touches each byte once.
    n = 200000
    data = encode("EXISTS", *(["x"] * n))
    with Client(timeout=30) as c, Client() as probe:
        t0 = time.time()
        worst_ping = 0.0
        for i in range(0, len(data), 1024):
            c.send_raw(data[i:i + 1024])
            if i % (64 * 1024) == 0:
                p0 = time.time()
                check(probe.cmd("PING"), "PONG")
                worst_ping = max(worst_ping, time.time() - p0)
        check(c.read_reply(), 0)
        elapsed = time.time() - t0
    assert elapsed < 10, f"took {elapsed:.1f}s"
    assert worst_ping < 0.25, f"other clients stalled: worst PING {worst_ping * 1000:.0f} ms"


def rss_kb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    raise RuntimeError("no VmRSS")


@lite_only  # needs the server's pid (SERVER_PID, set by run_tests.sh)
@timing_sensitive
def test_output_backpressure_bounds_memory():
    # ~3000 "GET bigkey" (1 MB value) fit in ~70 KB of request bytes, i.e.
    # about one read(). Without pausing execution at MAX_WBUF the server
    # would build ~3 GB of replies in one go. With it, it only ever holds
    # about MAX_WBUF plus one reply while the client isn't reading.
    pid = os.environ.get("SERVER_PID")
    if not pid:
        print("    (SERVER_PID not set; skipping memory assertion)")
    key = K + "bp-big"
    val = b"v" * (1024 * 1024)
    n = 3000
    with Client(timeout=60) as c:
        check(c.cmd("SET", key, val), "OK")
        before = rss_kb(pid) if pid else 0
        c.send_raw(b"".join(encode("GET", key) for _ in range(n)))
        time.sleep(1.0)  # let the server run as far as it will
        if pid:
            grown_mb = (rss_kb(pid) - before) / 1024
            assert grown_mb < 200, f"server RSS grew {grown_mb:.0f} MB while client wasn't reading"
        for _ in range(n):  # and every reply still arrives, in order
            assert c.read_reply() == val
        check(c.cmd("DEL", key), 1)


@lite_only  # real Redis frees the client on EOF and DROPS queued replies
def test_backpressure_then_half_close_delivers_everything():
    # Pause (output over MAX_WBUF) + client EOF: the paused commands must
    # still run and all replies arrive before the server closes.
    key = K + "bp-eof"
    with Client() as setup:
        setup.cmd("SET", key, b"w" * 200000)
    n = 200
    with Client(timeout=30) as c:
        c.send_raw(b"".join(encode("GET", key) for _ in range(n)) + encode("ECHO", "last"))
        c.sock.shutdown(socket.SHUT_WR)
        for _ in range(n):
            assert c.read_reply() == b"w" * 200000
        check(c.read_reply(), b"last")
        assert c.is_closed()
    with Client() as cleanup:
        cleanup.cmd("DEL", key)


# --------------------------------------------------------------------------
# Regressions from the second (fuzzing) review of the Stage 4 fixes
# --------------------------------------------------------------------------

def slow_read_all(sock, expected_len, slow_secs=2.0):
    """Read like a slow client (small chunks, pauses) then fast, until EOF."""
    got, err = bytearray(), None
    t_end = time.time() + slow_secs
    try:
        while True:
            if time.time() < t_end:
                chunk = sock.recv(4096)
                time.sleep(0.01)
            else:
                chunk = sock.recv(1 << 16)
            if not chunk:
                break
            got += chunk
    except OSError as e:
        err = e
    return bytes(got), err


def _send_ignoring_errors(sock, data):
    try:
        sock.sendall(data)
    except OSError:
        pass  # the server may close (correctly) before we finish sending junk


def _closing_with_trailing_input(terminator, final_reply):
    # Slow reader (tiny receive buffer) pipelines 50 big GETs, then QUIT or a
    # malformed command, then 200 KB more. All replies must arrive: closing
    # with unread input would make Linux send RST and drop queued replies.
    key = K + "rst-big"
    val = b"r" * 100000
    with Client() as c:
        c.cmd("SET", key, val)
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)
    s.settimeout(30)
    s.connect((HOST, PORT))
    req = encode("GET", key) * 50 + terminator + b"PING\r\n" * 33000
    t = threading.Thread(target=_send_ignoring_errors, args=(s, req))
    t.start()
    want = (b"$100000\r\n" + val + b"\r\n") * 50 + final_reply
    got, err = slow_read_all(s, len(want))
    t.join()
    s.close()
    with Client() as c:
        c.cmd("DEL", key)
    assert err is None, f"connection error {err!r} after {len(got)}/{len(want)} bytes"
    assert got == want, f"got {len(got)} of {len(want)} bytes"


def test_quit_with_trailing_input_delivers_all_replies():
    _closing_with_trailing_input(encode("QUIT"), b"+OK\r\n")


def test_protocol_error_with_trailing_input_delivers_all_replies():
    _closing_with_trailing_input(b"*1\r\n$x\r\n",
                                 b"-ERR Protocol error: invalid bulk length\r\n")


@timing_sensitive
def test_write_whole_pipeline_then_read_does_not_deadlock():
    # Like redis-py's pipeline.execute(): send EVERYTHING, then read. If the
    # server stopped reading while backpressured, both sides would block
    # forever (its send window full, our receive window full).
    key = K + "wv"
    n = 800000
    with Client() as c:
        c.cmd("SET", key, "0123456789")
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
    s.settimeout(20)
    s.connect((HOST, PORT))
    c = Client.__new__(Client)
    c.sock, c.buf = s, b""
    one = encode("GET", key)
    try:
        s.sendall(one * n)  # blocks forever on the deadlock bug -> timeout
    except socket.timeout:
        raise AssertionError("sendall stuck: server stopped reading (deadlock)")
    for _ in range(n):
        assert c.read_reply() == b"0123456789"
    s.close()


def test_header_error_is_split_invariant():
    # Same bytes, different TCP splits, must give the same reply.
    stream = b"*1\r\n$4" + b"x" * 40 + b"\r\nPING\r\n"
    for cut in [len(stream), 20, 44]:
        with Client() as c:
            c.send_raw(stream[:cut])
            time.sleep(0.05)
            if cut < len(stream):
                try:
                    c.send_raw(stream[cut:])
                except OSError:
                    pass
            check(str(c.read_reply()), "ERR Protocol error: invalid bulk length")


def test_unknown_command_text_stops_at_nul():
    with Client() as c:
        check(str(c.cmd(b"ab\x00cd", "x")),
              "ERR unknown command 'ab', with args beginning with: 'x' ")
        check(str(c.cmd("FOO", b"a\x00b", "z")),
              "ERR unknown command 'FOO', with args beginning with: 'a' 'z' ")


@lite_only  # Redis stalls on this input instead of replying
def test_protocol_error_never_contains_nul():
    with Client() as c:
        c.send_raw(b"*1\r\n\x004\r\nPING\r\n")
        check(str(c.read_reply()), "ERR Protocol error: expected '$', got ' '")


# --------------------------------------------------------------------------
# Key expiry: EXPIRE / PEXPIRE / TTL / PTTL / PERSIST / SET EX|PX (Stage 5)
# --------------------------------------------------------------------------

def test_expire_and_ttl_basics():
    k = K + "e1"
    with Client() as c:
        check(c.cmd("TTL", k), -2)            # missing
        check(c.cmd("EXPIRE", k, "100"), 0)   # can't expire a missing key
        c.cmd("SET", k, "v")
        check(c.cmd("TTL", k), -1)            # exists, no TTL
        check(c.cmd("EXPIRE", k, "100"), 1)
        check(c.cmd("TTL", k), 100)
        pttl = c.cmd("PTTL", k)
        assert 99000 < pttl <= 100000, pttl
        check(c.cmd("PEXPIRE", k, "1600"), 1)
        check(c.cmd("TTL", k), 2)             # 1.6 s rounds to 2, like Redis
        c.cmd("DEL", k)


def test_key_expires_lazily_and_is_invisible():
    k = K + "e2"
    with Client() as c:
        check(c.cmd("SET", k, "v", "PX", "100"), "OK")
        check(c.cmd("GET", k), b"v")
        time.sleep(0.15)
        check(c.cmd("GET", k), None)
        check(c.cmd("EXISTS", k), 0)
        check(c.cmd("TTL", k), -2)
        check(c.cmd("DEL", k), 0)             # already expired: nothing to delete
        check(c.cmd("EXPIRE", k, "10"), 0)
        check(c.cmd("PERSIST", k), 0)


def test_active_expiry_removes_untouched_keys():
    # Nobody reads these keys again; only ACTIVE expiry can reclaim them.
    # DBSIZE doesn't trigger lazy deletion, so it shows what's really stored.
    n = 500
    with Client() as c:
        before = c.cmd("DBSIZE")
        c.send_raw(b"".join(encode("SET", f"{K}act:{i}", "v", "PX", "50") for i in range(n)))
        for _ in range(n):
            check(c.read_reply(), "OK")
        check(c.cmd("DBSIZE"), before + n)
        deadline = time.time() + 2.0
        while c.cmd("DBSIZE") != before and time.time() < deadline:
            time.sleep(0.05)
        check(c.cmd("DBSIZE"), before)


@lite_only  # real Redis expires actively on a ~100 ms sampling cycle
@timing_sensitive
def test_active_expiry_is_timely():
    # The heap tells the event loop exactly when the next key is due, so an
    # untouched key disappears within a few ms of its deadline.
    k = K + "timely"
    with Client() as c:
        before = c.cmd("DBSIZE")
        t0 = time.time()
        c.cmd("SET", k, "v", "PX", "100")
        while c.cmd("DBSIZE") != before:
            time.sleep(0.002)
            assert time.time() - t0 < 1.0, "never expired"
        took = (time.time() - t0) * 1000
    assert 95 <= took < 140, f"expired after {took:.0f} ms (deadline 100 ms)"


def test_set_overwrite_clears_ttl_unless_keepttl():
    k = K + "e3"
    with Client() as c:
        c.cmd("SET", k, "a", "EX", "100")
        c.cmd("SET", k, "b")
        check(c.cmd("TTL", k), -1)
        c.cmd("EXPIRE", k, "100")
        check(c.cmd("SET", k, "c", "KEEPTTL"), "OK")
        check(c.cmd("TTL", k), 100)
        check(c.cmd("GET", k), b"c")
        c.cmd("DEL", k)


def test_set_expiry_options():
    k = K + "e4"
    with Client() as c:
        check(c.cmd("SET", k, "v", "EX", "10"), "OK")
        check(c.cmd("TTL", k), 10)
        check(c.cmd("SET", k, "v", "px", "1500"), "OK")
        assert 1400 < c.cmd("PTTL", k) <= 1500
        check(c.cmd("SET", k, "v", "EX", "10", "EX", "20"), "OK")  # last wins
        check(c.cmd("TTL", k), 20)
        check(c.cmd("SET", k, "v", "NX", "EX", "5"), None)           # exists: no-op
        check(c.cmd("TTL", k), 20)
        c.cmd("DEL", k)


def test_set_expiry_errors():
    k = K + "e5"
    with Client() as c:
        for args, err in [
            (["EX", "0"], "ERR invalid expire time in 'set' command"),
            (["PX", "-5"], "ERR invalid expire time in 'set' command"),
            (["EX", "9223372036854775807"], "ERR invalid expire time in 'set' command"),
            (["EX", "abc"], "ERR value is not an integer or out of range"),
            (["EX", "01"], "ERR value is not an integer or out of range"),
            (["EX"], "ERR syntax error"),
            (["EX", "10", "PX", "100"], "ERR syntax error"),
            (["KEEPTTL", "EX", "10"], "ERR syntax error"),
            (["EX", "10", "KEEPTTL"], "ERR syntax error"),
        ]:
            check_err(c.cmd("SET", k, "v", *args), err)
        check(c.cmd("EXISTS", k), 0)  # failed SETs never write


def test_set_get_option():
    k = K + "e6"
    with Client() as c:
        check(c.cmd("SET", k, "one", "GET"), None)        # no old value
        check(c.cmd("SET", k, "two", "GET"), b"one")
        check(c.cmd("GET", k), b"two")
        check(c.cmd("SET", k, "three", "NX", "GET"), b"two")  # NX fails, still returns old
        check(c.cmd("GET", k), b"two")
        check(c.cmd("SET", K + "e6-missing", "x", "XX", "GET"), None)
        check(c.cmd("EXISTS", K + "e6-missing"), 0)
        c.cmd("DEL", k)


def test_expire_non_positive_deletes_key():
    with Client() as c:
        for cmd, ttl in [("EXPIRE", "-1"), ("EXPIRE", "0"), ("PEXPIRE", "-100"), ("PEXPIRE", "0")]:
            k = f"{K}neg:{cmd}:{ttl}"
            c.cmd("SET", k, "v")
            check(c.cmd(cmd, k, ttl), 1)
            check(c.cmd("EXISTS", k), 0)


def test_expire_nx_xx_gt_lt():
    k = K + "e7"
    with Client() as c:
        c.cmd("SET", k, "v")
        check(c.cmd("EXPIRE", k, "100", "XX"), 0)   # no TTL yet
        check(c.cmd("EXPIRE", k, "100", "GT"), 0)   # no TTL = infinite: never greater
        check(c.cmd("EXPIRE", k, "100", "NX"), 1)
        check(c.cmd("EXPIRE", k, "200", "NX"), 0)   # already has one
        check(c.cmd("EXPIRE", k, "50", "GT"), 0)
        check(c.cmd("EXPIRE", k, "300", "gt"), 1)
        check(c.cmd("TTL", k), 300)
        check(c.cmd("EXPIRE", k, "400", "LT"), 0)
        check(c.cmd("EXPIRE", k, "30", "LT"), 1)
        check(c.cmd("EXPIRE", k, "60", "XX"), 1)
        check(c.cmd("TTL", k), 60)
        c.cmd("PERSIST", k)
        check(c.cmd("EXPIRE", k, "10", "LT"), 1)    # infinite -> anything is less
        check(c.cmd("TTL", k), 10)
        c.cmd("DEL", k)


def test_expire_errors():
    k = K + "e8"
    with Client() as c:
        c.cmd("SET", k, "v")
        for args, err in [
            (["10", "NX", "XX"], "ERR NX and XX, GT or LT options at the same time are not compatible"),
            (["10", "NX", "GT"], "ERR NX and XX, GT or LT options at the same time are not compatible"),
            (["10", "GT", "LT"], "ERR GT and LT options at the same time are not compatible"),
            (["10", "FOO"], "ERR Unsupported option FOO"),
            (["abc", "FOO"], "ERR Unsupported option FOO"),  # options are checked first
            (["abc"], "ERR value is not an integer or out of range"),
            (["1.5"], "ERR value is not an integer or out of range"),
            (["9223372036854775807"], "ERR invalid expire time in 'expire' command"),
        ]:
            check_err(c.cmd("EXPIRE", k, *args), err)
        check_err(c.cmd("PEXPIRE", k, "9223372036854775807"),
                  "ERR invalid expire time in 'pexpire' command")
        check_err(c.cmd("TTL"), "ERR wrong number of arguments for 'ttl' command")
        check_err(c.cmd("EXPIRE", k), "ERR wrong number of arguments for 'expire' command")
        check(c.cmd("TTL", k), -1)   # nothing above changed the key
        c.cmd("DEL", k)


def test_persist():
    k = K + "e9"
    with Client() as c:
        check(c.cmd("PERSIST", k), 0)        # missing
        c.cmd("SET", k, "v")
        check(c.cmd("PERSIST", k), 0)        # no TTL
        c.cmd("EXPIRE", k, "100")
        check(c.cmd("PERSIST", k), 1)
        check(c.cmd("TTL", k), -1)
        c.cmd("DEL", k)


def test_expired_key_is_replaced_fresh():
    # SET ... KEEPTTL on a key that has already expired: the old key (and its
    # TTL) is gone, so the new one has no TTL.
    k = K + "e10"
    with Client() as c:
        c.cmd("SET", k, "old", "PX", "50")
        time.sleep(0.1)
        check(c.cmd("SET", k, "new", "KEEPTTL"), "OK")
        check(c.cmd("TTL", k), -1)
        check(c.cmd("GET", k), b"new")
        c.cmd("DEL", k)


@lite_only  # measures our event loop's behaviour during a mass expiry
@timing_sensitive
def test_mass_expiry_does_not_stall_clients():
    # 200k keys all expiring in the same millisecond: the loop deletes them in
    # bounded batches, so other clients keep getting answers meanwhile.
    n = 200000
    with Client(timeout=60) as c, Client() as probe:
        before = c.cmd("DBSIZE")
        c.send_raw(b"".join(encode("SET", f"{K}mass:{i}", "v", "PX", "300") for i in range(n)))
        for _ in range(n):
            c.read_reply()
        worst = 0.0
        t_end = time.time() + 1.5
        while time.time() < t_end:
            t0 = time.time()
            check(probe.cmd("PING"), "PONG")
            worst = max(worst, time.time() - t0)
            time.sleep(0.005)
        check(c.cmd("DBSIZE"), before)
    assert worst < 0.1, f"a PING took {worst * 1000:.0f} ms during the mass expiry"


# --------------------------------------------------------------------------
# Regressions from the Stage 5 review
# --------------------------------------------------------------------------

def test_option_keywords_compare_like_c_strings():
    # Redis matches options as C strings, which stop at NUL: "NX\0junk" is NX.
    k = K + "nulopt"
    with Client() as c:
        check(c.cmd("SET", k, "a", b"NX\x00junk"), "OK")
        check(c.cmd("SET", k, "b", b"GET\x00x"), b"a")
        check(c.cmd("SET", k, "c", b"EX\x00", "100"), "OK")
        check(c.cmd("TTL", k), 100)
        check(c.cmd("PERSIST", k), 1)
        check(c.cmd("EXPIRE", k, "300", b"NX\x00"), 1)
        check_err(c.cmd("EXPIRE", k, "300", b"F\x00OO"), "ERR Unsupported option F")
        assert str(c.cmd("EXPIRE", k, "300", b"F\x00OO")) == "ERR Unsupported option F"
        c.cmd("DEL", k)


def test_huge_ttl_overflow_matches_redis():
    # Redis adds the TTL to Unix-epoch ms, so these overflow there even though
    # they'd fit on top of our (smaller) monotonic clock. Errors must match.
    k = K + "huge"
    with Client() as c:
        c.cmd("SET", k, "v")
        check_err(c.cmd("SET", k, "v", "PX", "9223372036000000000"),
                  "ERR invalid expire time in 'set' command")
        check_err(c.cmd("SET", k, "v", "EX", "9223372036000000"),
                  "ERR invalid expire time in 'set' command")
        check_err(c.cmd("PEXPIRE", k, "9223372036000000000"),
                  "ERR invalid expire time in 'pexpire' command")
        check_err(c.cmd("EXPIRE", k, "9223372036000000"),
                  "ERR invalid expire time in 'expire' command")
        check(c.cmd("TTL", k), -1)
        c.cmd("DEL", k)


# --------------------------------------------------------------------------

if __name__ == "__main__":
    filters = sys.argv[1:]
    tests = [(k, v) for k, v in list(globals().items())
             if k.startswith("test_") and (not filters or any(f in k for f in filters))]
    failed = skipped = 0
    for name, fn in tests:
        if TARGET != "lite" and name in LITE_ONLY:
            skipped += 1
            print(f"SKIP {name} (lite-redis specific)")
            continue
        if SLOW_SERVER and name in TIMING:
            skipped += 1
            print(f"SKIP {name} (timing-sensitive; SLOW_SERVER)")
            continue
        t0 = time.time()
        try:
            fn()
            print(f"PASS {name} ({time.time() - t0:.2f}s)")
        except Exception as e:
            failed += 1
            print(f"FAIL {name}: {e!r}")
    ran = len(tests) - skipped
    print(f"\n[{TARGET}] {ran - failed}/{ran} passed" + (f", {skipped} skipped" if skipped else ""))
    sys.exit(1 if failed else 0)
