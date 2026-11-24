#!/usr/bin/env bash
# Full test run (inside the dev container):   ./dev make test
#
# 1. starts the AddressSanitizer build of the server on a spare port
# 2. runs the raw-RESP integration tests
# 3. runs a smoke test with the real redis-cli (one-shot + interactive)
# 4. checks the server leaked no file descriptors and ASan reported nothing
# 5. maxclients / fd-exhaustion: extra clients are rejected with an error and
#    the server doesn't spin
# 6. graceful shutdown (SIGTERM) so LeakSanitizer checks for leaks at exit
# 7. differential check: runs the same integration tests against a REAL
#    redis-server, proving the expected replies match actual Redis behaviour
set -uo pipefail
cd "$(dirname "$0")/.."

export PORT=${PORT:-6399}
BIN=./lite-redis-server-debug
LOG=$(mktemp)
fail=0

$BIN --port "$PORT" >"$LOG" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null; kill $LPID 2>/dev/null; rm -f "$LOG"' EXIT
LPID=
for _ in $(seq 50); do redis-cli -p "$PORT" PING >/dev/null 2>&1 && break; sleep 0.1; done
FDS_BEFORE=$(ls /proc/$PID/fd | wc -l)

echo "== integration tests (raw RESP) =="
SERVER_PID=$PID python3 test/test_server.py || fail=1

echo
echo "== redis-cli smoke test =="
expect_eq() { # name, expected, actual
    if [ "$2" == "$3" ]; then echo "PASS $1"; else echo "FAIL $1: expected [$2] got [$3]"; fail=1; fi
}
expect_eq "redis-cli PING" "PONG" "$(redis-cli -p "$PORT" PING)"
expect_eq "redis-cli ECHO" "hi there" "$(redis-cli -p "$PORT" ECHO 'hi there')"
# Interactive mode sends COMMAND DOCS on startup. TERM=dumb stops its line
# editor from querying the (fake) terminal for the cursor position.
interactive=$( (sleep 0.3; printf 'PING\r'; sleep 0.3; printf 'quit\r'; sleep 0.3) |
    TERM=dumb timeout 5 script -qec "redis-cli -p $PORT" /dev/null | tr -d '\r' | grep -c '^PONG$')
expect_eq "redis-cli interactive" "1" "$interactive"

echo
echo "== resource checks =="
sleep 0.3
FDS_AFTER=$(ls /proc/$PID/fd | wc -l)
expect_eq "no fd leak" "$FDS_BEFORE" "$FDS_AFTER"
if ! kill -0 $PID 2>/dev/null; then echo "FAIL server died"; fail=1; fi

echo
echo "== graceful shutdown + LeakSanitizer =="
# SIGTERM -> event loop exits, closes every client, returns from main. ASan's
# LeakSanitizer then scans the heap at exit and reports any leaked block.
redis-cli -p "$PORT" SET leakcheck:key value >/dev/null   # leave data + a live client around
exec 3<>/dev/tcp/127.0.0.1/"$PORT"                       # an idle connection at shutdown
kill -TERM $PID
wait $PID; rc=$?
exec 3>&-
expect_eq "exit status after SIGTERM" "0" "$rc"
expect_eq "logged shutdown" "1" "$(grep -c 'shutting down' "$LOG")"
if grep -qE "AddressSanitizer|LeakSanitizer" "$LOG"; then
    echo "FAIL sanitizer report:"; grep -A25 -E "AddressSanitizer|LeakSanitizer" "$LOG" | head -60; fail=1
else
    echo "PASS no AddressSanitizer errors, no leaks at exit"
fi

echo
echo "== maxclients / fd exhaustion =="
# With only 64 fds the server must lower maxclients to 32 (64 - reserved),
# turn extra clients away with an error instead of leaving them hanging,
# and NOT busy-loop (an un-acceptable connection in a level-triggered
# listener used to spin at 100% CPU).
LIM_PORT=$((PORT + 1))
LIM_LOG=$(mktemp)
( ulimit -n 64; exec $BIN --port "$LIM_PORT" ) >"$LIM_LOG" 2>&1 &
LPID=$!
for _ in $(seq 50); do redis-cli -p "$LIM_PORT" PING >/dev/null 2>&1 && break; sleep 0.1; done
python3 - "$LIM_PORT" "$LPID" <<'PY'
import socket, sys, time
port, pid = int(sys.argv[1]), int(sys.argv[2])
def cpu_ticks():
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])          # utime + stime
held, rejected = [], 0
for _ in range(45):
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    s.sendall(b"PING\r\n")
    r = s.recv(100)
    if r.startswith(b"-ERR max number of clients reached"):
        rejected += 1; s.close()
    else:
        assert r == b"+PONG\r\n", r
        held.append(s)
t0 = cpu_ticks(); time.sleep(1.0); spent = cpu_ticks() - t0
ok = len(held) == 32 and rejected == 13 and spent < 20
print(f"{'PASS' if ok else 'FAIL'} maxclients: {len(held)} served, {rejected} rejected, "
      f"{spent} CPU ticks/s while saturated (expect 32, 13, ~0)")
for s in held: s.close()
time.sleep(0.2)
s = socket.create_connection(("127.0.0.1", port), timeout=2); s.sendall(b"PING\r\n")
r = s.recv(100); print(("PASS" if r == b"+PONG\r\n" else "FAIL") + " accepts again after clients leave")
sys.exit(0 if ok and r == b"+PONG\r\n" else 1)
PY
[ $? -eq 0 ] || fail=1
expect_eq "rejection log is rate-limited (lines)" "1" "$(grep -c 'max number of clients' "$LIM_LOG")"
kill -TERM $LPID; wait $LPID
rm -f "$LIM_LOG"

echo
echo "== differential: same tests against real redis-server =="
REAL_PORT=$((PORT - 1))
if command -v redis-server >/dev/null; then
    redis-server --port "$REAL_PORT" --save "" --appendonly no --daemonize yes \
        --logfile /dev/null
    for _ in $(seq 50); do redis-cli -p "$REAL_PORT" PING >/dev/null 2>&1 && break; sleep 0.1; done
    TARGET=redis PORT=$REAL_PORT python3 test/test_server.py | grep -Ev '^(PASS|SKIP)' || fail=1
    redis-cli -p "$REAL_PORT" shutdown nosave >/dev/null 2>&1
else
    echo "SKIP redis-server not installed"
fi

echo
[ $fail -eq 0 ] && echo "ALL OK" || echo "SOME TESTS FAILED"
exit $fail
