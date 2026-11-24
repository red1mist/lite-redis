#!/usr/bin/env bash
# Full test run (inside the dev container):   ./dev make test
#
# 1. starts the AddressSanitizer build of the server on a spare port
# 2. runs the raw-RESP integration tests
# 3. runs a smoke test with the real redis-cli (one-shot + interactive)
# 4. checks the server leaked no file descriptors and ASan reported nothing
set -uo pipefail
cd "$(dirname "$0")/.."

export PORT=${PORT:-6399}
BIN=./lite-redis-server-debug
LOG=$(mktemp)
fail=0

$BIN --port "$PORT" >"$LOG" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null; rm -f "$LOG"' EXIT
for _ in $(seq 50); do redis-cli -p "$PORT" PING >/dev/null 2>&1 && break; sleep 0.1; done
FDS_BEFORE=$(ls /proc/$PID/fd | wc -l)

echo "== integration tests (raw RESP) =="
python3 test/test_server.py || fail=1

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
if grep -q "AddressSanitizer" "$LOG"; then
    echo "FAIL AddressSanitizer report:"; grep -A20 AddressSanitizer "$LOG"; fail=1
else
    echo "PASS no AddressSanitizer errors"
fi

echo
[ $fail -eq 0 ] && echo "ALL OK" || echo "SOME TESTS FAILED"
exit $fail
