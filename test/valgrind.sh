#!/usr/bin/env bash
# Leak / memory-error verification under load (inside the dev container):
#
#     ./dev make valgrind
#
# Runs the OPTIMIZED server (-O2, plus -g so reports have line numbers)
# under Valgrind memcheck with the strictest settings:
#   --leak-check=full --show-leak-kinds=all --errors-for-leak-kinds=all
#       every unfreed block at exit counts as an ERROR, including "still
#       reachable" ones (memory a pointer still references but nobody freed)
#   --track-fds=yes
#       report every file descriptor still open at exit (leaked sockets)
#   --error-exitcode=99
#       any memory error (invalid read/write, use of uninitialised value,
#       bad free, ...) or leak makes valgrind exit 99
#
# Then drives it through every kind of load we have, shuts it down with
# SIGTERM while clients and keys are still live, and checks the verdict.
# A summary is written to results/valgrind.txt (committed as evidence).
#
# Set BIN=... to run a different binary (used to prove this script catches
# a deliberately planted leak).
set -uo pipefail
cd "$(dirname "$0")/.."

PORT=${PORT:-6410}
BIN=${BIN:-./lite-redis-server-vg}
LOG=$(mktemp)
OUT=${OUT:-results/valgrind.txt}
mkdir -p results

step() { echo; echo "== $* =="; }
commit_id() { # the container has no git: read .git directly
    local head ref
    head=$(cat .git/HEAD 2>/dev/null) || { echo unknown; return; }
    if [[ $head == ref:* ]]; then
        ref=${head#ref: }
        cat ".git/$ref" 2>/dev/null | cut -c1-7 ||
            grep " $ref\$" .git/packed-refs 2>/dev/null | cut -c1-7
    else
        echo "${head:0:7}"
    fi
}
bench() { # quiet redis-benchmark, print just the throughput lines
    timeout 600 redis-benchmark -p "$PORT" -q "$@" 2>&1 | tr '\r' '\n' |
        grep -E "requests per second|rror" | sed 's/^/   /'
}

step "starting $BIN under valgrind"
valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all \
    --errors-for-leak-kinds=all --track-fds=yes --error-exitcode=99 \
    --log-file="$LOG" "$BIN" --port "$PORT" >/dev/null 2>&1 &
VG=$!
trap 'kill $VG 2>/dev/null; rm -f "$LOG"' EXIT
for _ in $(seq 300); do redis-cli -p "$PORT" PING >/dev/null 2>&1 && break; sleep 0.1; done
redis-cli -p "$PORT" PING >/dev/null || { echo "server did not start"; cat "$LOG"; exit 1; }
T0=$(date +%s)

step "1. redis-benchmark: PING/SET/GET, 50 keep-alive clients"
bench -t ping,set,get -n 30000 -c 50

step "2. redis-benchmark: SET/GET pipelined (-P 16), random keys"
bench -t set,get -n 100000 -c 20 -P 16 -r 50000 -d 100

step "3. redis-benchmark: SET with TTL (keys expire DURING the run)"
bench -n 60000 -c 20 -P 8 -r 30000 SET "ttl:__rand_int__" v PX 1500
bench -n 30000 -c 20 -P 8 -r 30000 EXPIRE "ttl:__rand_int__" 1
bench -n 30000 -c 20 -P 8 -r 30000 PERSIST "ttl:__rand_int__"

step "4. connection churn: a new TCP connection for every request (-k 0)"
bench -t ping -n 3000 -c 10 -k 0

step "5. full integration suite (protocol errors, RST/half-close, backpressure, 32 MB values, ...)"
SLOW_SERVER=1 PORT=$PORT timeout 1800 python3 test/test_server.py > /tmp/vg-tests.txt 2>&1
TESTS_RC=$?
tail -1 /tmp/vg-tests.txt | sed 's/^/   /'
grep "^FAIL" /tmp/vg-tests.txt | sed 's/^/   /'

step "6. mass expiry: 100k keys expiring together, nobody reads them"
python3 - "$PORT" <<'PY'
import socket, sys, time
port = int(sys.argv[1]); n = 100000
s = socket.create_connection(("127.0.0.1", port)); s.settimeout(600)
def cmd(b):
    s.sendall(b); return s.recv(100)
before = int(cmd(b"DBSIZE\r\n")[1:-2])
s.sendall(b"".join(b"*5\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n$2\r\nPX\r\n$4\r\n2000\r\n"
                   % (len(k), k) for k in (f"mass:{i}".encode() for i in range(n))))
got = 0
while got < n * 5: got += len(s.recv(1 << 20))
t0 = time.time()
while int(cmd(b"DBSIZE\r\n")[1:-2]) > before and time.time() - t0 < 300: time.sleep(0.2)
print(f"   {n} keys set and all expired by active expiry (DBSIZE back to {before})")
PY

step "7. shutdown with live state: keys with/without TTL, idle + mid-command clients"
python3 - "$PORT" <<'PY' &
import socket, sys, time
port = int(sys.argv[1])
s = socket.create_connection(("127.0.0.1", port))
s.sendall(b"".join(b"SET live:%d value-%d EX 3600\r\nSET perm:%d value\r\n" % (i, i, i) for i in range(5000)))
time.sleep(3)
idle = [socket.create_connection(("127.0.0.1", port)) for _ in range(5)]
half = socket.create_connection(("127.0.0.1", port))
half.sendall(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100000\r\npartial")  # incomplete command
time.sleep(60)  # stay connected until the server is shut down
PY
HOLDER=$!
sleep 6
echo "   DBSIZE before shutdown: $(redis-cli -p "$PORT" DBSIZE)"

step "SIGTERM -> graceful shutdown -> valgrind's verdict"
kill -TERM $VG
wait $VG; VG_RC=$?
kill $HOLDER 2>/dev/null; wait $HOLDER 2>/dev/null
ELAPSED=$(( $(date +%s) - T0 ))

# ---- verdict ---------------------------------------------------------------
in_use=$(grep -o "in use at exit: .*" "$LOG" | head -1)
errors=$(grep -o "ERROR SUMMARY: [0-9]* errors" "$LOG" | tail -1)
fds=$(grep -o "FILE DESCRIPTORS: .*" "$LOG" | tail -1)
allocs=$(grep -o "total heap usage: .*" "$LOG" | head -1)
leak_lines=$(grep -E "(definitely|indirectly|possibly) lost:|still reachable:" "$LOG" || true)

pass=1
[ "$VG_RC" -eq 0 ] || pass=0
grep -q "in use at exit: 0 bytes in 0 blocks" "$LOG" || pass=0
grep -q "ERROR SUMMARY: 0 errors" "$LOG" || pass=0
# Only stdin/stdout/stderr may remain open. Valgrind lists its own --log-file
# as an open descriptor too, so that one is excluded.
leaked_fds=$(grep -E "Open (file descriptor|AF_INET socket) [0-9]+" "$LOG" |
    grep -vE "descriptor [012]:" | grep -vF "$LOG" || true)
[ -z "$leaked_fds" ] || pass=0
[ "$TESTS_RC" -eq 0 ] || pass=0

{
    echo "lite-redis: Valgrind memcheck under load"
    echo "commit:   $(commit_id)"
    echo "date:     $(date -u '+%Y-%m-%d %H:%M UTC')"
    echo "platform: $(uname -sm), $(valgrind --version), $(clang++ --version | head -1)"
    echo "binary:   $BIN (-O2 -g, no sanitizers)"
    echo "flags:    --leak-check=full --show-leak-kinds=all --errors-for-leak-kinds=all --track-fds=yes"
    echo
    echo "workload (${ELAPSED}s under valgrind):"
    echo "  1. redis-benchmark PING/SET/GET, 50 clients, 90k requests"
    echo "  2. redis-benchmark SET/GET -P 16, 100k random keys, 100 B values"
    echo "  3. SET PX 1500 / EXPIRE / PERSIST on 30k random keys (120k requests, keys expiring mid-run)"
    echo "  4. 3000 requests each on a fresh TCP connection (-k 0)"
    echo "  5. integration suite: $(tail -1 /tmp/vg-tests.txt)"
    echo "  6. 100k keys expiring together (active expiry)"
    echo "  7. SIGTERM with 10k live keys (5k with TTL), 5 idle clients, 1 client mid-command"
    echo
    echo "result:"
    echo "  valgrind exit code: $VG_RC (99 would mean errors or leaks)"
    echo "  $allocs"
    echo "  $in_use"
    [ -n "$leak_lines" ] && echo "$leak_lines" | sed 's/^==[0-9]*== */  /'
    echo "  $fds (std + valgrind's own log file)"
    if [ -z "$leaked_fds" ]; then
        echo "  leaked fds: none"
    else
        echo "  leaked fds: $(echo "$leaked_fds" | grep -c .), e.g.:"
        echo "$leaked_fds" | head -3 | sed 's/^==[0-9]*== */    /'
    fi
    echo "  $errors"
    echo
    [ $pass -eq 1 ] && echo "VERDICT: PASS (no leaks, no memory errors, no leaked fds)" \
                    || echo "VERDICT: FAIL"
} | tee "$OUT"

if [ $pass -ne 1 ]; then
    echo; echo "---- valgrind log (errors / leaks) ----"
    grep -E "Invalid|uninitialised|lost|reachable|Open|at 0x|by 0x" "$LOG" | head -80
    exit 1
fi
