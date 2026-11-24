# lite-redis

A Redis-compatible in-memory key-value store written from scratch in C++17 —
no threads, no external libraries, just POSIX sockets and Linux `epoll`.

> **Status:** work in progress. Built in stages; see the commit history.

## Roadmap

- [x] Stage 0 — project scaffolding
- [x] Stage 1 — blocking single-client TCP echo server
- [x] Stage 1.5 — Linux dev container
- [x] Stage 2 — non-blocking I/O + `epoll` event loop (concurrent clients)
- [x] Stage 3 — RESP protocol parser (`redis-cli` compatible)
- [x] Stage 4 — key-value store: `SET`, `GET`, `DEL` (+ `EXISTS`, `DBSIZE`)
- [x] Stage 5 — key expiry: `EXPIRE`, `TTL` (min-heap timer + lazy deletion)
- [x] Stage 6 — tests + Valgrind leak check under load
- [ ] Stage 7 — `redis-benchmark` results

## Layout

```
src/main.cpp      epoll event loop, connections, buffering, backpressure
src/resp.cpp      incremental RESP parser (partial reads, pipelining) + reply encoders
src/commands.cpp  command table and handlers
src/store.cpp     the keyspace (hash table, string_view lookups) and key
                  expiry (indexed min-heap + lazy deletion)
test/             raw-RESP integration tests (also run against real Redis
                  as a differential check), redis-cli smoke test, maxclients
                  and leak-at-exit checks
```

## Development

The server targets Linux (`epoll`, Valgrind), so everything runs in a Docker
container. The source is bind-mounted, so edit on the host as usual.

```sh
./dev make            # optimized build -> ./lite-redis-server
./dev make debug      # AddressSanitizer build -> ./lite-redis-server-debug
./dev make run        # build + start the server on :6379
./dev make test       # unit tests + ASan build + all integration tests +
                      # differential run vs real Redis
./dev make unit       # just the store/expiry-heap unit test (fake clock)
./dev make valgrind   # leak/memory-error check under load -> results/valgrind.txt

# in a second terminal (same container):
./dev redis-cli
./dev bash            # shell inside the container
./dev --stop          # tear the container down
```

Server flags: `--port N` (default 6379), `--maxclients N` (default 10000,
lowered automatically to fit the open-file limit), `--verbose` (log every
connect/disconnect). `Ctrl-C` / `SIGTERM` shut down cleanly.

## Supported commands

`PING`, `ECHO`, `QUIT`, `GET`,
`SET key value [NX|XX] [GET] [EX s|PX ms|KEEPTTL]`, `DEL`, `EXISTS`, `DBSIZE`,
`EXPIRE`/`PEXPIRE key ttl [NX|XX|GT|LT]`, `TTL`, `PTTL`, `PERSIST`, plus `COMMAND` / `CONFIG GET` stubs so `redis-cli` and
`redis-benchmark` work unmodified. Inline commands (telnet/nc) support Redis's
quoting rules.

When benchmarking, select the implemented commands, e.g.
`redis-benchmark -t set,get`: the default suite also runs `INCR`, `LPUSH` etc.,
which aren't implemented and make it exit.

## How key expiry works

- **Lazy deletion:** every lookup checks the key's deadline; an expired key
  is deleted on the spot and treated as missing, so clients never see one.
- **Active expiry:** keys with a TTL live in an **indexed binary min-heap**
  ordered by deadline. Before each `epoll_wait` the loop deletes due keys (at
  most 1000 per pass, so a mass expiry can't stall clients) and sleeps no
  longer than until the next deadline. Each entry stores its heap position, so
  `DEL` / `PERSIST` / a new `EXPIRE` fix the heap in O(log n) and the heap
  never accumulates stale nodes.
- Deadlines use `CLOCK_MONOTONIC`, so wall-clock adjustments can't mass-expire
  keys. (`EXAT`/`PXAT`/`EXPIREAT` absolute times are not supported.)

## Verification

| What | How | Result |
|---|---|---|
| Correctness | `make test`: 80+ raw-socket integration tests (fragmented/pipelined input, protocol errors, RST/half-close, backpressure, 32 MB values, expiry) | all pass |
| Redis compatibility | the same suite run against a real `redis-server` 7.0.15 (differential testing) | all shared tests match |
| Expiry heap | `make unit`: fake-clock randomized tests vs a reference model, invariants after every op; mutation-tested | 11/11 planted bugs caught |
| Memory safety, live | every `make test` runs an AddressSanitizer build and checks LeakSanitizer at a graceful exit | clean |
| **Leaks under load** | `make valgrind`: the -O2 server under memcheck (`--leak-check=full --show-leak-kinds=all --errors-for-leak-kinds=all --track-fds=yes`) through redis-benchmark, connection churn, TTL churn, the integration suite and a mass expiry, then SIGTERM with live clients and keys | **0 bytes in use at exit, 0 errors, 0 leaked fds** ([results/valgrind.txt](results/valgrind.txt)); the script is shown to FAIL on planted leaks, fd leaks and use-after-free |
