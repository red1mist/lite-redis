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
- [ ] Stage 4 — key-value store: `SET`, `GET`, `DEL`
- [ ] Stage 5 — key expiry: `EXPIRE`, `TTL` (min-heap timer + lazy deletion)
- [ ] Stage 6 — tests + Valgrind leak check under load
- [ ] Stage 7 — `redis-benchmark` results

## Layout

```
src/main.cpp      epoll event loop, connections, buffering, backpressure
src/resp.cpp      incremental RESP parser (partial reads, pipelining) + reply encoders
src/commands.cpp  command table and handlers
test/             raw-RESP integration tests + redis-cli smoke test
```

## Development

The server targets Linux (`epoll`, Valgrind), so everything runs in a Docker
container. The source is bind-mounted, so edit on the host as usual.

```sh
./dev make            # optimized build -> ./lite-redis-server
./dev make debug      # AddressSanitizer build -> ./lite-redis-server-debug
./dev make run        # build + start the server on :6379
./dev make test       # ASan build + integration tests + redis-cli smoke test

# in a second terminal (same container):
./dev redis-cli
./dev bash            # shell inside the container
./dev --stop          # tear the container down
```
