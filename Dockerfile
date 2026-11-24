# Linux dev environment for lite-redis.
#
# The server uses epoll (Linux-only) and is leak-checked with Valgrind
# (Linux-only, no Apple Silicon macOS support), so all building, testing and
# benchmarking happens inside this container. Source is bind-mounted from the
# host, so you still edit files normally in your editor.
FROM ubuntu:24.04

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      clang make \
      valgrind gdb strace \
      redis-tools redis-server \
      python3 \
      procps iproute2 lsof \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /work
