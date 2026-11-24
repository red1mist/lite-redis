// lite-redis server: single-threaded, non-blocking, driven by epoll.
//
// One thread juggles any number of connections by asking the kernel "which
// sockets are ready?" (epoll_wait) and only ever touching sockets that won't
// block. Bytes read from a client are parsed as RESP commands (resp.cpp),
// executed (commands.cpp), and the replies queued in that client's output
// buffer.

#include "commands.h"
#include "resp.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

constexpr int DEFAULT_PORT = 6379;
// Max pending connections the kernel queues before we accept() them. The
// kernel silently caps this at /proc/sys/net/core/somaxconn. redis-benchmark
// opens 50 connections at once by default, so 16 (Stage 1) is too small.
constexpr int BACKLOG = 511;
constexpr int MAX_EVENTS = 256;          // events returned per epoll_wait call
constexpr size_t READ_CHUNK = 64 * 1024; // bytes per read() syscall
// Backpressure: if a client sends faster than it reads its replies, its
// output buffer grows without bound. Past this size we stop reading from it
// until the buffer drains (see update_interest).
constexpr size_t MAX_WBUF = 1024 * 1024;

// Per-connection state. TCP is a byte stream, not a message queue: one read()
// may return half a command or several commands, and one write() may accept
// only part of a reply. So every client keeps its own buffers that survive
// between event-loop iterations.
struct Conn {
    int fd;
    std::string addr;   // "ip:port", for logging
    std::string rbuf;   // bytes received but not yet parsed into a full command
    std::string wbuf;   // bytes queued for sending
    size_t wpos = 0;    // how much of wbuf has already been sent
    uint32_t interest = 0; // events currently registered with epoll
    // Set after QUIT, a protocol error, or EOF from the client: stop reading,
    // flush whatever replies are still queued, then close.
    bool closing = false;
};

// Indexed by fd. The kernel always hands out the lowest free fd number, so
// fds stay small and dense and a vector gives O(1) lookup with no hashing.
static std::vector<std::unique_ptr<Conn>> conns;
static int epfd = -1;

static bool set_nonblocking(int fd) {
    // O_NONBLOCK changes what read/write/accept do when they can't make
    // progress: instead of putting the thread to sleep they return -1 with
    // errno = EAGAIN (== EWOULDBLOCK on Linux), and we go serve someone else.
    int flags = fcntl(fd, F_GETFL, 0);
    return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

static size_t pending_output(const Conn& c) { return c.wbuf.size() - c.wpos; }

// Level-triggered epoll (the default, no EPOLLET): epoll_wait keeps reporting
// a socket as long as the condition holds (unread data / room to write). That
// means we don't have to drain a socket to EAGAIN on every wakeup: leftovers
// are simply reported again next iteration. With edge-triggered we'd be
// notified only on state *changes* and would have to loop until EAGAIN or
// risk never hearing about that socket again.
//
// Which events we ask for is derived purely from the connection's state:
//   - EPOLLIN unless the output buffer is over MAX_WBUF (backpressure).
//   - EPOLLOUT only while there is unsent output. Asking for EPOLLOUT all the
//     time would be a busy loop: an idle socket is almost always writable.
static void update_interest(Conn& c) {
    uint32_t want = 0;
    // A closing connection must not ask for EPOLLIN: after EOF the socket is
    // permanently "readable" (read returns 0), so level-triggered epoll would
    // wake us for it on every iteration.
    if (!c.closing && pending_output(c) < MAX_WBUF) want |= EPOLLIN;
    if (pending_output(c) > 0) want |= EPOLLOUT;
    if (want == c.interest) return; // skip a pointless epoll_ctl syscall

    epoll_event ev{};
    ev.events = want;
    ev.data.fd = c.fd;
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, c.fd, &ev) < 0) perror("epoll_ctl MOD");
    c.interest = want;
}

static void close_conn(int fd) {
    std::printf("client disconnected: %s\n", conns[fd]->addr.c_str());
    // Order matters only for clarity: close() would also drop the fd from the
    // epoll set. Forgetting close() entirely leaks the fd; enough of those
    // and accept() fails with EMFILE ("Too many open files", ulimit -n).
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    conns[fd].reset(); // frees the Conn and its buffers
}

static void handle_accept(int listen_fd) {
    // Several clients may be queued behind a single readiness notification,
    // so keep accepting until the backlog is empty (EAGAIN).
    while (true) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        // accept4 with SOCK_NONBLOCK sets O_NONBLOCK atomically, saving the
        // two fcntl() calls set_nonblocking would cost per connection.
        int fd = accept4(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &len,
                         SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return; // backlog drained
            if (errno == EINTR || errno == ECONNABORTED) continue;
            // EMFILE/ENFILE: out of fds. The pending connection stays in the
            // backlog and level-triggered epoll will report it again.
            perror("accept4");
            return;
        }

        // Disable Nagle's algorithm. Nagle holds back small writes hoping to
        // coalesce them; combined with the client's delayed ACKs that can add
        // ~40ms to a tiny reply. Redis replies are tiny, so we want them sent
        // immediately. (Real Redis does the same.)
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));

        auto c = std::make_unique<Conn>();
        c->fd = fd;
        c->addr = std::string(ip) + ":" + std::to_string(ntohs(client_addr.sin_port));
        c->interest = EPOLLIN;

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            perror("epoll_ctl ADD");
            close(fd);
            continue;
        }

        if (static_cast<size_t>(fd) >= conns.size()) conns.resize(fd + 1);
        std::printf("client connected: %s\n", c->addr.c_str());
        conns[fd] = std::move(c);
    }
}

// Returns false if the connection was closed.
static bool handle_write(Conn& c) {
    while (pending_output(c) > 0) {
        // write() may accept only PART of what we ask (the kernel's socket
        // send buffer is finite). This is the bug Stage 1 had: it assumed one
        // write() sends everything.
        ssize_t n = write(c.fd, c.wbuf.data() + c.wpos, pending_output(c));
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break; // send buffer full
            // EPIPE / ECONNRESET: peer is gone. (EPIPE, not a SIGPIPE crash,
            // because main() ignores SIGPIPE.)
            close_conn(c.fd);
            return false;
        }
        c.wpos += static_cast<size_t>(n);
    }
    // Track progress with an offset instead of erasing the sent prefix after
    // every write (erase shifts the remaining bytes: O(n) per call).
    if (pending_output(c) == 0) {
        c.wbuf.clear();
        c.wpos = 0;
        if (c.closing) { // everything flushed; now we can hang up
            close_conn(c.fd);
            return false;
        }
    } else if (c.wpos >= 64 * 1024 && c.wpos * 2 >= c.wbuf.size()) {
        // A client that keeps pipelining may never let wbuf fully drain, so
        // "clear when empty" alone would let the already-sent prefix pile up
        // forever. Drop it once it's at least half the buffer: each byte is
        // moved at most about once, so this stays amortized O(1) per byte.
        c.wbuf.erase(0, c.wpos);
        c.wpos = 0;
    }
    update_interest(c);
    return true;
}

// Parse and execute every COMPLETE command sitting in rbuf. A trailing
// partial command stays in rbuf until the rest of its bytes arrive.
//
// Pipelining falls out of this naturally: if a client sends 16 commands in
// one packet, a single read() returns all 16, we run them back to back, and
// their 16 replies go out in a single write(). That is why redis-benchmark -P
// (pipelining) multiplies throughput: syscalls, not command execution, are
// the expensive part.
static void process_input(Conn& c) {
    static std::vector<std::string_view> args; // reused: no allocation per command
    std::string err;
    size_t pos = 0;

    while (!c.closing) {
        size_t used;
        auto st = resp::parse_command(std::string_view(c.rbuf).substr(pos), args, used, err);
        if (st == resp::ParseStatus::Incomplete) break;
        if (st == resp::ParseStatus::Error) {
            // We can't tell where the next command starts in a corrupt
            // stream, so (like real Redis) reply with the error and hang up.
            resp::add_error(c.wbuf, "ERR Protocol error: " + err);
            c.closing = true;
            break;
        }
        pos += used;
        if (args.empty()) continue; // blank line / "*0"

        // args point into c.rbuf; replies go to c.wbuf, a different buffer,
        // so the views stay valid while the command runs.
        CommandContext ctx{c.wbuf};
        execute_command(args, ctx);
        if (ctx.close_after_reply) c.closing = true;
    }
    // Drop all consumed bytes with ONE erase per read, not one per command.
    c.rbuf.erase(0, pos);
}

static void handle_read(Conn& c) {
    // One read() per wakeup is enough under level-triggered epoll: anything
    // left in the socket gets reported again on the next epoll_wait. It also
    // keeps one chatty client from starving the others.
    char buf[READ_CHUNK];
    ssize_t n = read(c.fd, buf, sizeof(buf));
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
        close_conn(c.fd); // ECONNRESET etc.
        return;
    }
    if (n == 0) {
        // EOF: the client is done sending (it may have only half-closed, as
        // `printf 'PING\r\n' | nc` does). Every complete command was already
        // run when its bytes arrived, so just flush pending replies and close.
        c.closing = true;
    } else {
        c.rbuf.append(buf, static_cast<size_t>(n));
        process_input(c);
    }

    // Try to send right away rather than waiting for an EPOLLOUT round trip:
    // the socket is almost always writable, so this usually finishes the
    // reply in the same iteration. Only leftovers wait for EPOLLOUT.
    // (handle_write also closes the connection if it's closing and drained.)
    handle_write(c);
}

int main(int argc, char** argv) {
    // Usage: lite-redis-server [--port N]
    int port = DEFAULT_PORT;
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = std::atoi(argv[++i]);
        } else {
            std::fprintf(stderr, "usage: %s [--port N]\n", argv[0]);
            return 1;
        }
    }
    if (port <= 0 || port > 65535) {
        std::fprintf(stderr, "invalid port\n");
        return 1;
    }

    // stdout is fully-buffered (not line-buffered) whenever it isn't a
    // terminal; line-buffer it so logs show up even if we're killed.
    setvbuf(stdout, nullptr, _IOLBF, 0);

    // write() to a socket the peer already closed raises SIGPIPE, whose
    // default action kills the process. Ignore it and handle EPIPE instead.
    signal(SIGPIPE, SIG_IGN);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    // Allow immediate restart: otherwise bind() fails with "Address already in
    // use" while the old port sits in TIME_WAIT.
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));                 // host -> network byte order
    addr.sin_addr.s_addr = htonl(INADDR_ANY);    // all local interfaces

    if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(listen_fd, BACKLOG) < 0) {
        perror("listen");
        return 1;
    }
    // The listening socket must be non-blocking too, otherwise the accept()
    // loop in handle_accept would block once the backlog is empty.
    if (!set_nonblocking(listen_fd)) {
        perror("fcntl");
        return 1;
    }

    // epoll instance: a kernel object holding the set of fds we care about.
    // Unlike select/poll, we register each fd ONCE (epoll_ctl) instead of
    // passing the whole list on every call, and epoll_wait returns only the
    // fds that are ready. So the per-wakeup cost scales with the number of
    // READY sockets, not the number of connected ones.
    epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        perror("epoll_create1");
        return 1;
    }
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
        perror("epoll_ctl ADD listen");
        return 1;
    }

    std::printf("lite-redis listening on port %d (epoll)\n", port);

    epoll_event events[MAX_EVENTS];
    while (true) {
        // Sleep until at least one fd is ready. Timeout -1 = wait forever.
        // (Stage 5 replaces -1 with "time until the next key expires".)
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) continue; // interrupted by a signal; retry
            perror("epoll_wait");
            return 1;
        }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint32_t e = events[i].events;

            if (fd == listen_fd) {
                handle_accept(listen_fd);
                continue;
            }

            // An earlier event in this same batch may have closed this fd.
            if (static_cast<size_t>(fd) >= conns.size() || !conns[fd]) continue;
            Conn& c = *conns[fd];

            // EPOLLERR/EPOLLHUP are always reported, even if not requested.
            // Only bail out if there's nothing left to read: a peer can send
            // data and then close, and we still want to process that data.
            if ((e & (EPOLLERR | EPOLLHUP)) && !(e & EPOLLIN)) {
                close_conn(fd);
                continue;
            }
            if (e & EPOLLOUT) {
                if (!handle_write(c)) continue; // closed
            }
            if (e & EPOLLIN) {
                handle_read(c);
            }
        }
    }
}
