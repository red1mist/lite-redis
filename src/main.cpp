// lite-redis server: single-threaded, non-blocking, driven by epoll.
//
// One thread juggles any number of connections by asking the kernel "which
// sockets are ready?" (epoll_wait) and only ever touching sockets that won't
// block. Bytes read from a client are parsed as RESP commands (resp.cpp),
// executed (commands.cpp), and the replies queued in that client's output
// buffer.

#include "commands.h"
#include "resp.h"
#include "store.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <memory>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <string>
#include <sys/epoll.h>
#include <sys/resource.h>
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
// Backpressure threshold for a client's pending output. Past it we (1) stop
// executing that client's queued commands and (2) stop reading from it,
// until the kernel has taken the output. So a client that pipelines faster
// than it reads replies can't make the server buffer unbounded output.
constexpr size_t MAX_WBUF = 1024 * 1024;
// Hard cap on a client's unparsed input (same default as Redis's
// client-query-buffer-limit). Exceed it and the client is disconnected.
constexpr size_t MAX_QUERY_BUF = 1024ul * 1024 * 1024;
// Buffers bigger than this are given back to the allocator once idle, so one
// big request doesn't pin memory for the rest of a connection's life.
constexpr size_t IDLE_BUF_KEEP = 64 * 1024;
// Like Redis's maxclients. Extra connections are accepted and immediately
// told "-ERR max number of clients reached" instead of hanging in the backlog.
constexpr int DEFAULT_MAX_CLIENTS = 10000;
// fds we need besides clients: stdin/out/err, listener, epoll, spare fd, slack.
constexpr int RESERVED_FDS = 32;

// Per-connection state. TCP is a byte stream, not a message queue: one read()
// may return half a command or several commands, and one write() may accept
// only part of a reply. So every client keeps its own buffers that survive
// between event-loop iterations.
struct Conn {
    int fd;
    std::string addr;   // "ip:port", for logging (only filled with --verbose)
    std::string rbuf;   // bytes received but not yet parsed into a full command
    resp::ParseState parse; // progress through the partial command at rbuf[0]
    std::string wbuf;   // bytes queued for sending
    size_t wpos = 0;    // how much of wbuf has already been sent
    uint32_t interest = 0; // events currently registered with epoll
    // Complete commands are waiting in rbuf, but we stopped executing them
    // because pending output hit MAX_WBUF. Resumed from flush() once the
    // output drains (no new EPOLLIN would ever arrive for bytes we already
    // read, so the write path must restart processing).
    bool paused = false;
    // Client sent EOF (it may have only half-closed). Run what's left in
    // rbuf, flush the replies, then close.
    bool peer_eof = false;
    // QUIT or a protocol error: run nothing more, flush, then close.
    bool closing = false;
};

// Indexed by fd. The kernel always hands out the lowest free fd number, so
// fds stay small and dense and a vector gives O(1) lookup with no hashing.
static std::vector<std::unique_ptr<Conn>> conns;
static int epfd = -1;
static int listen_fd = -1;
static int num_clients = 0;
static int max_clients = DEFAULT_MAX_CLIENTS;
static bool verbose = false;

// A file descriptor held in reserve for the EMFILE case: see handle_accept.
static int spare_fd = -1;
// Set when even the spare-fd trick failed and we stopped watching the
// listener; re-armed when a connection closes and frees an fd.
static bool listener_disarmed = false;

// Set by SIGINT/SIGTERM. The handler only sets a flag (almost nothing is
// safe to call inside a signal handler); the event loop does the shutdown.
static volatile sig_atomic_t stop_requested = 0;

// The whole keyspace. Only the event-loop thread touches it, so no locking.
static Store store;

static void on_stop_signal(int) { stop_requested = 1; }

// For errors that can repeat thousands of times per second (EMFILE under a
// connection flood): print at most one line per second, so a bad situation
// can't also fill the disk with logs.
static void log_ratelimited(const char* msg) {
    static time_t last = 0;
    static long suppressed = 0;
    time_t now = time(nullptr);
    if (now == last) {
        suppressed++;
        return;
    }
    if (suppressed > 0)
        std::fprintf(stderr, "%s (and %ld similar messages suppressed)\n", msg, suppressed);
    else
        std::fprintf(stderr, "%s\n", msg);
    last = now;
    suppressed = 0;
}

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
//   - EPOLLIN only if we'd actually use more input right now: not closing,
//     no EOF seen (after EOF the socket is permanently "readable", which
//     level-triggered epoll would report on every iteration), not paused,
//     and output below MAX_WBUF (backpressure).
//   - EPOLLOUT only while there is unsent output. Asking for EPOLLOUT all the
//     time would be a busy loop: an idle socket is almost always writable.
static void update_interest(Conn& c) {
    uint32_t want = 0;
    if (!c.closing && !c.peer_eof && !c.paused && pending_output(c) < MAX_WBUF) want |= EPOLLIN;
    if (pending_output(c) > 0) want |= EPOLLOUT;
    if (want == c.interest) return; // skip a pointless epoll_ctl syscall

    epoll_event ev{};
    ev.events = want;
    ev.data.fd = c.fd;
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, c.fd, &ev) < 0) perror("epoll_ctl MOD");
    c.interest = want;
}

static void set_listener(bool armed) {
    epoll_event ev{};
    ev.events = armed ? EPOLLIN : 0;
    ev.data.fd = listen_fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, listen_fd, &ev);
    listener_disarmed = !armed;
}

static void close_conn(int fd) {
    if (verbose) std::printf("client disconnected: %s\n", conns[fd]->addr.c_str());
    // Order matters only for clarity: close() would also drop the fd from the
    // epoll set. Forgetting close() entirely leaks the fd; enough of those
    // and accept() fails with EMFILE ("Too many open files", ulimit -n).
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    conns[fd].reset(); // frees the Conn, its buffers and parser state
    num_clients--;
    if (listener_disarmed) set_listener(true); // an fd just became free
}

// Best-effort one-line error to a connection we're about to drop. The socket
// is brand new, so its send buffer is empty and a tiny write won't block.
static void reject(int fd, const char* reply) {
    ssize_t ignored = write(fd, reply, std::strlen(reply));
    (void)ignored;
    close(fd);
}

static void handle_accept() {
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
            if (errno == EMFILE || errno == ENFILE) {
                // Out of fds. We can't just return: the connection stays in
                // the backlog, level-triggered epoll reports the listener
                // again immediately, and we'd spin at 100% CPU. The classic
                // fix (libev, nginx): give up the spare fd we kept for this,
                // accept the connection, tell it why, close it, re-reserve.
                log_ratelimited("accept: out of file descriptors; rejecting connections");
                if (spare_fd >= 0) {
                    close(spare_fd);
                    spare_fd = -1;
                    int victim = accept4(listen_fd, nullptr, nullptr, SOCK_CLOEXEC);
                    if (victim >= 0) reject(victim, "-ERR max number of clients reached\r\n");
                    spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
                    if (victim >= 0) continue; // maybe more are queued
                }
                // Couldn't even do that: stop watching the listener until a
                // client disconnects (close_conn re-arms it).
                set_listener(false);
                return;
            }
            perror("accept4");
            return;
        }

        if (num_clients >= max_clients) {
            log_ratelimited("max number of clients reached; rejecting connection");
            reject(fd, "-ERR max number of clients reached\r\n");
            continue;
        }

        // Disable Nagle's algorithm. Nagle holds back small writes hoping to
        // coalesce them; combined with the client's delayed ACKs that can add
        // ~40ms to a tiny reply. Redis replies are tiny, so we want them sent
        // immediately. (Real Redis does the same.)
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        auto c = std::make_unique<Conn>();
        c->fd = fd;
        c->interest = EPOLLIN;
        if (verbose) {
            // Logging costs a write() syscall per connect/disconnect, which is
            // measurable when clients reconnect for every request, so it's
            // off by default (Redis also logs connections only at "verbose").
            char ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
            c->addr = std::string(ip) + ":" + std::to_string(ntohs(client_addr.sin_port));
            std::printf("client connected: %s\n", c->addr.c_str());
        }

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            perror("epoll_ctl ADD");
            close(fd);
            continue;
        }

        if (static_cast<size_t>(fd) >= conns.size()) conns.resize(fd + 1);
        conns[fd] = std::move(c);
        num_clients++;
    }
}

// Parse and execute the COMPLETE commands sitting in rbuf. A trailing partial
// command stays in rbuf (its parse progress in c.parse) until the rest of its
// bytes arrive.
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
    c.paused = false;

    while (!c.closing) {
        if (pending_output(c) >= MAX_WBUF) {
            // Backpressure: e.g. 3000 pipelined "GET bigkey" fit in one 64 KB
            // read but could produce gigabytes of replies. Stop here, let the
            // output drain, and resume from flush().
            c.paused = true;
            break;
        }
        size_t used;
        auto st = resp::parse_command(std::string_view(c.rbuf).substr(pos), c.parse, args, used, err);
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
        CommandContext ctx{c.wbuf, store};
        execute_command(args, ctx);
        if (ctx.close_after_reply) c.closing = true;
    }
    // Drop all consumed bytes with ONE erase per call, not one per command.
    // The partial command (if any) moves to rbuf[0]; c.parse stores offsets
    // relative to the command's start, so they stay valid.
    c.rbuf.erase(0, pos);
    if (c.rbuf.capacity() > IDLE_BUF_KEEP && c.rbuf.size() < c.rbuf.capacity() / 4) {
        if (c.rbuf.empty()) std::string().swap(c.rbuf); // release entirely
        else c.rbuf.shrink_to_fit();
    }
}

// Send as much pending output as the socket accepts; resume paused commands
// whenever the output fully drains; close the connection when it's finished.
// Returns false if the connection was closed (the Conn is gone).
static bool flush(Conn& c) {
    while (true) {
        while (pending_output(c) > 0) {
            // write() may accept only PART of what we ask (the kernel's socket
            // send buffer is finite). This is the bug Stage 1 had: it assumed
            // one write() sends everything.
            ssize_t n = write(c.fd, c.wbuf.data() + c.wpos, pending_output(c));
            if (n < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break; // send buffer full
                // EPIPE / ECONNRESET: peer is gone. (EPIPE, not a SIGPIPE
                // crash, because main() ignores SIGPIPE.)
                close_conn(c.fd);
                return false;
            }
            c.wpos += static_cast<size_t>(n);
        }
        if (pending_output(c) > 0) {
            // Kernel buffer full; EPOLLOUT will bring us back. Track progress
            // with an offset rather than erasing the sent prefix after every
            // write (O(n) each time). But a client that keeps pipelining may
            // never let wbuf fully drain, so drop the prefix once it's at
            // least half the buffer: each byte moves at most about once,
            // amortized O(1).
            if (c.wpos >= 64 * 1024 && c.wpos * 2 >= c.wbuf.size()) {
                c.wbuf.erase(0, c.wpos);
                c.wpos = 0;
            }
            break;
        }

        // Everything sent.
        c.wpos = 0;
        if (c.wbuf.capacity() > IDLE_BUF_KEEP) std::string().swap(c.wbuf); // give memory back
        else c.wbuf.clear();

        if (c.closing) { // QUIT / protocol error: replies delivered, hang up
            close_conn(c.fd);
            return false;
        }
        if (!c.paused) break;
        process_input(c); // run the commands backpressure held back...
        // ...and loop to send their replies.
    }
    if (c.peer_eof && !c.paused && pending_output(c) == 0) {
        close_conn(c.fd); // client is done sending and has all its replies
        return false;
    }
    update_interest(c);
    return true;
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
        // `printf 'PING\r\n' | nc` does). Complete commands already ran (or
        // are paused and will run); flush() closes once all replies are out.
        // An incomplete trailing command can never complete: dropped.
        c.peer_eof = true;
    } else {
        c.rbuf.append(buf, static_cast<size_t>(n));
        if (c.rbuf.size() > MAX_QUERY_BUF) {
            log_ratelimited("closing client that exceeded the query buffer limit");
            close_conn(c.fd);
            return;
        }
        process_input(c);
    }

    // Try to send right away rather than waiting for an EPOLLOUT round trip:
    // the socket is almost always writable, so this usually finishes the
    // reply in the same iteration. Only leftovers wait for EPOLLOUT.
    flush(c);
}

// Raise the per-process open-file limit so we can actually hold max_clients
// connections (the soft limit is often just 1024), and lower max_clients if
// the hard limit doesn't allow it. Redis does the same at startup.
static void adjust_fd_limit() {
    rlimit rl{};
    if (getrlimit(RLIMIT_NOFILE, &rl) != 0) return;
    rlim_t want = static_cast<rlim_t>(max_clients + RESERVED_FDS);
    if (rl.rlim_cur < want) {
        rlimit raised = rl;
        raised.rlim_cur = (rl.rlim_max == RLIM_INFINITY || rl.rlim_max >= want) ? want : rl.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &raised) == 0) rl = raised;
    }
    if (rl.rlim_cur < want) {
        max_clients = static_cast<int>(rl.rlim_cur) - RESERVED_FDS;
        if (max_clients < 1) max_clients = 1;
        std::fprintf(stderr, "open-file limit is %llu: max clients lowered to %d\n",
                     static_cast<unsigned long long>(rl.rlim_cur), max_clients);
    }
}

int main(int argc, char** argv) {
    int port = DEFAULT_PORT;
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--maxclients") == 0 && i + 1 < argc) {
            max_clients = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        } else {
            std::fprintf(stderr, "usage: %s [--port N] [--maxclients N] [--verbose]\n", argv[0]);
            return 1;
        }
    }
    if (port <= 0 || port > 65535 || max_clients < 1) {
        std::fprintf(stderr, "invalid --port or --maxclients\n");
        return 1;
    }

    // stdout is fully-buffered (not line-buffered) whenever it isn't a
    // terminal; line-buffer it so logs show up even if we're killed.
    setvbuf(stdout, nullptr, _IOLBF, 0);

    // write() to a socket the peer already closed raises SIGPIPE, whose
    // default action kills the process. Ignore it and handle EPIPE instead.
    signal(SIGPIPE, SIG_IGN);

    // Graceful shutdown on Ctrl-C / `kill`: so leak checkers (Valgrind, ASan)
    // see a normal exit with every connection freed, not a process killed
    // mid-flight.
    //
    // The race to avoid: a signal arriving just BEFORE we enter epoll_wait
    // would set the flag, but we'd then sleep (possibly forever) without
    // noticing. Fix: keep SIGINT/SIGTERM blocked while running, and let
    // epoll_pwait unblock them atomically only while sleeping (same idea
    // as pselect). The signal then always interrupts the wait (EINTR).
    struct sigaction sa{};
    sa.sa_handler = on_stop_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    sigset_t blocked, wait_mask;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGINT);
    sigaddset(&blocked, SIGTERM);
    sigprocmask(SIG_BLOCK, &blocked, &wait_mask); // wait_mask = mask without them

    adjust_fd_limit();
    spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);

    listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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
    addr.sin_port = htons(static_cast<uint16_t>(port)); // host -> network byte order
    addr.sin_addr.s_addr = htonl(INADDR_ANY);           // all local interfaces

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

    std::printf("lite-redis listening on port %d (epoll, max %d clients)\n", port, max_clients);

    epoll_event events[MAX_EVENTS];
    while (!stop_requested) {
        // Sleep until at least one fd is ready. Timeout -1 = wait forever.
        // (Stage 5 replaces -1 with "time until the next key expires".)
        int n = epoll_pwait(epfd, events, MAX_EVENTS, -1, &wait_mask);
        if (n < 0) {
            if (errno == EINTR) continue; // a signal: loop re-checks stop_requested
            perror("epoll_wait");
            return 1;
        }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint32_t e = events[i].events;

            if (fd == listen_fd) {
                handle_accept();
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
                if (!flush(c)) continue; // closed
            }
            if (e & EPOLLIN) {
                handle_read(c);
            }
        }
    }

    // Clean shutdown: close every connection and free everything, so a leak
    // checker can tell "still reachable at exit" apart from real leaks.
    std::printf("shutting down: closing %d client(s)\n", num_clients);
    for (auto& c : conns)
        if (c) close_conn(c->fd);
    conns.clear();
    conns.shrink_to_fit();
    close(listen_fd);
    close(epfd);
    if (spare_fd >= 0) close(spare_fd);
    return 0;
}
