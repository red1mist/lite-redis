#include <arpa/inet.h>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

constexpr int PORT = 6379;
constexpr int BACKLOG = 16;

int main() {
    // stdout is fully-buffered (not line-buffered) whenever it isn't a
    // terminal — e.g. redirected to a file or piped. Without this, our log
    // lines sit in a buffer and never reach disk until the process exits
    // cleanly, which makes a killed/crashed server look like it logged
    // nothing at all.
    setvbuf(stdout, nullptr, _IOLBF, 0);

    // write() to a socket the peer already closed raises SIGPIPE, and the
    // default disposition for that signal is to kill the process. We'd
    // rather get an EPIPE error back from write() and handle it in code.
    signal(SIGPIPE, SIG_IGN);

    // AF_INET = IPv4, SOCK_STREAM = TCP (reliable, ordered byte stream).
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    // Without this, restarting the server right after killing it fails with
    // "Address already in use" — the OS holds the port in TIME_WAIT for a
    // while after the last connection on it closes.
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    // Network byte order is big-endian; x86/ARM are little-endian, so port
    // and address numbers must be byte-swapped with htons/htonl before they
    // go into a struct the kernel/network will read.
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = htonl(INADDR_ANY); // listen on all local interfaces

    if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    // backlog = max pending connections the kernel will queue before we
    // accept() them.
    if (listen(listen_fd, BACKLOG) < 0) {
        perror("listen");
        return 1;
    }

    std::printf("lite-redis listening on port %d (echo mode)\n", PORT);

    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        // accept() blocks until a client connects, then returns a NEW fd
        // dedicated to that one client. listen_fd keeps listening for the
        // next connection — it's never used for reading/writing data.
        int client_fd = accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        std::printf("client connected: %s:%d\n", ip, ntohs(client_addr.sin_port));

        // Blocking model: while this client is connected, we serve nobody
        // else. A second client's connection just sits in the kernel's
        // backlog queue until we come back around to accept() again.
        char buf[4096];
        while (true) {
            ssize_t n = read(client_fd, buf, sizeof(buf));
            if (n <= 0) {
                // n == 0 means the peer closed the connection cleanly;
                // n < 0 means an error.
                break;
            }
            ssize_t written = write(client_fd, buf, static_cast<size_t>(n));
            if (written < 0) {
                perror("write");
                break;
            }
        }

        std::printf("client disconnected: %s:%d\n", ip, ntohs(client_addr.sin_port));
        close(client_fd);
    }

    close(listen_fd);
    return 0;
}
