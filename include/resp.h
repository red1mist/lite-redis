// RESP (REdis Serialization Protocol), version 2: the wire format spoken by
// redis-cli, redis-benchmark, and every Redis client library.
//
// Clients send each command as an array of bulk strings:
//
//     SET k hello   ->   *3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n
//                        ^ 3 elements, then each is $<len>\r\n<bytes>\r\n
//
// Bulk strings are length-prefixed, so values are binary-safe: they may
// contain \r\n or NUL bytes and we never scan them for delimiters.
//
// Replies use a type prefix byte:
//     +OK\r\n               simple string
//     -ERR message\r\n      error
//     :42\r\n               integer
//     $5\r\nhello\r\n       bulk string     ($-1\r\n = null, e.g. GET miss)
//     *2\r\n...             array of N more replies
#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace resp {

// Bulk limit = real Redis's proto-max-bulk-len default. The arg-count cap is
// stricter than Redis 7 (which allows up to INT_MAX elements).
// They stop a malicious or buggy client from making us allocate unbounded
// memory just by sending a huge length header.
constexpr size_t MAX_BULK_LEN = 512 * 1024 * 1024;
constexpr size_t MAX_ARGS = 1024 * 1024;
// An inline command longer than this without a newline is garbage, not a
// command still in flight (same limit as Redis).
constexpr size_t MAX_INLINE_LEN = 64 * 1024;
// A "*<n>" / "$<n>" header holds at most a sign and 19 digits. Anything much
// longer without CRLF is garbage; rejecting early keeps re-scanning cheap.
constexpr size_t MAX_HEADER_LEN = 32;

enum class ParseStatus {
    Ok,          // one full command parsed
    Incomplete,  // valid so far, but need more bytes (TCP split it)
    Error,       // protocol violation; reply with an error and close
};

// Per-connection parser progress. A command can arrive spread over many
// read()s; this remembers how far we got so each byte is examined ONCE
// instead of re-parsing the whole command from its first byte on every read.
// (Without it, a command with many small args trickling in costs
// O(args x reads): quadratic. Real Redis keeps the same kind of state in its
// client struct: multibulklen / bulklen.)
//
// Positions are byte OFFSETS from the start of the current command, not
// pointers/string_views, because the connection's read buffer may be
// reallocated (it grows) or have consumed bytes erased in front of the
// command between reads. Offsets relative to the command start survive both.
struct ParseState {
    long long remaining = -1; // args still to read; -1 = "*<n>" header not read yet
    size_t pos = 0;           // bytes of this command fully parsed so far
    std::vector<std::pair<size_t, size_t>> spans; // (offset, length) of each arg found
    // Inline commands with quotes/escapes ("a\x41") can't be views into the
    // raw bytes, so their unescaped args live here.
    std::vector<std::string> inline_args;

    void reset() {
        remaining = -1;
        pos = 0;
        spans.clear(); // keeps capacity: no reallocation for the next command
    }
};

// Try to parse ONE command. `buf` must begin at the start of the current
// command (the same command `st` describes), every call.
//
// Ok:         `args` holds the command's arguments and `consumed` is how many
//             bytes of `buf` it used; `st` is reset for the next command.
//             The args are string_views pointing INTO `buf` (no copies; for
//             inline commands, into st.inline_args): valid only until `buf`
//             changes, so a command must copy anything it wants to keep.
//             `args` may be empty (blank inline line, "*0"): skip it.
// Incomplete: need more bytes. Progress is saved in `st`; call again with the
//             same command start once more bytes have arrived.
// Error:      `err` describes the problem.
ParseStatus parse_command(std::string_view buf, ParseState& st,
                          std::vector<std::string_view>& args, size_t& consumed,
                          std::string& err);

// Reply encoders: append one RESP value to `out`.
void add_simple(std::string& out, std::string_view s);  // must not contain \r\n
void add_error(std::string& out, std::string_view msg); // CR/LF replaced by spaces
void add_integer(std::string& out, long long n);
void add_bulk(std::string& out, std::string_view s);
void add_null(std::string& out);
void add_array_header(std::string& out, size_t n);

} // namespace resp
