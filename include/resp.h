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
#include <vector>

namespace resp {

// Same limits as real Redis (proto-max-bulk-len, multibulk length limit).
// They stop a malicious or buggy client from making us allocate unbounded
// memory just by sending a huge length header.
constexpr size_t MAX_BULK_LEN = 512 * 1024 * 1024;
constexpr size_t MAX_ARGS = 1024 * 1024;
// A header line ("*3", "$5") or inline command longer than this without a
// newline is garbage, not a command still in flight.
constexpr size_t MAX_LINE_LEN = 64 * 1024;

enum class ParseStatus {
    Ok,          // one full command parsed
    Incomplete,  // valid so far, but need more bytes (TCP split it)
    Error,       // protocol violation; reply with an error and close
};

// Try to parse ONE command from the front of `buf`.
//
// Ok:         `args` holds the command's arguments and `consumed` is how many
//             bytes of `buf` it used. The args are string_views pointing INTO
//             `buf` (no copies): they stay valid only until `buf` changes, so
//             a command must copy anything it wants to keep.
//             `args` may be empty (blank inline line, "*0"): skip it.
// Incomplete: nothing consumed. Call again once more bytes have arrived.
//             Re-parsing from the start is cheap: header lines are short, and
//             bulk payloads are skipped by length, never scanned.
// Error:      `err` describes the problem.
ParseStatus parse_command(std::string_view buf, std::vector<std::string_view>& args,
                          size_t& consumed, std::string& err);

// Reply encoders: append one RESP value to `out`.
void add_simple(std::string& out, std::string_view s);  // must not contain \r\n
void add_error(std::string& out, std::string_view msg); // must not contain \r\n
void add_integer(std::string& out, long long n);
void add_bulk(std::string& out, std::string_view s);
void add_null(std::string& out);
void add_array_header(std::string& out, size_t n);

} // namespace resp
