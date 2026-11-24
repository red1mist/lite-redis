#include "resp.h"

#include <charconv>

namespace resp {

namespace {

// Strict decimal integer: optional '-', then digits only, no overflow.
// (atoi/strtol would silently accept "12abc" or "" as a number.)
bool parse_int(std::string_view s, long long& out) {
    if (s.empty()) return false;
    const char* end = s.data() + s.size();
    auto [ptr, ec] = std::from_chars(s.data(), end, out);
    return ec == std::errc() && ptr == end;
}

// Inline commands: a plain text line like "PING\r\n" or "SET k v\n".
// Real Redis accepts these so you can poke a server with telnet/nc. We split
// on spaces/tabs (no quoting support).
ParseStatus parse_inline(std::string_view buf, std::vector<std::string_view>& args,
                         size_t& consumed, std::string& err) {
    size_t nl = buf.find('\n');
    if (nl == std::string_view::npos) {
        if (buf.size() > MAX_LINE_LEN) {
            err = "too big inline request";
            return ParseStatus::Error;
        }
        return ParseStatus::Incomplete;
    }
    std::string_view line = buf.substr(0, nl);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) i++;
        size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t') i++;
        if (i > start) args.push_back(line.substr(start, i - start));
    }
    consumed = nl + 1;
    return ParseStatus::Ok;
}

// Reads a "<prefix><integer>\r\n" header line starting at `pos`.
// On success advances `pos` past the CRLF.
ParseStatus read_header(std::string_view buf, size_t& pos, long long& value,
                        std::string& err, const char* what) {
    size_t eol = buf.find("\r\n", pos);
    if (eol == std::string_view::npos) {
        if (buf.size() - pos > MAX_LINE_LEN) {
            err = std::string("too big ") + what + " count string";
            return ParseStatus::Error;
        }
        return ParseStatus::Incomplete;
    }
    if (!parse_int(buf.substr(pos + 1, eol - pos - 1), value)) {
        err = std::string("invalid ") + what + " length";
        return ParseStatus::Error;
    }
    pos = eol + 2;
    return ParseStatus::Ok;
}

} // namespace

ParseStatus parse_command(std::string_view buf, std::vector<std::string_view>& args,
                          size_t& consumed, std::string& err) {
    args.clear();
    consumed = 0;
    if (buf.empty()) return ParseStatus::Incomplete;
    if (buf[0] != '*') return parse_inline(buf, args, consumed, err);

    size_t pos = 0;
    long long n;
    if (auto st = read_header(buf, pos, n, err, "multibulk"); st != ParseStatus::Ok) return st;
    if (n > static_cast<long long>(MAX_ARGS)) {
        err = "invalid multibulk length";
        return ParseStatus::Error;
    }
    if (n <= 0) { // "*0" / "*-1": an empty command. Redis silently skips it.
        consumed = pos;
        return ParseStatus::Ok;
    }

    for (long long i = 0; i < n; i++) {
        if (pos >= buf.size()) return ParseStatus::Incomplete;
        if (buf[pos] != '$') {
            err = std::string("expected '$', got '") + buf[pos] + "'";
            return ParseStatus::Error;
        }
        long long len;
        if (auto st = read_header(buf, pos, len, err, "bulk"); st != ParseStatus::Ok) return st;
        if (len < 0 || len > static_cast<long long>(MAX_BULK_LEN)) {
            err = "invalid bulk length";
            return ParseStatus::Error;
        }
        size_t ulen = static_cast<size_t>(len);
        // The payload is NOT scanned: we know its length, so we just check
        // whether all of it (plus the trailing CRLF) has arrived yet. That is
        // what makes values binary-safe and re-parsing cheap.
        if (buf.size() - pos < ulen + 2) return ParseStatus::Incomplete;
        if (buf[pos + ulen] != '\r' || buf[pos + ulen + 1] != '\n') {
            err = "expected CRLF after bulk string";
            return ParseStatus::Error;
        }
        args.push_back(buf.substr(pos, ulen));
        pos += ulen + 2;
    }
    consumed = pos;
    return ParseStatus::Ok;
}

void add_simple(std::string& out, std::string_view s) {
    out += '+';
    out += s;
    out += "\r\n";
}

void add_error(std::string& out, std::string_view msg) {
    out += '-';
    out += msg;
    out += "\r\n";
}

void add_integer(std::string& out, long long n) {
    out += ':';
    out += std::to_string(n);
    out += "\r\n";
}

void add_bulk(std::string& out, std::string_view s) {
    out += '$';
    out += std::to_string(s.size());
    out += "\r\n";
    out += s;
    out += "\r\n";
}

void add_null(std::string& out) { out += "$-1\r\n"; }

void add_array_header(std::string& out, size_t n) {
    out += '*';
    out += std::to_string(n);
    out += "\r\n";
}

} // namespace resp
