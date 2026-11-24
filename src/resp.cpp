#include "resp.h"

#include <algorithm>
#include <cctype>
#include <charconv>

namespace resp {

// (atoi/strtol would silently accept "12abc" or "" as a number.)
bool parse_integer(std::string_view s, long long& out) {
    if (s.empty()) return false;
    size_t digits_at = (s[0] == '-') ? 1 : 0;
    if (digits_at == s.size()) return false;
    if (s[digits_at] == '0' && s.size() != 1) return false; // leading zero / "-0"
    const char* end = s.data() + s.size();
    auto [ptr, ec] = std::from_chars(s.data(), end, out);
    return ec == std::errc() && ptr == end;
}

namespace {

bool is_hex(char c) { return std::isxdigit(static_cast<unsigned char>(c)) != 0; }
int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    return std::tolower(static_cast<unsigned char>(c)) - 'a' + 10;
}

// Splits an inline command line into args the way Redis's sdssplitargs does:
//   - whitespace separates args
//   - "double quotes" allow spaces and escapes: \n \r \t \b \a \xHH, \<other>
//   - 'single quotes' allow spaces; only \' is an escape
//   - a closing quote must be followed by whitespace or end of line
// Returns false on unbalanced quotes.
bool split_inline(std::string_view line, std::vector<std::string>& out) {
    size_t i = 0, n = line.size();
    while (true) {
        while (i < n && std::isspace(static_cast<unsigned char>(line[i]))) i++;
        if (i == n) return true;

        std::string cur;
        bool inq = false, insq = false, done = false;
        while (!done) {
            if (inq) {
                if (i == n) return false; // unterminated "
                if (line[i] == '\\' && i + 3 < n && line[i + 1] == 'x' &&
                    is_hex(line[i + 2]) && is_hex(line[i + 3])) {
                    cur += static_cast<char>(hex_val(line[i + 2]) * 16 + hex_val(line[i + 3]));
                    i += 3;
                } else if (line[i] == '\\' && i + 1 < n) {
                    i++;
                    switch (line[i]) {
                        case 'n': cur += '\n'; break;
                        case 'r': cur += '\r'; break;
                        case 't': cur += '\t'; break;
                        case 'b': cur += '\b'; break;
                        case 'a': cur += '\a'; break;
                        default: cur += line[i]; break;
                    }
                } else if (line[i] == '"') {
                    // closing quote must be followed by a space or nothing
                    if (i + 1 < n && !std::isspace(static_cast<unsigned char>(line[i + 1])))
                        return false;
                    done = true;
                } else {
                    cur += line[i];
                }
            } else if (insq) {
                if (i == n) return false; // unterminated '
                if (line[i] == '\\' && i + 1 < n && line[i + 1] == '\'') {
                    i++;
                    cur += '\'';
                } else if (line[i] == '\'') {
                    if (i + 1 < n && !std::isspace(static_cast<unsigned char>(line[i + 1])))
                        return false;
                    done = true;
                } else {
                    cur += line[i];
                }
            } else {
                if (i == n) break;
                switch (line[i]) {
                    case ' ': case '\n': case '\r': case '\t': done = true; break;
                    case '"': inq = true; break;   // a quote can start mid-token,
                    case '\'': insq = true; break; // exactly as in Redis
                    default: cur += line[i]; break;
                }
            }
            if (i < n) i++;
        }
        out.push_back(std::move(cur));
    }
}

// Inline commands: a plain text line like "PING\r\n" or SET k "a b"\n.
// Real Redis accepts these so you can poke a server with telnet/nc.
ParseStatus parse_inline(std::string_view buf, ParseState& st, std::vector<std::string_view>& args,
                         size_t& consumed, std::string& err) {
    // Only look for the newline within the first MAX_INLINE_LEN + 1 bytes,
    // resuming where the last search stopped (st.scanned).
    size_t limit = std::min(buf.size(), MAX_INLINE_LEN + 1);
    size_t nl = buf.substr(0, limit).find('\n', st.scanned);
    if (nl == std::string_view::npos) {
        if (limit > MAX_INLINE_LEN) {
            err = "too big inline request";
            return ParseStatus::Error;
        }
        st.scanned = limit;
        return ParseStatus::Incomplete;
    }
    std::string_view line = buf.substr(0, nl);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    // A NUL byte ends the line's content. (Intentional difference: Redis
    // searches for the newline with strchr, which stops AT the NUL, so it
    // never sees the newline and waits for more data. We run the part before
    // the NUL instead of stalling.)
    line = line.substr(0, std::min(line.size(), line.find('\0')));

    st.inline_args.clear();
    if (!split_inline(line, st.inline_args)) {
        err = "unbalanced quotes in request";
        return ParseStatus::Error;
    }
    // Build views only now: inline_args is complete, so its strings won't move.
    for (const std::string& a : st.inline_args) args.push_back(a);
    consumed = nl + 1;
    st.reset();
    return ParseStatus::Ok;
}

// Reads a "<prefix><integer>\r\n" header line starting at `pos`.
// On success advances `pos` past the CRLF; otherwise leaves it unchanged.
// `scanned` remembers how far a previous call already searched for the CRLF
// (so a header trickling in isn't re-scanned from its start every read).
// `what` names it in "invalid <what> length"; `big` in "too big <big> count
// string" (Redis's exact wording: "multibulk" vs "mbulk").
//
// Intentional difference: we require a real "\r\n". Redis finds the first
// '\r' and skips whatever single byte follows it.
ParseStatus read_header(std::string_view buf, size_t& pos, size_t& scanned, long long& value,
                        std::string& err, const char* what, const char* big) {
    // The CRLF must start within MAX_INLINE_LEN bytes of the header start.
    size_t limit = std::min(buf.size(), pos + MAX_INLINE_LEN + 2);
    size_t from = std::max(pos, scanned > 0 ? scanned - 1 : 0); // a '\r' may end the last scan
    size_t eol = buf.substr(0, limit).find("\r\n", from);
    if (eol == std::string_view::npos) {
        if (limit - pos > MAX_INLINE_LEN + 1) {
            err = std::string("too big ") + big + " count string";
            return ParseStatus::Error;
        }
        scanned = limit;
        return ParseStatus::Incomplete;
    }
    scanned = 0;
    if (!parse_integer(buf.substr(pos + 1, eol - pos - 1), value)) {
        err = std::string("invalid ") + what + " length";
        return ParseStatus::Error;
    }
    pos = eol + 2;
    return ParseStatus::Ok;
}

} // namespace

ParseStatus parse_command(std::string_view buf, ParseState& st,
                          std::vector<std::string_view>& args, size_t& consumed,
                          std::string& err) {
    args.clear();
    consumed = 0;
    if (buf.empty()) return ParseStatus::Incomplete;
    if (buf[0] != '*') return parse_inline(buf, st, args, consumed, err);

    size_t pos = st.pos; // resume where the previous call stopped
    if (st.remaining < 0) {
        long long n;
        if (auto r = read_header(buf, pos, st.scanned, n, err, "multibulk", "mbulk"); r != ParseStatus::Ok)
            return r;
        if (n > static_cast<long long>(MAX_ARGS)) {
            err = "invalid multibulk length";
            return ParseStatus::Error;
        }
        if (n <= 0) { // "*0" / "*-1": an empty command. Redis silently skips it.
            consumed = pos;
            st.reset();
            return ParseStatus::Ok;
        }
        st.remaining = n;
        st.pos = pos;
    }

    while (st.remaining > 0) {
        if (pos >= buf.size()) return ParseStatus::Incomplete;
        if (buf[pos] != '$') {
            err = std::string("expected '$', got '") + buf[pos] + "'";
            return ParseStatus::Error;
        }
        long long len;
        if (auto r = read_header(buf, pos, st.scanned, len, err, "bulk", "bulk"); r != ParseStatus::Ok)
            return r; // header incomplete: st.pos still points at this '$'
        if (len < 0 || len > static_cast<long long>(MAX_BULK_LEN)) {
            err = "invalid bulk length";
            return ParseStatus::Error;
        }
        size_t ulen = static_cast<size_t>(len);
        // The payload is NOT scanned: we know its length, so we just check
        // whether all of it (plus the trailing CRLF) has arrived yet. That is
        // what makes values binary-safe and waiting for a big value O(1).
        // If it hasn't all arrived, st.pos still points at this arg's '$'
        // header, so next time we re-read only this one short header line.
        if (buf.size() - pos < ulen + 2) return ParseStatus::Incomplete;
        if (buf[pos + ulen] != '\r' || buf[pos + ulen + 1] != '\n') {
            err = "expected CRLF after bulk string";
            return ParseStatus::Error;
        }
        st.spans.emplace_back(pos, ulen);
        pos += ulen + 2;
        st.pos = pos;      // this arg is done: never look at its bytes again
        st.remaining--;
    }

    // Complete: turn the recorded offsets into views over the final buffer.
    for (auto [off, len] : st.spans) args.push_back(buf.substr(off, len));
    consumed = pos;
    st.reset();
    return ParseStatus::Ok;
}

void add_simple(std::string& out, std::string_view s) {
    out += '+';
    out += s;
    out += "\r\n";
}

void add_error(std::string& out, std::string_view msg) {
    out += '-';
    size_t start = out.size();
    out += msg;
    // An error reply is ONE line. Messages can echo client input (unknown
    // command names, a bad byte in a protocol error), and a CR or LF in there
    // would end the line early and let the rest be parsed as a separate,
    // forged reply. Neutralize them.
    // (NUL too: clients that treat error text as a C string would truncate.)
    for (size_t i = start; i < out.size(); i++)
        if (out[i] == '\r' || out[i] == '\n' || out[i] == '\0') out[i] = ' ';
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
