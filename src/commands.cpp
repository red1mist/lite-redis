#include "commands.h"

#include <cctype>
#include <climits>
#include <unordered_map>

#include "resp.h"

namespace {

using Handler = void (*)(const Args&, CommandContext&);

struct CommandSpec {
    // Redis-style arity, counting the command name itself:
    //   N > 0  exactly N args     (ECHO msg      -> 2)
    //   N < 0  at least -N args   (PING [msg]    -> -1)
    int arity;
    Handler handler;
    // Which args are keys (for COMMAND INFO): first, last (-1 = last arg),
    // step. 0,0,0 = no keys. Cluster-aware clients use this for routing.
    int first_key = 0, last_key = 0, key_step = 0;
};

const std::unordered_map<std::string, CommandSpec>& command_table();

// ASCII case-insensitive compare, for keywords like NX / xx / GET.
bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); i++)
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i])))
            return false;
    return true;
}

std::string to_lower(std::string_view s) {
    std::string r(s);
    for (char& ch : r) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    return r;
}

void unknown_subcommand(CommandContext& ctx, std::string_view sub, const char* cmd) {
    std::string msg = "ERR unknown subcommand '";
    msg.append(sub.substr(0, 128));
    msg += "'. Try ";
    msg += cmd;
    msg += " HELP.";
    resp::add_error(ctx.out, msg);
}

// ---------------------------------------------------------------------------
// Connection / server commands
// ---------------------------------------------------------------------------

void cmd_ping(const Args& args, CommandContext& ctx) {
    if (args.size() > 2) {
        resp::add_error(ctx.out, "ERR wrong number of arguments for 'ping' command");
    } else if (args.size() == 2) {
        resp::add_bulk(ctx.out, args[1]);
    } else {
        resp::add_simple(ctx.out, "PONG");
    }
}

void cmd_echo(const Args& args, CommandContext& ctx) { resp::add_bulk(ctx.out, args[1]); }

// QUIT ignores any arguments (arity -1), like Redis 7.
void cmd_quit(const Args&, CommandContext& ctx) {
    resp::add_simple(ctx.out, "OK");
    ctx.close_after_reply = true;
}

// COMMAND [DOCS | COUNT | INFO name...]
// redis-cli (v7+) sends "COMMAND DOCS" as soon as it starts in interactive
// mode, to fetch docs for its inline hints. An empty array is a valid answer
// ("no docs"). COUNT and INFO are used by some client libraries at connect
// time, so they must at least return the right reply TYPES.
void cmd_command(const Args& args, CommandContext& ctx) {
    if (args.size() == 1 || iequals(args[1], "docs")) {
        resp::add_array_header(ctx.out, 0);
    } else if (iequals(args[1], "count")) {
        resp::add_integer(ctx.out, static_cast<long long>(command_table().size()));
    } else if (iequals(args[1], "info")) {
        // One entry per requested name, in the Redis 7 RESP2 layout:
        // [name, arity, flags, first key, last key, step, acl categories,
        //  tips, key specs, subcommands]; nil for unknown commands.
        resp::add_array_header(ctx.out, args.size() - 2);
        for (size_t i = 2; i < args.size(); i++) {
            std::string name = to_lower(args[i]);
            auto it = command_table().find(name);
            if (it == command_table().end()) {
                resp::add_null(ctx.out);
                continue;
            }
            const CommandSpec& s = it->second;
            resp::add_array_header(ctx.out, 10);
            resp::add_bulk(ctx.out, name);
            resp::add_integer(ctx.out, s.arity);
            resp::add_array_header(ctx.out, 0);
            resp::add_integer(ctx.out, s.first_key);
            resp::add_integer(ctx.out, s.last_key);
            resp::add_integer(ctx.out, s.key_step);
            for (int k = 0; k < 4; k++) resp::add_array_header(ctx.out, 0);
        }
    } else {
        unknown_subcommand(ctx, args[1], "COMMAND");
    }
}

// CONFIG GET parameter [parameter ...]
// We have no runtime config, but redis-benchmark asks for "save" and
// "appendonly" before every run and prints "WARNING: Could not fetch server
// CONFIG" unless it gets a [name, value] array back. Report the truth: we
// never persist anything. Reply is a flat array of name/value pairs; unknown
// parameters are simply absent (same as Redis). "*" matches everything.
void cmd_config(const Args& args, CommandContext& ctx) {
    if (!iequals(args[1], "get")) {
        unknown_subcommand(ctx, args[1], "CONFIG");
        return;
    }
    if (args.size() < 3) {
        resp::add_error(ctx.out, "ERR wrong number of arguments for 'config|get' command");
        return;
    }
    static const std::pair<const char*, const char*> PARAMS[] = {
        {"save", ""},          // no RDB snapshots
        {"appendonly", "no"},  // no AOF
    };
    std::vector<std::pair<const char*, const char*>> matched;
    for (const auto& p : PARAMS) {
        for (size_t i = 2; i < args.size(); i++) {
            if (args[i] == "*" || iequals(args[i], p.first)) {
                matched.push_back(p);
                break; // report each parameter once
            }
        }
    }
    resp::add_array_header(ctx.out, matched.size() * 2);
    for (const auto& [name, value] : matched) {
        resp::add_bulk(ctx.out, name);
        resp::add_bulk(ctx.out, value);
    }
}

// ---------------------------------------------------------------------------
// Keyspace commands
// ---------------------------------------------------------------------------

const char* const NOT_AN_INTEGER = "ERR value is not an integer or out of range";

void invalid_expire_time(CommandContext& ctx, const char* cmd) {
    resp::add_error(ctx.out, std::string("ERR invalid expire time in '") + cmd + "' command");
}

// GET key  ->  bulk string, or null ($-1) if the key doesn't exist.
void cmd_get(const Args& args, CommandContext& ctx) {
    Store::Node* n = ctx.store.find(args[1]); // lazily deletes it if expired
    if (n) resp::add_bulk(ctx.out, n->second.value);
    else resp::add_null(ctx.out);
}

// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | KEEPTTL]
//   NX / XX:  only set if the key does NOT / DOES exist (else reply null).
//   GET:      reply with the OLD value (or null) instead of OK.
//   EX / PX:  give the key a TTL.   KEEPTTL: keep its existing TTL.
//   Without KEEPTTL, overwriting a key REMOVES its TTL (Redis semantics).
// Parsing order and error precedence follow Redis 7: option syntax first,
// then the expire value, then GET, then the NX/XX check.
// (Not supported: EXAT / PXAT absolute deadlines -> syntax error.)
void cmd_set(const Args& args, CommandContext& ctx) {
    bool nx = false, xx = false, get = false, keepttl = false;
    enum { NONE, EX, PX } unit = NONE;
    std::string_view expire_arg;
    for (size_t i = 3; i < args.size(); i++) {
        std::string_view opt = args[i];
        bool has_next = i + 1 < args.size();
        if (iequals(opt, "nx") && !xx) nx = true;
        else if (iequals(opt, "xx") && !nx) xx = true;
        else if (iequals(opt, "get")) get = true;
        else if (iequals(opt, "keepttl") && unit == NONE) keepttl = true;
        else if (iequals(opt, "ex") && !keepttl && unit != PX && has_next) {
            unit = EX;
            expire_arg = args[++i];
        } else if (iequals(opt, "px") && !keepttl && unit != EX && has_next) {
            unit = PX;
            expire_arg = args[++i];
        } else {
            resp::add_error(ctx.out, "ERR syntax error");
            return;
        }
    }

    int64_t deadline = NO_EXPIRY;
    if (unit != NONE) {
        long long ttl;
        if (!resp::parse_integer(expire_arg, ttl)) {
            resp::add_error(ctx.out, NOT_AN_INTEGER);
            return;
        }
        // Unlike EXPIRE, SET rejects non-positive TTLs. Overflow-check the
        // unit conversion and the addition of "now", as Redis does.
        if (ttl <= 0 || (unit == EX && ttl > LLONG_MAX / 1000)) {
            invalid_expire_time(ctx, "set");
            return;
        }
        if (unit == EX) ttl *= 1000;
        if (ttl > LLONG_MAX - ctx.store.now()) {
            invalid_expire_time(ctx, "set");
            return;
        }
        deadline = ctx.store.now() + ttl;
    }

    Store::Node* existing = ctx.store.find(args[1]);
    if (get) {
        if (existing) resp::add_bulk(ctx.out, existing->second.value);
        else resp::add_null(ctx.out);
    }
    if ((nx && existing) || (xx && !existing)) {
        if (!get) resp::add_null(ctx.out); // with GET, the old value was the reply
        return;
    }
    Store::Node* n = ctx.store.upsert(args[1], args[2], keepttl);
    if (deadline != NO_EXPIRY) ctx.store.set_expire(n, deadline);
    if (!get) resp::add_simple(ctx.out, "OK");
}

// DEL key [key ...]  ->  integer: how many keys were actually removed.
// "DEL k k" counts k once; an already-expired key counts as not there.
void cmd_del(const Args& args, CommandContext& ctx) {
    long long removed = 0;
    for (size_t i = 1; i < args.size(); i++)
        if (ctx.store.erase(args[i])) removed++;
    resp::add_integer(ctx.out, removed);
}

// EXISTS key [key ...]  ->  integer: how many of the given keys exist.
// Unlike DEL, a repeated key is counted every time ("EXISTS k k" -> 2),
// matching real Redis.
void cmd_exists(const Args& args, CommandContext& ctx) {
    long long n = 0;
    for (size_t i = 1; i < args.size(); i++)
        if (ctx.store.find(args[i])) n++;
    resp::add_integer(ctx.out, n);
}

// DBSIZE  ->  number of keys (like Redis, may include expired keys that
// haven't been removed yet; active expiry keeps that window ~1 ms).
void cmd_dbsize(const Args&, CommandContext& ctx) {
    resp::add_integer(ctx.out, static_cast<long long>(ctx.store.size()));
}

// EXPIRE key seconds [NX | XX | GT | LT]     (PEXPIRE: milliseconds)
//   -> 1 if the timeout was set (or the key was deleted because the TTL was
//      <= 0), 0 if the key doesn't exist or an option condition failed.
//   NX: only if the key has no TTL.   XX: only if it has one.
//   GT: only if the new deadline is later.   LT: only if it's sooner.
//   (A key without a TTL counts as an INFINITE TTL for GT/LT.)
void expire_generic(const Args& args, CommandContext& ctx, long long unit_ms, const char* cmd) {
    bool nx = false, xx = false, gt = false, lt = false;
    for (size_t i = 3; i < args.size(); i++) {
        if (iequals(args[i], "nx")) nx = true;
        else if (iequals(args[i], "xx")) xx = true;
        else if (iequals(args[i], "gt")) gt = true;
        else if (iequals(args[i], "lt")) lt = true;
        else {
            std::string msg = "ERR Unsupported option ";
            msg.append(args[i]);
            resp::add_error(ctx.out, msg);
            return;
        }
    }
    if (nx && (xx || gt || lt)) {
        resp::add_error(ctx.out, "ERR NX and XX, GT or LT options at the same time are not compatible");
        return;
    }
    if (gt && lt) {
        resp::add_error(ctx.out, "ERR GT and LT options at the same time are not compatible");
        return;
    }

    long long when;
    if (!resp::parse_integer(args[2], when)) {
        resp::add_error(ctx.out, NOT_AN_INTEGER);
        return;
    }
    // Negative TTLs are allowed (they delete the key), but the conversion to
    // an absolute ms deadline must not overflow.
    if (unit_ms == 1000) {
        if (when > LLONG_MAX / 1000 || when < LLONG_MIN / 1000) {
            invalid_expire_time(ctx, cmd);
            return;
        }
        when *= 1000;
    }
    if (when > LLONG_MAX - ctx.store.now()) {
        invalid_expire_time(ctx, cmd);
        return;
    }
    when += ctx.store.now();

    Store::Node* n = ctx.store.find(args[1]);
    if (!n) {
        resp::add_integer(ctx.out, 0);
        return;
    }
    int64_t current = n->second.expire_at; // NO_EXPIRY (-1) = infinite
    if ((nx && current != NO_EXPIRY) || (xx && current == NO_EXPIRY) ||
        (gt && (current == NO_EXPIRY || when <= current)) ||
        (lt && current != NO_EXPIRY && when >= current)) {
        resp::add_integer(ctx.out, 0);
        return;
    }
    if (when <= ctx.store.now()) {
        ctx.store.erase(n); // deadline already passed: delete right away
    } else {
        ctx.store.set_expire(n, when);
    }
    resp::add_integer(ctx.out, 1);
}

void cmd_expire(const Args& args, CommandContext& ctx) { expire_generic(args, ctx, 1000, "expire"); }
void cmd_pexpire(const Args& args, CommandContext& ctx) { expire_generic(args, ctx, 1, "pexpire"); }

// TTL key  ->  seconds left (rounded), -1 if the key has no TTL, -2 if the
// key doesn't exist.   PTTL: the same in milliseconds.
void ttl_generic(const Args& args, CommandContext& ctx, bool in_ms) {
    Store::Node* n = ctx.store.find(args[1]);
    if (!n) {
        resp::add_integer(ctx.out, -2);
        return;
    }
    if (n->second.expire_at == NO_EXPIRY) {
        resp::add_integer(ctx.out, -1);
        return;
    }
    long long left = n->second.expire_at - ctx.store.now();
    if (left < 0) left = 0;
    resp::add_integer(ctx.out, in_ms ? left : (left + 500) / 1000); // round like Redis
}

void cmd_ttl(const Args& args, CommandContext& ctx) { ttl_generic(args, ctx, false); }
void cmd_pttl(const Args& args, CommandContext& ctx) { ttl_generic(args, ctx, true); }

// PERSIST key  ->  1 if a TTL was removed, 0 if the key is missing or had none.
void cmd_persist(const Args& args, CommandContext& ctx) {
    Store::Node* n = ctx.store.find(args[1]);
    resp::add_integer(ctx.out, (n && ctx.store.clear_expire(n)) ? 1 : 0);
}

const std::unordered_map<std::string, CommandSpec>& command_table() {
    //                         arity  handler      first last step
    static const std::unordered_map<std::string, CommandSpec> table = {
        {"ping",    {-1, cmd_ping}},
        {"echo",    {2,  cmd_echo}},
        {"quit",    {-1, cmd_quit}},
        {"command", {-1, cmd_command}},
        {"config",  {-2, cmd_config}},
        {"get",     {2,  cmd_get,      1, 1, 1}},
        {"set",     {-3, cmd_set,      1, 1, 1}},
        {"del",     {-2, cmd_del,      1, -1, 1}},
        {"exists",  {-2, cmd_exists,   1, -1, 1}},
        {"dbsize",  {1,  cmd_dbsize}},
        {"expire",  {-3, cmd_expire,   1, 1, 1}},
        {"pexpire", {-3, cmd_pexpire,  1, 1, 1}},
        {"ttl",     {2,  cmd_ttl,      1, 1, 1}},
        {"pttl",    {2,  cmd_pttl,     1, 1, 1}},
        {"persist", {2,  cmd_persist,  1, 1, 1}},
    };
    return table;
}

// Same wording as Redis 7: the args part shares ONE ~128-byte budget (each
// arg is cut to whatever room is left), so the message stays short no matter
// how many or how long the args are. CR/LF are neutralized by add_error.
// Redis formats these with printf's %s, which stops at a NUL byte, so each
// name/arg is cut at its first NUL before the length limits apply.
std::string_view until_nul(std::string_view s) { return s.substr(0, s.find('\0')); }

void unknown_command(const Args& args, CommandContext& ctx) {
    std::string arglist;
    for (size_t i = 1; i < args.size() && arglist.size() < 128; i++) {
        size_t room = 128 - arglist.size();
        arglist += '\'';
        arglist.append(until_nul(args[i]).substr(0, room));
        arglist += "' ";
    }
    std::string msg = "ERR unknown command '";
    msg.append(until_nul(args[0]).substr(0, 128));
    msg += "', with args beginning with: ";
    msg += arglist;
    resp::add_error(ctx.out, msg);
}

} // namespace

void execute_command(const Args& args, CommandContext& ctx) {
    // No command name is longer than 7 chars ("command", "pexpire"); don't lowercase-copy a 100 MB
    // garbage "name" just to find out it's unknown.
    if (args[0].size() > 16) {
        unknown_command(args, ctx);
        return;
    }
    // Short enough to fit std::string's inline (SSO) buffer: no heap allocation.
    std::string name = to_lower(args[0]);
    auto it = command_table().find(name);
    if (it == command_table().end()) {
        unknown_command(args, ctx);
        return;
    }
    const CommandSpec& spec = it->second;
    int argc = static_cast<int>(args.size());
    if ((spec.arity > 0 && argc != spec.arity) || (spec.arity < 0 && argc < -spec.arity)) {
        resp::add_error(ctx.out, "ERR wrong number of arguments for '" + name + "' command");
        return;
    }
    spec.handler(args, ctx);
}
