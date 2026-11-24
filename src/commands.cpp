#include "commands.h"

#include <cctype>
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

// GET key  ->  bulk string, or null ($-1) if the key doesn't exist.
void cmd_get(const Args& args, CommandContext& ctx) {
    const std::string* v = ctx.store.get(args[1]);
    if (v) resp::add_bulk(ctx.out, *v);
    else resp::add_null(ctx.out);
}

// SET key value [NX | XX]
//   NX: only set if the key does NOT exist.  XX: only if it DOES exist.
//   Reply: +OK, or null if NX/XX prevented the write.
// (EX / PX expiry options arrive with Stage 5.)
void cmd_set(const Args& args, CommandContext& ctx) {
    bool nx = false, xx = false;
    for (size_t i = 3; i < args.size(); i++) {
        if (iequals(args[i], "nx")) nx = true;
        else if (iequals(args[i], "xx")) xx = true;
        else {
            resp::add_error(ctx.out, "ERR syntax error");
            return;
        }
    }
    if (nx && xx) { // contradictory; real Redis rejects this too
        resp::add_error(ctx.out, "ERR syntax error");
        return;
    }
    if (nx || xx) {
        bool exists = ctx.store.exists(args[1]);
        if ((nx && exists) || (xx && !exists)) {
            resp::add_null(ctx.out);
            return;
        }
    }
    ctx.store.set(args[1], args[2]);
    resp::add_simple(ctx.out, "OK");
}

// DEL key [key ...]  ->  integer: how many keys were actually removed.
// "DEL k k" counts k once: the second delete finds nothing.
void cmd_del(const Args& args, CommandContext& ctx) {
    long long removed = 0;
    for (size_t i = 1; i < args.size(); i++)
        if (ctx.store.del(args[i])) removed++;
    resp::add_integer(ctx.out, removed);
}

// EXISTS key [key ...]  ->  integer: how many of the given keys exist.
// Unlike DEL, a repeated key is counted every time ("EXISTS k k" -> 2),
// matching real Redis.
void cmd_exists(const Args& args, CommandContext& ctx) {
    long long n = 0;
    for (size_t i = 1; i < args.size(); i++)
        if (ctx.store.exists(args[i])) n++;
    resp::add_integer(ctx.out, n);
}

// DBSIZE  ->  number of keys.
void cmd_dbsize(const Args&, CommandContext& ctx) {
    resp::add_integer(ctx.out, static_cast<long long>(ctx.store.size()));
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
    // No command name is longer than 7 chars; don't lowercase-copy a 100 MB
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
