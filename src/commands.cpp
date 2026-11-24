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
};

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

void cmd_quit(const Args&, CommandContext& ctx) {
    resp::add_simple(ctx.out, "OK");
    ctx.close_after_reply = true;
}

// redis-cli (v7+) sends "COMMAND DOCS" as soon as it starts in interactive
// mode, to fetch command docs for its inline hints. An empty array is a valid
// answer ("no docs") and keeps the client happy.
void cmd_command(const Args&, CommandContext& ctx) { resp::add_array_header(ctx.out, 0); }

const std::unordered_map<std::string, CommandSpec> COMMANDS = {
    {"ping", {-1, cmd_ping}},
    {"echo", {2, cmd_echo}},
    {"quit", {1, cmd_quit}},
    {"command", {-1, cmd_command}},
};

std::string to_lower(std::string_view s) {
    std::string r(s);
    for (char& ch : r) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    return r;
}

// Same wording as real Redis, so clients and humans see familiar errors.
void unknown_command(const Args& args, CommandContext& ctx) {
    std::string msg = "ERR unknown command '";
    msg.append(args[0].substr(0, 128));
    msg += "', with args beginning with: ";
    for (size_t i = 1; i < args.size() && i <= 8; i++) {
        msg += '\'';
        msg.append(args[i].substr(0, 128));
        msg += "' ";
    }
    // An error reply is a single line: strip anything that would break it.
    for (char& ch : msg)
        if (ch == '\r' || ch == '\n') ch = ' ';
    resp::add_error(ctx.out, msg);
}

} // namespace

void execute_command(const Args& args, CommandContext& ctx) {
    std::string name = to_lower(args[0]);
    auto it = COMMANDS.find(name);
    if (it == COMMANDS.end()) {
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
