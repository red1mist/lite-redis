// Command dispatch: turns a parsed argument list into a RESP reply.
#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "store.h"

using Args = std::vector<std::string_view>;

struct CommandContext {
    std::string& out;               // reply bytes are appended here (conn's wbuf)
    Store& store;                   // the keyspace
    bool close_after_reply = false; // set by QUIT
};

// args[0] is the command name (case-insensitive). Always appends exactly one
// reply to ctx.out, an error reply if the command is unknown or malformed.
void execute_command(const Args& args, CommandContext& ctx);
