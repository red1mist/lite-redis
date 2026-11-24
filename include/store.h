// The keyspace: an in-memory hash table from string keys to string values.
//
// Single-threaded by design: only the event loop thread ever touches it, so
// there are no locks anywhere. Every operation runs to completion before the
// next command starts, which is what makes each command atomic for free.
#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>

// "Transparent" hash: lets find() take a std::string_view directly (C++20
// heterogeneous lookup) instead of forcing us to build a temporary
// std::string from the parsed argument. That matters on the hot path:
// redis-benchmark's keys ("key:000000012345") are 16 bytes, one more than
// std::string's small-string buffer (15), so every GET would otherwise do a
// heap allocation + free just to look a key up.
struct StringHash {
    using is_transparent = void;
    size_t operator()(std::string_view s) const noexcept {
        return std::hash<std::string_view>{}(s);
    }
};

struct Entry {
    std::string value;
    // Stage 5 adds the key's expiry time here.
};

class Store {
public:
    // Returns the value, or nullptr if the key doesn't exist. The pointer is
    // only valid until the next mutation of the store (a rehash moves nothing
    // in std::unordered_map, but erase/overwrite would invalidate it).
    const std::string* get(std::string_view key) const;

    // Insert or overwrite. Copies key and value: the arguments point into the
    // connection's read buffer, which is reused for the next command.
    void set(std::string_view key, std::string_view value);

    // Returns true if the key existed.
    bool del(std::string_view key);

    bool exists(std::string_view key) const;
    size_t size() const { return map_.size(); }

private:
    // std::equal_to<> (no template argument) is the transparent comparator
    // that pairs with StringHash for string_view lookups.
    std::unordered_map<std::string, Entry, StringHash, std::equal_to<>> map_;
};
