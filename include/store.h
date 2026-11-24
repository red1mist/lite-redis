// The keyspace: an in-memory hash table from string keys to string values,
// plus key expiry.
//
// Single-threaded by design: only the event loop thread ever touches it, so
// there are no locks anywhere. Every operation runs to completion before the
// next command starts, which is what makes each command atomic for free.
//
// EXPIRY uses two complementary mechanisms:
//   1. Lazy deletion: every lookup checks the key's deadline; an expired key
//      is deleted on the spot and reported as missing. Cheap, and guarantees
//      clients NEVER see an expired key. But on its own, a key that is never
//      touched again would sit in memory forever.
//   2. Active expiry with a min-heap: all keys that have a TTL sit in a
//      binary min-heap ordered by deadline, so the soonest-expiring key is
//      always at index 0 (O(1) peek, O(log n) insert/remove). The event loop
//      asks for that deadline, sleeps in epoll_wait no longer than until
//      then, and on waking pops every key that is due.
//
// The heap is INDEXED: each Entry records its own position in the heap
// (heap_idx), kept up to date on every swap. So when DEL / SET / PERSIST /
// another EXPIRE changes a key's deadline, its heap node is found in O(1) and
// fixed or removed in O(log n). The alternative (leave stale nodes in the
// heap and skip them when popped) is simpler, but lets the heap grow without
// bound: a client calling EXPIRE on the same key in a loop would add a node
// per call.
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

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

constexpr int64_t NO_EXPIRY = -1;

struct Entry {
    std::string value;
    int64_t expire_at = NO_EXPIRY;      // deadline in ms (monotonic clock), or NO_EXPIRY
    size_t heap_idx = SIZE_MAX;         // position in the expiry heap; SIZE_MAX = not in it
};

class Store {
public:
    // A key/value pair as stored in the hash table. std::unordered_map
    // allocates one node per element and NEVER moves it (rehashing only
    // relinks buckets), so a Node* stays valid until that key is erased.
    // That's what lets the heap hold plain pointers into the table.
    using Node = std::pair<const std::string, Entry>;

    // Current time in ms, set by the event loop before each command (and
    // before each expiry cycle). Taking time as an input rather than reading
    // the clock inside makes the store deterministic and unit-testable with
    // a fake clock. All commands in one call see one consistent "now".
    void set_now(int64_t now_ms) { now_ = now_ms; }
    int64_t now() const { return now_; }

    // Live lookup with lazy expiry: an expired key is deleted and reported
    // missing. The pointer is valid until the next mutation of the store.
    Node* find(std::string_view key);

    // Insert or overwrite. Overwriting removes any TTL unless keep_ttl (that's
    // Redis's SET semantics; KEEPTTL keeps it). Copies key and value: the
    // arguments point into the connection's read buffer.
    Node* upsert(std::string_view key, std::string_view value, bool keep_ttl = false);

    // Delete a live key. Returns false if it didn't exist (or had expired).
    bool erase(std::string_view key);
    void erase(Node* n);

    // Give a key a deadline (absolute ms), or replace its existing one.
    void set_expire(Node* n, int64_t at_ms);
    // Remove a key's TTL. Returns true if it had one.
    bool clear_expire(Node* n);

    // Active expiry. next_expiry(): earliest deadline, or NO_EXPIRY if no key
    // has a TTL. expire_due(): delete up to `budget` keys whose deadline has
    // passed; returns true if more are still due (the caller should come back
    // soon rather than sleep). The budget keeps one iteration short even if
    // a million keys expire in the same millisecond.
    int64_t next_expiry() const { return heap_.empty() ? NO_EXPIRY : heap_[0].at; }
    bool expire_due(size_t budget);

    // Number of keys, including expired ones not yet removed (same as Redis's
    // DBSIZE; active expiry keeps that window small).
    size_t size() const { return map_.size(); }
    size_t keys_with_ttl() const { return heap_.size(); }
    size_t heap_capacity() const { return heap_.capacity(); }   // for tests
    size_t bucket_count() const { return map_.bucket_count(); } // for tests
    uint64_t expired_total() const { return expired_total_; }

    // Debug/test helper: verifies the heap property and every heap_idx.
    bool check_invariants() const;

private:
    bool expired(const Node& n) const {
        return n.second.expire_at != NO_EXPIRY && n.second.expire_at <= now_;
    }

    // Heap slot: the deadline is copied INTO the heap array next to the
    // pointer. Sifting compares deadlines dozens of times per operation; if
    // it had to follow each Node* into its separately allocated hash-table
    // node, nearly every comparison would be a cache miss (measured: ~2x
    // slower active expiry with 1M keys). This way comparisons scan one
    // contiguous array, and the node is touched only to update heap_idx.
    struct HeapItem {
        int64_t at;
        Node* node;
    };

    // Min-heap primitives over heap_. Every move goes through heap_place(),
    // which also records the new index in the entry.
    void heap_place(size_t i, HeapItem item) {
        heap_[i] = item;
        item.node->second.heap_idx = i;
    }
    // Give memory back after a mass expiry/deletion (see store.cpp).
    void maybe_shrink();
    void sift_up(size_t i);
    void sift_down(size_t i);
    void heap_insert(Node* n);
    void heap_remove(Node* n);

    // std::equal_to<> (no template argument) is the transparent comparator
    // that pairs with StringHash for string_view lookups.
    std::unordered_map<std::string, Entry, StringHash, std::equal_to<>> map_;
    std::vector<HeapItem> heap_;
    int64_t now_ = 0;
    uint64_t expired_total_ = 0;
};
