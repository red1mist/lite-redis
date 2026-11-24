// Unit test for Store (keyspace + expiry heap), driven by a FAKE clock.
//
// Because Store takes "now" as an input instead of reading the clock, we can
// run hundreds of thousands of random operations in a few hundred ms, with
// precise control over time, and compare every observable result against a
// dead-simple reference model (std::map + brute force). After every single
// operation we also verify the heap property and that every entry's
// heap_idx points back at itself.
//
//   ./dev make unit        (built with AddressSanitizer + UBSan)
#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <string>

#include "store.h"

namespace {

int failures = 0;

#define CHECK(cond, ...)                                                    \
    do {                                                                    \
        if (!(cond)) {                                                      \
            std::fprintf(stderr, "FAIL %s:%d: %s: ", __FILE__, __LINE__, #cond); \
            std::fprintf(stderr, __VA_ARGS__);                              \
            std::fprintf(stderr, "\n");                                     \
            if (++failures > 20) std::exit(1);                              \
        }                                                                   \
    } while (0)

struct ModelEntry {
    std::string value;
    int64_t expire_at; // NO_EXPIRY or absolute ms
};

// Reference model: a key is visible iff present and not past its deadline.
struct Model {
    std::map<std::string, ModelEntry> m;
    int64_t now = 0;
    const ModelEntry* find(const std::string& k) {
        auto it = m.find(k);
        if (it == m.end()) return nullptr;
        if (it->second.expire_at != NO_EXPIRY && it->second.expire_at <= now) {
            m.erase(it);
            return nullptr;
        }
        return &it->second;
    }
};

void test_basic_semantics() {
    Store s;
    s.set_now(1000);
    CHECK(s.find("a") == nullptr, "missing key");
    Store::Node* n = s.upsert("a", "1");
    CHECK(n && n->second.value == "1", "insert");
    s.set_expire(n, 1100);
    CHECK(s.next_expiry() == 1100, "next_expiry %lld", (long long)s.next_expiry());
    s.set_now(1099);
    CHECK(s.find("a") != nullptr, "not yet expired at 1099");
    s.set_now(1100);
    CHECK(s.find("a") == nullptr, "expired exactly at deadline (lazy)");
    CHECK(s.size() == 0 && s.keys_with_ttl() == 0, "lazy delete removed it from map AND heap");

    // Overwrite clears TTL; KEEPTTL keeps it.
    n = s.upsert("b", "x");
    s.set_expire(n, 5000);
    s.upsert("b", "y");
    CHECK(s.find("b")->second.expire_at == NO_EXPIRY, "SET clears TTL");
    CHECK(s.keys_with_ttl() == 0, "and removes it from the heap");
    s.set_expire(s.find("b"), 5000);
    s.upsert("b", "z", /*keep_ttl=*/true);
    CHECK(s.find("b")->second.expire_at == 5000, "KEEPTTL keeps TTL");

    // Active expiry deletes due keys without anyone touching them.
    for (int i = 0; i < 10; i++) s.set_expire(s.upsert("k" + std::to_string(i), "v"), 2000 + i);
    s.set_now(2004);
    CHECK(!s.expire_due(100), "no more due");
    CHECK(s.size() == 1 + 5, "5 of 10 expired (+ key b): size=%zu", s.size());
    CHECK(s.next_expiry() == 2005, "next is k5");
    CHECK(s.check_invariants(), "invariants");

    // Budget: more due than allowed -> reports more_due.
    s.set_now(10000);
    CHECK(s.expire_due(2), "budget 2 with 6 due -> more");
    CHECK(s.check_invariants(), "invariants after partial expiry");
    while (s.expire_due(2)) {}
    CHECK(s.size() == 0 && s.keys_with_ttl() == 0, "all gone: size=%zu", s.size());
}

void test_randomized_against_model(uint32_t seed, int ops, int keyspace) {
    std::mt19937 rng(seed);
    auto rnd = [&](int lo, int hi) { return std::uniform_int_distribution<int>(lo, hi)(rng); };
    Store s;
    Model model;
    int64_t now = 1'000'000;
    s.set_now(now);
    model.now = now;

    for (int op = 0; op < ops; op++) {
        std::string key = "k" + std::to_string(rnd(0, keyspace - 1));
        int kind = rnd(0, 99);
        if (kind < 25) { // SET [KEEPTTL]
            std::string val = "v" + std::to_string(op);
            bool keep = rnd(0, 3) == 0;
            s.upsert(key, val, keep);
            const ModelEntry* old = model.find(key);
            int64_t exp = (keep && old) ? old->expire_at : NO_EXPIRY;
            model.m[key] = {val, exp};
        } else if (kind < 45) { // EXPIRE with a deadline in [-20, +200) ms
            int64_t at = now + rnd(-20, 199);
            Store::Node* n = s.find(key);
            const ModelEntry* me = model.find(key);
            CHECK((n != nullptr) == (me != nullptr), "existence before EXPIRE, key %s", key.c_str());
            if (n) {
                if (at <= now) {
                    s.erase(n);
                    model.m.erase(key);
                } else {
                    s.set_expire(n, at);
                    model.m[key].expire_at = at;
                }
            }
        } else if (kind < 52) { // PERSIST
            Store::Node* n = s.find(key);
            const ModelEntry* me = model.find(key);
            bool had = n && s.clear_expire(n);
            CHECK(had == (me && me->expire_at != NO_EXPIRY), "PERSIST result, key %s", key.c_str());
            if (me) model.m[key].expire_at = NO_EXPIRY;
        } else if (kind < 62) { // DEL
            bool removed = s.erase(key);
            bool model_had = model.find(key) != nullptr;
            CHECK(removed == model_had, "DEL result, key %s", key.c_str());
            model.m.erase(key);
        } else if (kind < 85) { // GET
            Store::Node* n = s.find(key);
            const ModelEntry* me = model.find(key);
            CHECK((n != nullptr) == (me != nullptr), "GET existence, key %s", key.c_str());
            if (n && me) {
                CHECK(n->second.value == me->value, "GET value, key %s", key.c_str());
                CHECK(n->second.expire_at == me->expire_at, "TTL, key %s", key.c_str());
            }
        } else if (kind < 95) { // time passes
            now += rnd(0, 15);
            s.set_now(now);
            model.now = now;
        } else { // active expiry pass with a random budget
            size_t budget = static_cast<size_t>(rnd(0, 8));
            bool more = s.expire_due(budget);
            if (!more) {
                int64_t next = s.next_expiry();
                CHECK(next == NO_EXPIRY || next > now, "nothing due after a complete pass");
            }
        }
        CHECK(s.check_invariants(), "heap invariants after op %d (seed %u)", op, seed);
    }

    // Final full reconciliation: expire everything due, then the store must
    // hold exactly the model's live keys.
    while (s.expire_due(1000)) {}
    size_t live = 0;
    for (auto it = model.m.begin(); it != model.m.end();) {
        if (it->second.expire_at != NO_EXPIRY && it->second.expire_at <= now) {
            it = model.m.erase(it);
        } else {
            Store::Node* n = s.find(it->first);
            CHECK(n && n->second.value == it->second.value, "final value, key %s", it->first.c_str());
            live++;
            ++it;
        }
    }
    CHECK(s.size() == live, "final size %zu vs model %zu (seed %u)", s.size(), live, seed);
}

void test_mass_expiry_is_bounded() {
    // 200k keys expiring in the same millisecond must be removed in bounded
    // chunks, each leaving the heap valid.
    Store s;
    s.set_now(0);
    for (int i = 0; i < 200000; i++) s.set_expire(s.upsert("m" + std::to_string(i), "v"), 50);
    s.set_now(50);
    int passes = 0;
    while (s.expire_due(1000)) passes++;
    CHECK(passes == 199, "200 passes of 1000 (199 report 'more'), got %d", passes);
    CHECK(s.size() == 0 && s.keys_with_ttl() == 0, "all expired");
}

void test_heap_order_many_updates() {
    // Repeatedly moving deadlines up and down must keep the heap valid and
    // the heap must never hold more nodes than keys with a TTL (no garbage,
    // unlike the "leave stale entries in the heap" design).
    Store s;
    s.set_now(0);
    std::mt19937 rng(7);
    for (int i = 0; i < 1000; i++) s.set_expire(s.upsert("u" + std::to_string(i), "v"), 1 + rng() % 100000);
    for (int round = 0; round < 100000; round++) {
        Store::Node* n = s.find("u" + std::to_string(rng() % 1000));
        s.set_expire(n, 1 + rng() % 100000);
    }
    CHECK(s.keys_with_ttl() == 1000, "heap size stays = keys with TTL: %zu", s.keys_with_ttl());
    CHECK(s.check_invariants(), "invariants after 100k re-EXPIREs");
    int64_t last = 0;
    s.set_now(200000);
    // Pop one at a time and confirm non-decreasing deadlines.
    while (s.keys_with_ttl() > 0) {
        int64_t next = s.next_expiry();
        CHECK(next >= last, "pop order %lld after %lld", (long long)next, (long long)last);
        last = next;
        s.expire_due(1);
    }
}

} // namespace

int main() {
    test_basic_semantics();
    for (uint32_t seed = 1; seed <= 20; seed++) test_randomized_against_model(seed, 10000, 50);
    test_randomized_against_model(99, 50000, 2000);
    test_mass_expiry_is_bounded();
    test_heap_order_many_updates();
    if (failures) {
        std::printf("store_test: %d failure(s)\n", failures);
        return 1;
    }
    std::printf("store_test: all passed (21 randomized runs vs model, 250k ops, invariants checked after every op)\n");
    return 0;
}
