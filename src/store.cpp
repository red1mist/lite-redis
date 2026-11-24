#include "store.h"

Store::Node* Store::find(std::string_view key) {
    auto it = map_.find(key); // string_view lookup: no temporary std::string
    if (it == map_.end()) return nullptr;
    if (expired(*it)) { // lazy deletion
        erase(&*it);
        expired_total_++;
        return nullptr;
    }
    return &*it;
}

Store::Node* Store::upsert(std::string_view key, std::string_view value, bool keep_ttl) {
    Node* n = find(key); // an expired key counts as absent: it's replaced fresh
    if (!n) {
        auto [it, inserted] = map_.emplace(std::string(key), Entry{std::string(value)});
        (void)inserted;
        return &*it;
    }
    // Overwrite in place: assign() reuses the existing heap buffer when the
    // new value fits, avoiding a free + malloc. But a key that once held a
    // 100 MB value and is now set to "x" would keep the 100 MB capacity
    // forever, so give memory back when the buffer is mostly empty.
    std::string& v = n->second.value;
    v.assign(value);
    if (v.capacity() > 4096 && v.capacity() / 4 > v.size()) v.shrink_to_fit();
    if (!keep_ttl) clear_expire(n);
    return n;
}

bool Store::erase(std::string_view key) {
    Node* n = find(key);
    if (!n) return false;
    erase(n);
    return true;
}

void Store::erase(Node* n) {
    // Order matters: the heap holds a pointer to this node, so take it out
    // of the heap BEFORE the hash table frees the node.
    if (n->second.heap_idx != SIZE_MAX) heap_remove(n);
    map_.erase(map_.find(n->first));
}

void Store::set_expire(Node* n, int64_t at_ms) {
    n->second.expire_at = at_ms;
    if (n->second.heap_idx == SIZE_MAX) {
        heap_insert(n);
    } else {
        // Deadline changed: it may need to move toward the root (sooner) or
        // toward the leaves (later). Only one of these actually moves it.
        sift_up(n->second.heap_idx);
        sift_down(n->second.heap_idx);
    }
}

bool Store::clear_expire(Node* n) {
    if (n->second.heap_idx == SIZE_MAX) return false;
    heap_remove(n);
    n->second.expire_at = NO_EXPIRY;
    return true;
}

bool Store::expire_due(size_t budget) {
    while (!heap_.empty() && heap_[0]->second.expire_at <= now_) {
        if (budget-- == 0) return true; // more due; let clients run first
        erase(heap_[0]);
        expired_total_++;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Binary min-heap stored in a vector: children of i are 2i+1 and 2i+2,
// parent is (i-1)/2. Smallest expire_at is always at index 0.
// ---------------------------------------------------------------------------

void Store::sift_up(size_t i) {
    Node* n = heap_[i];
    int64_t t = n->second.expire_at;
    while (i > 0) {
        size_t parent = (i - 1) / 2;
        if (heap_[parent]->second.expire_at <= t) break;
        heap_place(i, heap_[parent]); // move the parent down one level
        i = parent;
    }
    heap_place(i, n);
}

void Store::sift_down(size_t i) {
    Node* n = heap_[i];
    int64_t t = n->second.expire_at;
    size_t size = heap_.size();
    while (true) {
        size_t child = 2 * i + 1;
        if (child >= size) break;
        if (child + 1 < size && heap_[child + 1]->second.expire_at < heap_[child]->second.expire_at)
            child++; // the smaller of the two children
        if (heap_[child]->second.expire_at >= t) break;
        heap_place(i, heap_[child]); // move the child up one level
        i = child;
    }
    heap_place(i, n);
}

void Store::heap_insert(Node* n) {
    heap_.push_back(n);
    n->second.heap_idx = heap_.size() - 1;
    sift_up(heap_.size() - 1);
}

void Store::heap_remove(Node* n) {
    // Classic indexed-heap delete: move the LAST node into the hole, shrink
    // the vector, then restore order around the hole. The moved node may
    // belong above or below that position, so try both directions.
    size_t i = n->second.heap_idx;
    Node* last = heap_.back();
    heap_.pop_back();
    n->second.heap_idx = SIZE_MAX;
    if (last == n) return; // it WAS the last node
    heap_place(i, last);
    sift_up(i);
    sift_down(last->second.heap_idx);
}

bool Store::check_invariants() const {
    for (size_t i = 0; i < heap_.size(); i++) {
        const Node* n = heap_[i];
        if (n->second.heap_idx != i || n->second.expire_at == NO_EXPIRY) return false;
        if (i > 0 && heap_[(i - 1) / 2]->second.expire_at > n->second.expire_at) return false;
    }
    size_t with_ttl = 0;
    for (const auto& kv : map_) {
        bool in_heap = kv.second.heap_idx != SIZE_MAX;
        if (in_heap != (kv.second.expire_at != NO_EXPIRY)) return false;
        if (in_heap) {
            if (kv.second.heap_idx >= heap_.size() || heap_[kv.second.heap_idx] != &kv) return false;
            with_ttl++;
        }
    }
    return with_ttl == heap_.size();
}
