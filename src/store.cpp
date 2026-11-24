#include "store.h"

const std::string* Store::get(std::string_view key) const {
    auto it = map_.find(key); // string_view lookup: no temporary std::string
    return it == map_.end() ? nullptr : &it->second.value;
}

void Store::set(std::string_view key, std::string_view value) {
    auto it = map_.find(key);
    if (it == map_.end()) {
        map_.emplace(std::string(key), Entry{std::string(value)});
        return;
    }
    // Overwrite in place: assign() reuses the existing heap buffer when the
    // new value fits, avoiding a free + malloc. But a key that once held a
    // 100 MB value and is now set to "x" would keep the 100 MB capacity
    // forever, so give memory back when the buffer is mostly empty.
    std::string& v = it->second.value;
    v.assign(value);
    if (v.capacity() > 4096 && v.capacity() / 4 > v.size()) v.shrink_to_fit();
}

bool Store::del(std::string_view key) {
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    map_.erase(it);
    return true;
}

bool Store::exists(std::string_view key) const { return map_.find(key) != map_.end(); }
