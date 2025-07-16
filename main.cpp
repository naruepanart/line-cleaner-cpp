#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>
#include <cmath>

namespace fs = std::filesystem;

constexpr size_t BATCH_SIZE = 4096;
constexpr size_t LINE_BUF_SIZE = 65536;

int64_t fast_hash(std::string_view str) {
    uint64_t hash = 0xcbf29ce484222325;
    for (size_t i = 0; i < std::min(size_t(8), str.size()); ++i) {
        hash ^= static_cast<uint8_t>(str[i]);
        hash *= 0x100000001b3;
    }
    return static_cast<int64_t>(hash);
}

class FixedHashSet {
    std::vector<uint64_t> slots_;
    size_t mask_;

public:
    explicit FixedHashSet(size_t capacity) {
        size_t size = 1ull << static_cast<size_t>(std::ceil(std::log2(capacity)));
        slots_.resize(size, 0);
        mask_ = size - 1;
    }

    bool insert(uint64_t hash) {
        size_t idx = hash & mask_;
        for (size_t i = 0; i < slots_.size(); ++i) {
            if (slots_[idx] == 0) {
                slots_[idx] = hash;
                return true;
            }
            if (slots_[idx] == hash)
                return false;
            idx = (idx + 1) & mask_;
        }
        slots_[idx] = hash; // Fallback: overwrite
        return true;
    }
};

fs::path temp_path(const fs::path& original) {
    fs::path tmp = original;
    tmp.replace_extension(".tmp");
    return tmp;
}

bool dedupe_to_writer(const fs::path& src_path, std::ofstream& out) {
    std::ifstream in(src_path);
    if (!in.is_open()) return false;

    FixedHashSet seen(1024);
    std::vector<char> batch;
    batch.reserve(BATCH_SIZE);

    std::string line;
    while (std::getline(in, line)) {
        line.erase(std::find_if(line.rbegin(), line.rend(),
            [](unsigned char c) { return !std::isspace(c); }).base(), line.end());

        if (line.empty()) continue;

        auto hash = fast_hash(line);
        if (!seen.insert(hash)) continue;

        if (batch.size() + line.size() + 1 > batch.capacity()) {
            out.write(batch.data(), batch.size());
            batch.clear();
        }

        if (!batch.empty())
            batch.push_back('\n');

        batch.insert(batch.end(), line.begin(), line.end());
    }

    if (!batch.empty())
        out.write(batch.data(), batch.size());

    return true;
}

bool atomic_dedupe(const std::string& path_str) {
    fs::path path(path_str);
    if (!fs::exists(path) || fs::is_directory(path) || fs::file_size(path) == 0)
        return true;

    fs::path tmp = temp_path(path);
    std::ofstream tmp_file(tmp, std::ios::binary | std::ios::trunc);
    if (!tmp_file.is_open()) return false;

    if (!dedupe_to_writer(path, tmp_file)) return false;

    tmp_file.flush();
    tmp_file.close();

    std::error_code ec;
    fs::rename(tmp, path, ec);
    return !ec;
}

int main() {
    if (!atomic_dedupe("data.txt")) {
        std::cerr << "Failed to dedupe\n";
        return 1;
    }
    return 0;
}
