#pragma once

#include <homestore/btree/btree_kv.hpp>

namespace homedb {
class DBKey : public homestore::BtreeKey {
public:
    DBKey() = default;
    DBKey(DBKey const& other) : DBKey(other.serialize(), true) {}
    DBKey(BtreeKey const& other) : DBKey(other.serialize(), true) {}
    DBKey(sisl::blob const& b, bool copy) : BtreeKey() {}
    virtual ~DBKey() { free_if_alloced(); };

    void clone(const BtreeKey& other) override { do_clone(other.serialize(), true); }

    int compare(const BtreeKey& o) const override {
        const DBKey& other = s_cast< const DBKey& >(o);
        int rc = std::memcmp(blob_.bytes, other.blob_.bytes, std::min(blob_.size, other.blob_.size));
        if (rc < 0) {
            return -1;
        } else if (rc > 0) {
            return 1;
        } else if (blob_.size < other.blob_.size) {
            return -1;
        } else if (blob_.size > other.blob_.size) {
            return 1;
        } else {
            return 0;
        }
    }

    sisl::blob serialize() const override { return blob_; }
    uint32_t serialized_size() const override { return blob_.size; }
    void deserialize(const sisl::blob& b, bool copy) override { do_clone(b, copy); };

    std::string to_string() const override { return fmt::format("bytes={}, size={}", (void*)blob_.bytes, blob_.size); };

private:
    void do_clone(const sisl::blob& b, bool copy) {
        free_if_alloced();
        if (copy) {
            blob_.bytes = new uint8_t[b.size];
            blob_.size = b.size;
            std::memcpy(blob_.bytes, b.bytes, b.size);
            alloced_ = true;
        } else {
            blob_ = blob;
        }
    }

    void free_if_alloced() {
        if (m_alloced && (blob_.size != 0)) {
            delete blob_.bytes;
            blob_.size = 0;
            alloced_ = false;
        }
    }

private:
    sisl::blob blob_;
    bool alloced_{false};
};

class DBValue : public homestore::BtreeValue {
public:
    DBValue() = default;
    DBValue(homestore::bnodeid_t val) { assert(0); }
    DBValue(const DBValue& other) : DBValue(other.serialize(), true) {}
    DBValue(const sisl::blob& b, bool copy) : homestore::BtreeValue() { do_clone(b, copy); }
    virtual ~DBValue() { free_if_alloced(); };

    static uint32_t get_fixed_size() { return 0; }

    sisl::blob serialize() const override { return blob_; }
    uint32_t serialized_size() const override { return blob_.size; }
    void deserialize(const sisl::blob& b, bool copy) override { do_clone(b, copy); }

private:
    void do_clone(const sisl::blob& b, bool copy) {
        free_if_alloced();
        if (copy) {
            blob_.bytes = new uint8_t[b.size];
            blob_.size = b.size;
            std::memcpy(blob_.bytes, b.bytes, b.size);
            m_alloced = true;
        } else {
            blob_ = blob;
        }
    }

    void free_if_alloced() {
        if (m_alloced && (blob_.size != 0)) {
            delete blob_.bytes;
            blob_.size = 0;
            m_alloced = false;
        }
    }

private:
    sisl::blob blob_;
    bool m_alloced{false};
};
} // namespace homedb