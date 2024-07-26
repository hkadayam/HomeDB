#pragma once

#include "lib/db_kv.h"

namespace homedb {
struct DBOpts {
    std::string to_string() const { return ""; }
};

#pragma pack(1)
struct db_super_blk {
    static constexpr uint64_t MAGIC{0xDABAF00D};
    static constexpr uint32_t VERSION{1};
    static constexpr size_t MAX_NAME_LEN{512};

    const uint64_t magic{MAGIC};
    const uint32_t version{VERSION};
    uuid_t uuid;
    char name[MAX_NAME_LEN];

    uint64_t get_magic() const { return magic; }
    uint32_t get_version() const { return version; }
};

struct db_index_super_blk {
    uint64_t index_ordinal{0};
    // TODO: Write schema related details for secondary index here
};
#pragma pack()

struct IndexPutRequest : public homestore::BtreeSinglePutRequest {
public:
    const DBKey m_key;
    const DBValue m_value;

public:
    IndexPutRequest(const sisl::blob& key, const sisl::blob& value, homestore::btree_put_type ptype) :
            homestore::BtreeSinglePutRequest(&m_key, &m_value, ptype),
            m_key{key, false /* copy */},
            m_value{value, false, /* copy */} {}
};

struct IndexGetRequest : public homestore::BtreeSingleGetRequest {
public:
    const DBKey m_key;
    DBValue m_value;

public:
    IndexGetRequest(const sisl::blob& key, sisl::blob& value) :
            homestore::BtreeSingleGetRequest(&m_key, &m_value),
            m_key{key, false /* copy */},
            m_value{value, false, /* copy */} {}
};

class DB {
public:
    DB(DBFamily*, const std::string& name, const DBOpts& opts);
    DB(DBFamily*, const homestore::superblk< db_super_blk >& sb);
    void open(const DBOpts& opts);

    uuid_t uuid() const { return m_uuid; }
    std::string name() const { return m_name; }

    folly::Future< Result > put(put_type_t ptype, sisl::blob const& key,  sisl::blob const& value);
    folly::Future< Result > get( sisl::blob const& key, sisl::blob& out_value);

private:
    void create_primary_index();

private:
    DBOpts opts_;
    DBFamily* db_family_; // Back pointer to parent db family
    std::string name_;
    uuid_t uuid_;

    homestore::superblk< db_super_blk > sb_;
    shared< homestore::IndexTable< DBKey, DBValue > > primary_index_;
};
} // namespace homedb