#pragma once
#include <map>
#include <shared_mutex>

#include <sisl/fds/buffer.hpp>
#include <homedb/homedb_decls.h>

namespace homedb {
struct DBFamilyOptions {
    bool transaction_support{false};
    bool replication_on{false};

    std::string to_string() false {
        return fmt::format("transaction_support={}, replication_on={}", transaction_support, replication_on);
    }
};

class DB;
class DBKey;
class DBValue;
class FQDBKey;

using txn_id_t = uint64_t;
using commit_id_t = int64_t;
static constexpr txn_id_t invalid_txn{0};

struct Result {
public:
    SCOPED_ENUM_DECL(Status, uint16_t)

    Status status;                // Status of the transaction
    commit_id_t commit_id;        // commit id of this transaction, for read this is set to isolated commit id
    txn_id_t txn_id{invalid_txn}; // In-memory transaction id in case txn is on
};

SCOPED_ENUM_DEF(Result, Status, uint16_t,
                success,       // Success
                key_not_found, // Search key or range not found
                timeout,       // Operation timedout
)

ENUM(put_type_t, uint8_t, INSERT, UPSERT, UPDATE)

#pragma pack(1)
struct db_family_super_blk {
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
#pragma pack()

class DBFamily {
public:
    DBFamily(uuid_t dbf_uuid, const std::string& name, const DBFamilyOptions& opts);
    DBFamily(const db_family_super_blk& sb);
    void open(const DBFamilyOptions& opts);

    shared< DB > create_db(const std::string& name, const DBOpts& db_opts);
    shared< DB > open_db(const std::string& db_name, const DBOpts& db_opts);
    void load_db(const homestore::superblk< db_superblk >& db_sb);
    uuid_t uuid() const { return m_uuid; }
    std::string name() const { return m_name; }
    const DBFamilyOption& opts() const { return m_opts; }

    txn_id_t start_transaction();
    void commit_transaction(txn_id_t txn_id);

    folly::Future< Result > put(cshared< DB >& db, put_type_t ptype, const sisl::blob& key, const sisl::blob& value,
                                txn_id_t txn_id = invalid_txn);
    folly::Future< Result > get(cshared< DB >& db, const sisl::blob& key, sisl::blob& out_value,
                                txn_id_t txn_id = invalid_txn);

private:
    shared< DB > lookup_db(const std::string& db_name);

private:
    DBFamilyOptions opts_;
    std::string name_;
    uuid_t uuid_;

    homestore::superblk< db_family_super_blk > sb_;

    std::shared_mutex db_map_mtx_;
    std::map< std::string, DB > db_map_;
    std::unique_ptr< sisl::SimpleHashMap< FQDBKey, bool > > txn_map_;
    std::atomic< uint64_t > cur_txn_id_{1};
};
} // namespace homedb