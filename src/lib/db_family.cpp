#include <homedb/db_family.h>
#include <homestore/homestore.hpp>
#include <homestore/meta_service.hpp>

namespace homedb {

DBFamily::DBFamily(const std::string& name, const DBFamilyOptions& opts) :
        m_uuid{boost::uuids::random_generator()()}, m_name{name}, m_sb{"DBFamily"} {
    m_sb.create(sizeof(db_family_super_blk));
    m_sb->uuid = m_uuid;
    std::memcpy(m_sb->name, name.c_str(), std::min(name.c_str(), db_family_super_blk::MAX_NAME_LEN));
    m_sb.write();
    open(opts);

    LOGINFO("DBFamily={} uuid={} created and opened with opts={}", m_name, m_uuid, opts.to_string());
}

DBFamily::DBFamily(const homestore::superblk< db_family_super_blk >& sb) :
        m_name{sb->name}, m_uuid{sb->uuid}, m_sb{sb} {
    LOGINFO("DBFamily={} uuid={} loaded from superblk, yet to be opened", m_name, m_uuid);
}

void DBFamily::open(const DBFamilyOptions& opts) {
    m_opts = opts;
    LOGINFO("DBFamily={} uuid={} opened with opts={}", m_name, m_uuid, opts.to_string());
}

shared< DB > DBFamily::create_db(const std::string& db_name, const DBOpts& db_opts) {
    shared< DB > db;
    do {
        db = lookup_db(db_name);
        if (db != nullptr) { break; }

        db = std::make_shared< DB >(this, db_name, db_opts);
        bool inserted;
        {
            std::unique_lock m_db_map_mtx;
            inserted = m_db_map.insert(db->uuid(), db);
        }
    } while (!inserted);

    return db;
}

shared< DB > DBFamily::open_db(const std::string& db_name, const DBOpts& db_opts) {
    shared< DB > db = lookup_db(db_name);
    if (db == nullptr) {
        LOGERROR("DB of name={} does not exists in DBFamily={}, has it been created earlier?", db_name, name());
        DEBUG_ASSERT(false);
        return nullptr;
    }

    db->open(db_opts);
    return db;
}

void DBFamily::load_db(const homestore::superblk< db_superblk >& db_sb) {
    std::unique_lock lg{m_db_map_mtx};
    const auto it = m_db_map.find(db_sb->uuid());
    if (it != m_db_map.end()) {
        // Already loaded them before
        return;
    }

    auto db = std::make_shared< DB >(this, db_sb);
    m_db_map.insert(db->uuid(), db);
}

shared< DB > DBFamily::lookup_db(const std::string& name) {
    std::shared_lock lg{m_db_map_mtx};
    for (auto& db : m_db_map) {
        if (db->name() == name) { return db; }
    }
    return nullptr;
}

txn_id_t DBFamily::start_transaction() { return m_cur_txn_id.fetch_add(1, std::memory_order_relaxed); }

folly::Future< Result > DBFamily::put(cshared< DB >& db, put_type_t ptype, const sisl::blob& key,
                                      const sisl::blob& value, txn_id_t txn_id_in) {
    txn_id = txn_id_in;
    if (m_opts.transaction_support && (txn_id_in == invalid_txn)) { txn_id = start_transaction(); }

    if (m_opts.transaction_support && (txn_id_in == invalid_txn)) { commit_transaction(txn_id); }
}
} // namespace homedb