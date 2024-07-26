#include <homedb/homedb.h>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace homedb {
void HomeDB(const homestore::hs_input_params& params) : m_cfg{params} {
    sisl::MallocMetrics::enable();

    HomeStore::instance()
        ->with_params(m_cfg)
        .with_meta_service(HomeDB::meta_store_pct())
        .with_index_service(HomeDB::index_store_pct(), std::make_unique< HomeDBIndexCallbacks >())
        .with_log_service(HomeDB::data_logdev_store_pct(), HomeDB::ctrl_logdev_store_pct())
        .with_data_service(HomeBlks::data_store_pct())
        .before_init_devices([this]() {
            init_meta_blks();
            m_init_thread_id = iomanager.iothread_self();
        })
        .init(true /* wait_for_init */);

    m_db_families_load_pending = false; // After homestore is inited, all db_families must have been loaded
    m_dbs_load_pending = false;
}

std::shared_ptr< homestore::IndexTableBase >
HomeDBIndexCallbacks::on_index_table_found(const homestore::superblk< homestore::index_table_sb >& sb) {
    uuid_t db_uuid = sb->m_parent_uuid;

    // Try to find if the uuid of this belongs to any specific DBFamily.
    std::shared_lock lg{m_family_mtx};
    for ()
}

shared< DBFamily > HomeDB::create_db_family(const std::string& name, const DBFamilyOptions& opts) {
    std::unique_lock lg{m_family_mtx};
    for (auto& dbf : m_db_families) {
        if (dbf->name() == name) {
            LOGERROR("DBFamily of name={} already exists, cannot create dbfamily", name);
            DEBUG_ASSERT(false);
            return nullptr;
        }
    }

    auto dbf = std::make_shared< DBFamily >(name, opts);
    m_db_families.insert(std::make_pair(dbf->uuid(), dbf));
    return dbf;
}

shared< DBFamily > HomeDB::open_db_family(const std::string& name, const DBFamilyOptions& opts) {
    std::shared_lock lg{m_family_mtx};
    for (auto& dbf : m_db_families) {
        if (dbf->name() == name) {
            dbf->open(opts);
            return dbf;
        }
    }

    LOGERROR("DBFamily of name={} does not exists, has it been created earlier?", name);
    DEBUG_ASSERT(false);
    return nullptr;
}

void HomeDB::init_meta_blks() {
    homestore::meta_service().register_handler(
        "DBFamily",
        [this](meta_blk* mblk, sisl::byte_view buf, size_t size) {
            dbfamily_super_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        nullptr);

    homestore::meta_service().register_handler(
        "DB",
        [this](meta_blk* mblk, sisl::byte_view buf, size_t size) {
            db_super_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        nullptr);
}

void HomeDB::dbfamily_super_blk_found(const sisl::byte_view& buf, void* meta_cookie) {
    homestore::superblk< db_family_superblk > dbf_sb;
    dbf_sb.load(buf, meta_cookie);
    DEBUG_ASSERT_EQ(dbf_sb->get_magic(), db_family_superblk::MAGIC, "Invalid db family metablk, magic mismatch");
    DEBUG_ASSERT_EQ(dbf_sb->get_version(), db_familysuperblk::VERSION, "Invalid version of db family metablk");

    {
        std::unique_lock m_family_mtx;
        if (m_db_families.find(dbf_sb->uuid) != m_db_families.cend()) {
            // We already loaded the DB Family, ignore this super_blk found
            return;
        }

        auto dbf = std::make_shared< DBFamily >(dbf_sb);
        m_db_families.insert(dbf->uuid(), dbf);
    }
}

void HomeDB::db_super_blk_found(const sisl::byte_view& buf, void* meta_cookie) {
    homestore::superblk< db_superblk > db_sb;
    db_sb.load(buf, meta_cookie);
    DEBUG_ASSERT_EQ(db_sb->get_magic(), db_superblk::MAGIC, "Invalid db metablk, magic mismatch");
    DEBUG_ASSERT_EQ(db_sb->get_version(), db_superblk::VERSION, "Invalid version of db metablk");

    // Find db family corresponding to this DB
    if (m_db_families_load_pending) {
        m_db_families_load_pending = false;
        homestore::meta_service().read_sub_sb("DBFamily");
    }

    auto dbf = find_db_family(db_sb->db_family_uuid);
    if (dbf == nullptr) {
        LOGWARN("DB uuid={} superblk containing family uuid={} isn't found, unexpected",
                    db_sb->db_uuid, db_sb->db_family_uuid));
        DEBUG_ASSERT(false, "Inconsistent db super block");
        return;
    }
    dbf->load_db(db_sb);
}

shared< homestore::IndexTable< DBKey, DBValue > >
HomeDB::index_super_blk_found(const homestore::superblk< homestore::index_table_sb >& index_sb) {
    uuid_t db_uuid = index_sb->m_parent_uuid;

    // Ensure all DB Families and DBs are loaded
    if (m_db_families_loaded == false) {
        m_db_families_loaded = true;
        homestore::meta_service().read_sub_sb("DBFamily");
    }
    if (m_dbs_loaded == false) {
        m_dbs_loaded = true;
        homestore::meta_service().read_sub_sb("DB");
    }

    // Try to find if the uuid of this belongs to any specific DBFamily.
    std::shared_lock lg{m_family_mtx};
    shared< homestore::IndexTable< DBKey, DBValue > > index{nullptr};
    for (auto& dbf : m_db_families) {
        auto db = dbf->find_db(db_uuid);
        if (db != nullptr) {
            index = db->on_index_found(index_sb);
            break;
        }
    }

    if (index == nullptr) {
        LOGWARN("Index uuid={} superblk is not found on any DB", index_sb->uuid);
        DEBUG_ASSERT(false, "Inconsistent index super block");
    }
    return index;
}

shared< DBFamily > HomeDB::find_db_family(uuid_t uuid) const {
    std::shared_lock lg{m_family_mtx};
    const auto it = m_db_families.find(uuid);
    return (it == m_db_families.cend()) ? nullptr : *it;
}
}; // namespace homedb