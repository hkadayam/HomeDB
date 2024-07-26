#pragma once

#include <memory>
#include <map>
#include <shared_mutex>

#include <homedb/homedb_decls.h>
#include <homedb/db_family.h>

namespace homedb {
class DBFamily;
class HomeDB {
public:
    void init();
    void shutdown();

    shared< DBFamily > create_db_family(uuid_t dbf_uuid, const std::string& name);
    shared< DBFamily > open_db_family(uuid_t dbf_uuid);

private:
    std::map< uuid_t, shared< DBFamily > > db_families_;
    mutable std::shared_mutex family_mtx_;
    bool db_families_load_pending_{true};
    bool dbs_load_pending_{true};

private:
    void init_meta_blks();
    void dbfamily_super_blk_found(const sisl::byte_view& buf, void* meta_cookie);
    void db_super_blk_found(const sisl::byte_view& buf, void* meta_cookie);
    shared< DBFamily > find_db_family(uuid_t uuid);

    static float data_store_pct() {
        return (homestore::is_data_drive_hdd() ? hdd_data_store_pct : nvme_data_store_pct);
    }
    static float index_store_pct() {
        return (homestore::is_data_drive_hdd() ? hdd_indx_store_pct : nvme_indx_store_pct);
    }
    static float meta_store_pct() {
        return (homestore::is_data_drive_hdd() ? hdd_meta_store_pct : nvme_meta_store_pct);
    }
    static float data_logdev_store_pct() {
        return (homestore::is_data_drive_hdd() ? hdd_data_logdev_store_pct : nvme_data_logdev_store_pct);
    }
    static float ctrl_logdev_store_pct() {
        return (homestore::is_data_drive_hdd() ? hdd_ctrl_logdev_store_pct : nvme_ctrl_logdev_store_pct);
    }

    static constexpr float nvme_data_store_pct{42.0};
    static constexpr float nvme_meta_store_pct{0.5};
    static constexpr float nvme_indx_store_pct{45.0};
    static constexpr float nvme_data_logdev_store_pct{1.8};
    static constexpr float nvme_ctrl_logdev_store_pct{0.2};

    static constexpr float hdd_data_store_pct{95.0};
    static constexpr float hdd_meta_store_pct{0.5};
    static constexpr float hdd_indx_store_pct{87.0};
    static constexpr float hdd_data_logdev_store_pct{8};
    static constexpr float hdd_ctrl_logdev_store_pct{2};
};
} // namespace homedb