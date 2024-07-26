#include <homedb/db_family.h>
#include <homestore/homestore.hpp>
#include <homestore/meta_service.hpp>

using namespace homestore;

namespace homedb {

DB::DB(DBFamily* db_family, std::string const& name, DBOpts const& opts) :
        db_family_{db_family}, uuid_{boost::uuids::random_generator()()}, name_{name}, sb_{"DB"} {
    sb_.create(sizeof(db_super_blk));
    sb_->uuid = uuid_;
    std::memcpy(sb_->name, name.c_str(), std::min(name.c_str(), db_super_blk::MAX_NAME_LEN));
    sb_.write();
    open(opts);

    create_primary_index();
    LOGINFO("DB={} uuid={} created and opened with opts={}", name_, uuid_, opts.to_string());
}

DB::DB(DBFamily* db_family, homestore::superblk< db_super_blk > const& sb) :
        db_family_{db_family}, name_{sb->name}, uuid_{sb->uuid}, sb_{sb} {
    LOGINFO("DB={} uuid={} loaded from superblk, yet to be opened", name_, uuid_);
}

void DB::open(const DBOpts& opts) {
    opts_ = opts;
    LOGINFO("DB={} uuid={} opened with opts={}", name_, uuid_, opts.to_string());

    if (primary_index_ == nullptr) {
        LOGINFO("DB={} is opened, but it has not found the primary index, perhaps before index creation, system has "
                "exited, recreating primary index",
                name_);
        create_primary_index();
    }
}

shared< homestore::IndexTable< DBKey, DBValue > > DB::on_index_found(homestore::superblk< index_table_sb > const& sb) {
    LOGINFO("DB={} uuid={} found an index={}", name_, uuid_, sb->uuid);
    auto* dbi_sb = r_cast< db_index_super_blk* >(sb->user_sb_bytes);

    if (dbi_sb->index_ordinal == 0) {
        BtreeConfig cfg{index_service().node_size(), name_ + "_primary"};
        primary_index_ = std::make_shared< homestore::IndexTable< DBKey, DBValue > >(sb, cfg);
    } else {
        DEBUG_ASSERT(false, "Yet to support secondary index");
    }
}

void DB::create_primary_index() {
    homestore::BtreeConfig cfg{index_service().node_size(), name_ + "_primary"};
    primary_index_ = std::make_shared< homestore::IndexTable< DBKey, DBValue > >(
        boost::uuids::random_generator()(), uuid_, sizeof(db_index_super_blk), cfg);
    auto sb = primary_index_->mutable_super_blk();
    auto* dbi_sb = r_cast< db_index_super_blk* >(sb->user_sb_bytes);
    dbi_sb->index_ordinal = 0;
    sb.write();
}

folly::Future< homestore::btree_status_t > DB::async_put(put_type_t ptype, sisl::blob const& key,
                                                         sisl::blob const& value) {
    if ((db_family_->opts().transaction_support) || (db_family_->opts().versioned_support) ||
        (db_family_->opts().replication_on)) {
        return folly::make_future< homestore::btree_status_t >(homestore::btree_status_t::not_supported);
    } else {
        db_req* req;
        repl_dev_->async_alloc_write();
        if (ptype == put_type_t::INSERT) {
            bt_ptype = homestore::btree_put_type::INSERT;
        } else if (ptype == put_type_t::UPSERT) {
            bt_ptype = homestore::btree_put_type::UPSERT;
        } else {
            bt_ptype = homestore::btree_put_type::UPDATE;
        }
        return primary_index_->put(DBKey{key}, DBValue{value}, bt_ptype);
    }
}

folly::Future< btree_status_t > DB::get(const sisl::blob& key, sisl::blob& out_value) {
    if ((db_family_->opts().transaction_support) || (db_family_->opts().replication_on)) {
        return folly::make_future< btree_status_t >(btree_status_t::not_supported);
    } else {
        auto req = std::make_shared< IndexGetRequest >(key, out_value);
        return primary_index_->async_get(std::dynamic_pointer_cast< BtreeSingleGetRequest >(req));
    }
}

} // namespace homedb