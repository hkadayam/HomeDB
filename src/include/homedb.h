#pragma once

#include <memory>
#include <map>
#include <homedb/db_family.h>

namespace homedb {
template < typename T >
using shared = std::shared_ptr< T >;

class DBFamily;
class HomeDB {
private:
    std::map< std::string, shared< DBFamily > > db_families_;
    std::shared_lock family_mtx_;

public:
    shared< DBFamily > create_db_family(const std::string& name);
    shared< DB > create_db(const shared< DBFamily >& family, const std::string& name);
};
} // namespace homedb