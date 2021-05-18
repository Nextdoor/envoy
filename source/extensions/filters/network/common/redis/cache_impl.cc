#include "extensions/filters/network/common/redis/cache_impl.h"

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <memory>
#include <type_traits>
#include "extensions/filters/network/common/redis/client.h"
#include "extensions/filters/network/common/redis/codec.h"
#include "extensions/filters/network/common/redis/utility.h"


namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {

void CacheImpl::makeCacheRequest(const RespValue& request) {
    client_->makeRequest(request, *this);
}

void CacheImpl::set(const std::string &key, const std::string& value) {
    RespValuePtr request(new RespValue());
    std::vector<RespValue> values(3);
    values[0].type(RespType::BulkString);
    values[0].asString() = "set";
    values[1].type(RespType::BulkString);
    values[1].asString() = key;
    values[2].type(RespType::BulkString);
    values[2].asString() = value;

    request->type(RespType::Array);
    request->asArray().swap(values);

    client_->makeRequest(*request, *this);
}

void CacheImpl::expire(const std::string &key) {
    cache_store_.erase(key);
}

// Extensions::NetworkFilters::Common::Redis::Client::ClientCallbacks
void CacheImpl::onResponse(NetworkFilters::Common::Redis::RespValuePtr&& value) {
    // Handle Cache set or delete responses
    if (value->type() == RespType::SimpleString && value->asString() == "OK") {
        return;
    }
    
    if (value->type() != RespType::Error) {
        callbacks_.front()->onCacheResponse(std::move(value));
    } else {
        callbacks_.front()->onCacheResponse(nullptr);
    }
}

void CacheImpl::onFailure() {
    // TODO(slava): implement
}

} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
