#include "extensions/filters/network/common/redis/cache_impl.h"

#include <cstddef>
#include "extensions/filters/network/common/redis/client.h"
#include "extensions/filters/network/common/redis/codec.h"
#include "extensions/filters/network/common/redis/utility.h"


namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {

void CacheImpl::makeCacheRequest(const RespValue& request) {
    pending_requests_.emplace_back(Operation::Get);
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

    pending_requests_.emplace_back(Operation::Set);

    client_->makeRequest(*request, *this);
}

void CacheImpl::expire(const std::string &key) {
    RespValuePtr request(new RespValue());
    std::vector<RespValue> values(2);
    values[0].type(RespType::BulkString);
    values[0].asString() = "del";
    values[1].type(RespType::BulkString);
    values[1].asString() = key;

    request->type(RespType::Array);
    request->asArray().swap(values);

    pending_requests_.emplace_back(Operation::Expire);

    client_->makeRequest(*request, *this);
}

// Extensions::NetworkFilters::Common::Redis::Client::ClientCallbacks
void CacheImpl::onResponse(NetworkFilters::Common::Redis::RespValuePtr&& value) {
    ASSERT(!pending_requests_.empty());

    PendingCacheRequest& req = pending_requests_.front();
    pending_requests_.pop_front();

    switch (req.op_) {
    case Operation::Set:
    case Operation::Expire:
    break;
    case Operation::Get:
        if (value->type() == RespType::Error || value->type() == RespType::Null) {
            callbacks_.front()->onCacheResponse(nullptr);
        } else {
            callbacks_.front()->onCacheResponse(std::move(value));
        }
    break;
    }
}

void CacheImpl::onFailure() {
    ASSERT(!pending_requests_.empty());

    PendingCacheRequest& req = pending_requests_.front();
    pending_requests_.pop_front();

    switch (req.op_) {
    case Operation::Set:
    case Operation::Expire:
    break;
    case Operation::Get:
        callbacks_.front()->onCacheResponse(nullptr);
    break;
    }
}

CacheImpl::PendingCacheRequest::PendingCacheRequest(const Operation op) : op_(op) {}

} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
