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
    std::vector<RespValue> values(5);
    values[0].type(RespType::BulkString);
    values[0].asString() = "set";
    values[1].type(RespType::BulkString);
    values[1].asString() = key;
    values[2].type(RespType::BulkString);
    values[2].asString() = value;

    // Set a default TTL to ensure that even if we miss an invalidation
    // message from the server that the value will auto expire.
    values[3].type(RespType::BulkString);
    values[3].asString() = "EX";
    values[4].type(RespType::BulkString);
    values[4].asString() = "900";

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

void CacheImpl::clearCache(bool synchronous) {
    RespValuePtr request(new RespValue());
    std::vector<RespValue> values(2);
    values[0].type(RespType::BulkString);
    values[0].asString() = "FLUSHALL";

    values[1].type(RespType::BulkString);
    if (synchronous) {
        values[1].asString() = "SYNC";
    } else {
        values[1].asString() = "ASYNC";
    }

    request->type(RespType::Array);
    request->asArray().swap(values);

    pending_requests_.emplace_back(Operation::Flush);
    client_->makeRequest(*request, *this);
}

void CacheImpl::initialize(const std::string& auth_username, const std::string& auth_password, bool clear_cache) {
    client_->initialize(auth_username, auth_password);

    // Ensures that if the cache connection was ever lost that on
    // reconnect cache is flushed as we may have missed invalidation
    // messages.
    if (clear_cache) {
        clearCache(true);
    }
}

// Extensions::NetworkFilters::Common::Redis::Client::ClientCallbacks
void CacheImpl::onResponse(NetworkFilters::Common::Redis::RespValuePtr&& value) {
    ASSERT(!pending_requests_.empty());

    PendingCacheRequest& req = pending_requests_.front();
    pending_requests_.pop_front();

    switch (req.op_) {
    case Operation::Set:
    case Operation::Expire:
    case Operation::Flush:
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
    pending_requests_.pop_front();
}

void CacheImpl::onEvent(Network::ConnectionEvent event) {
    if (event == Network::ConnectionEvent::RemoteClose ||
        event == Network::ConnectionEvent::LocalClose) {
        callbacks_.front()->onCacheClose();
    }
}

CacheImpl::~CacheImpl() {
    this->client_->close();
}

CacheImpl::PendingCacheRequest::PendingCacheRequest(const Operation op) : op_(op) {}

} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
