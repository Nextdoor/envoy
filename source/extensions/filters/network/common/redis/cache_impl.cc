#include "extensions/filters/network/common/redis/cache_impl.h"

#include <cstddef>
#include <string>
#include "extensions/filters/network/common/redis/client.h"
#include "extensions/filters/network/common/redis/codec.h"
#include "extensions/filters/network/common/redis/utility.h"


namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {

const std::string* CacheImpl::getRequestKey(const RespValue& request) {
    const std::string *key = nullptr;

    if (request.type() == RespType::Array && absl::AsciiStrToLower(request.asArray()[0].asString()) == "get") {
        key = &(request.asArray()[1].asString());
    } else if (request.type() == RespType::CompositeArray && absl::AsciiStrToLower(request.asCompositeArray().command()->asString()) == "get") {
        auto start_index = request.asCompositeArray().begin().index_;
        key = &(request.asCompositeArray().baseArray()->asArray()[start_index].asString());
    }

    return key;
}

bool CacheImpl::makeCacheRequest(const RespValue& request) {
    if (!((request.type() == RespType::Array && absl::AsciiStrToLower(request.asArray()[0].asString()) == "get") ||
        (request.type() == RespType::CompositeArray && absl::AsciiStrToLower(request.asCompositeArray().command()->asString()) == "get"))) {
        return false;
    }

    const std::string *key = getRequestKey(request);
    ASSERT(key != nullptr);

    // Verify key is not in the ignore list for caching. If it is
    // then don't query the cache.
    bool skip_cache = false;
    for (const auto& prefix : ignore_key_prefixes_) {
      if (key->rfind(prefix, 0) != std::string::npos) {
        skip_cache = true;
        break;
      }
    }

    if (skip_cache) {
        return false;
    }

    pending_requests_.emplace_back(std::move(new PendingCacheRequest(Operation::Get)));
    client_->makeRequest(request, *this);
    return true;
}

void CacheImpl::set(const RespValue& request, const RespValue& response) {
    if (response.type() != RespType::BulkString) {
        return;
    }

    const std::string *key = getRequestKey(request);
    if (key == nullptr) {
        // non cachable if we can't figure out what the key is
        return;
    }

    // Verify key is not in the ignore list for caching. If it is
    // then don't set value in cache.
    bool skip_caching = false;
    for (const auto& prefix : ignore_key_prefixes_) {
      if (key->rfind(prefix, 0) != std::string::npos) {
        skip_caching = true;
        break;
      }
    }

    if (skip_caching) {
        return;
    }

    RespValuePtr cache_request(new RespValue());
    std::vector<RespValue> values(5);
    values[0].type(RespType::BulkString);
    values[0].asString() = "SET";
    values[1].type(RespType::BulkString);
    values[1].asString() = *key;
    values[2].type(RespType::BulkString);
    values[2].asString() = response.asString();

    // Set a default TTL to ensure that even if we miss an invalidation
    // message from the server that the value will auto expire.
    values[3].type(RespType::BulkString);
    values[3].asString() = "PX";
    values[4].type(RespType::BulkString);
    values[4].asString() = cache_ttl_;

    cache_request->type(RespType::Array);
    cache_request->asArray().swap(values);

    pending_requests_.emplace_back(std::move(new PendingCacheRequest(Operation::Set)));

    client_->makeRequest(*cache_request, *this);
}

void CacheImpl::expire(const RespValue& keys) {
    // Normally we get a list of keys to expire but if the server did
    // a FLUSHALL then the invalidate returns null to signify all keys
    // must be invalidated.
    if (keys.type() == Common::Redis::RespType::Null) {
        clearCache(true);
        return;
    }

    ASSERT(keys.type() == Common::Redis::RespType::Array);
    const std::vector<RespValue>& key_arr = keys.asArray();

    RespValuePtr request(new RespValue());
    std::vector<RespValue> values(1);

    values[0].type(RespType::BulkString);
    values[0].asString() = "UNLINK";

    values.insert(std::end(values), std::begin(key_arr), std::end(key_arr));
    request->type(RespType::Array);
    request->asArray().swap(values);

    pending_requests_.emplace_back(std::move(new PendingCacheRequest(Operation::Expire)));

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

    pending_requests_.emplace_back(std::move(new PendingCacheRequest(Operation::Flush)));
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

    PendingCacheRequestPtr req = std::move(pending_requests_.front());
    pending_requests_.pop_front();

    switch (req->op_) {
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
