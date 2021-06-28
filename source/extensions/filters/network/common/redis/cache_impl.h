#pragma once

#include "common/common/logger.h"

#include "extensions/filters/network/common/redis/codec.h"
#include "extensions/filters/network/common/redis/client.h"


namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {

enum class Operation {
  Get,
  Set,
  Expire,
  Flush
};

class CacheImpl : public Client::Cache, public Logger::Loggable<Logger::Id::redis>, public Client::ClientCallbacks, public Network::ConnectionCallbacks {
public:
  CacheImpl(Client::ClientPtr&& client) : client_(std::move(client)) {
    client_->addConnectionCallbacks(*this);
  }
  ~CacheImpl() override;
  void makeCacheRequest(const RespValue& request) override;
  void set(const std::string &key, const std::string& value) override;
  void expire(const std::string &key) override;
  void addCallbacks(Client::CacheCallbacks& callbacks) override {
    this->callbacks_.push_front(&callbacks);
  }
  void clearCache(bool synchronous) override;
  void initialize(const std::string& auth_username, const std::string& auth_password, bool clear_cache) override;

  // Extensions::NetworkFilters::Common::Redis::Client::ClientCallbacks
  void onResponse(NetworkFilters::Common::Redis::RespValuePtr&& value) override;
  void onFailure() override;
  bool onRedirection(NetworkFilters::Common::Redis::RespValuePtr&&, const std::string&,
                      bool) override {
    return true;
  }

  // Network::ConnectionCallbacks
  void onEvent(Network::ConnectionEvent event) override;
  void onAboveWriteBufferHighWatermark() override {}
  void onBelowWriteBufferLowWatermark() override {}

private:
  struct PendingCacheRequest : public Client::PoolRequest {
    PendingCacheRequest(const Operation op);
    ~PendingCacheRequest() override = default;

    // PoolRequest
    void cancel() override {};

    const Operation op_;
  };

  Client::ClientPtr client_;
  std::list<Client::CacheCallbacks*> callbacks_;
  std::list<PendingCacheRequest> pending_requests_;
};

class CacheFactoryImpl : public Client::CacheFactory {
public:
  // RedisProxy::ConnPool::ClientFactoryImpl
  Client::CachePtr create(Client::ClientPtr&& client) override {
    return Client::CachePtr{new CacheImpl(std::move(client))};
  }
};

} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
