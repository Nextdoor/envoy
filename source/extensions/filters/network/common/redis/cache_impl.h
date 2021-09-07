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
  Flush,
  Select
};

class CacheImpl : public Client::Cache, public Logger::Loggable<Logger::Id::redis>, public Client::ClientCallbacks, public Network::ConnectionCallbacks {
public:
  CacheImpl(Client::ClientPtr&& client, std::chrono::milliseconds cache_ttl, std::vector<std::string> ignore_key_prefixes) :
    client_(std::move(client)), ignore_key_prefixes_(ignore_key_prefixes) {

    cache_ttl_ = std::to_string(cache_ttl.count());
    client_->addConnectionCallbacks(*this);
  }
  ~CacheImpl() override;
  bool makeCacheRequest(const RespValue& request) override;
  void set(const RespValue& request, const RespValue& response) override;
  void invalidate(const RespValue& keys) override;
  void expire(const RespValue& request) override;
  void addCallbacks(Client::CacheCallbacks& callbacks) override {
    this->callbacks_.push_front(&callbacks);
  }
  void clearCache(bool synchronous) override;
  void initialize(const std::string& auth_username, const std::string& auth_password, bool clear_cache, int db_slot) override;

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
  const std::string* readRequestKey(const RespValue& request);
  const std::string* writeRequestKey(const RespValue& request);
  void selectDatabase(int db);

  struct PendingCacheRequest : public Client::PoolRequest {
    PendingCacheRequest(const Operation op);
    ~PendingCacheRequest() override = default;

    // PoolRequest
    void cancel() override {};

    const Operation op_;
  };

  using PendingCacheRequestPtr = std::unique_ptr<PendingCacheRequest>;

  Client::ClientPtr client_;
  std::string cache_ttl_;
  std::list<Client::CacheCallbacks*> callbacks_;
  std::list<PendingCacheRequestPtr> pending_requests_;
  std::vector<std::string> ignore_key_prefixes_;
};

class CacheFactoryImpl : public Client::CacheFactory {
public:
  // RedisProxy::ConnPool::ClientFactoryImpl
  Client::CachePtr create(Client::ClientPtr&& client, std::chrono::milliseconds cache_ttl, std::vector<std::string> ignore_key_prefixes) override {
    return Client::CachePtr{new CacheImpl(std::move(client), cache_ttl, ignore_key_prefixes)};
  }
};

} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
