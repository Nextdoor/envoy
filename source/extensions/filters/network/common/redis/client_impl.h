#pragma once

#include <chrono>
#include <string>

#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.h"
#include "envoy/stats/timespan.h"
#include "envoy/thread_local/thread_local.h"
#include "envoy/upstream/cluster_manager.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/common/hash.h"
#include "source/common/network/filter_impl.h"
#include "source/common/protobuf/utility.h"
#include "source/common/singleton/const_singleton.h"
#include "source/common/upstream/load_balancer_impl.h"
#include "source/common/upstream/upstream_impl.h"
#include "source/extensions/filters/network/common/redis/client.h"
#include "source/extensions/filters/network/common/redis/cache_impl.h"
#include "source/extensions/filters/network/common/redis/utility.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {
namespace Client {

// TODO(mattklein123): Circuit breaking
// TODO(rshriram): Fault injection

struct RedirectionValues {
  const std::string ASK = "ASK";
  const std::string MOVED = "MOVED";
  const std::string CLUSTER_DOWN = "CLUSTERDOWN";
};

using RedirectionResponse = ConstSingleton<RedirectionValues>;

struct PushValues {
  const std::string INVALIDATE = "invalidate";
};

using PushResponse = ConstSingleton<PushValues>;

class ConfigImpl : public Config {
public:
  ConfigImpl(
      const envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings&
          config);

  bool disableOutlierEvents() const override { return false; }
  std::chrono::milliseconds opTimeout() const override { return op_timeout_; }
  bool enableHashtagging() const override { return enable_hashtagging_; }
  bool enableRedirection() const override { return enable_redirection_; }
  uint32_t maxBufferSizeBeforeFlush() const override { return max_buffer_size_before_flush_; }
  std::chrono::milliseconds bufferFlushTimeoutInMs() const override {
    return buffer_flush_timeout_;
  }
  uint32_t maxUpstreamUnknownConnections() const override {
    return max_upstream_unknown_connections_;
  }
  bool enableCommandStats() const override { return enable_command_stats_; }
  ReadPolicy readPolicy() const override { return read_policy_; }

  std::string cacheCluster() const override { return cache_cluster_; }
  std::chrono::milliseconds cacheOpTimeout() const override { return cache_op_timeout_; }
  uint32_t cacheMaxBufferSizeBeforeFlush() const override { return cache_max_buffer_size_before_flush_; }
  std::chrono::milliseconds cacheBufferFlushTimeoutInMs() const override {
    return cache_buffer_flush_timeout_;
  }
  std::chrono::milliseconds cacheTtl() const override { return cache_ttl_; }
  bool cacheEnableBcastMode() const override { return cache_enable_bcast_mode_; }
  std::vector<std::string> cacheIgnoreKeyPrefixes() const override { return cache_ignore_key_prefixes_; }
  uint32_t cacheShards() const override { return cache_shards_; }
  bool cacheDisableTracking() const override { return cache_disable_tracking_; }
  bool cacheDisableFlushing() const override { return cache_disable_flushing_; }
  bool useUnhealthyHosts() const override { return use_unhealthy_hosts_; }

private:
  const std::chrono::milliseconds op_timeout_;
  const bool enable_hashtagging_;
  const bool enable_redirection_;
  const uint32_t max_buffer_size_before_flush_;
  const std::chrono::milliseconds buffer_flush_timeout_;
  const uint32_t max_upstream_unknown_connections_;
  const bool enable_command_stats_;
  ReadPolicy read_policy_;

  const std::string cache_cluster_;
  const std::chrono::milliseconds cache_op_timeout_;
  const uint32_t cache_max_buffer_size_before_flush_;
  const std::chrono::milliseconds cache_buffer_flush_timeout_;
  const std::chrono::milliseconds cache_ttl_;
  const bool cache_enable_bcast_mode_;
  const std::vector<std::string> cache_ignore_key_prefixes_;
  const uint32_t cache_shards_;
  const bool cache_disable_tracking_;
  const bool cache_disable_flushing_;
  const bool use_unhealthy_hosts_;
};

class ClientImpl : public Client, public DecoderCallbacks, public CacheCallbacks, public Network::ConnectionCallbacks, public Logger::Loggable<Logger::Id::redis> {
public:
  static ClientPtr create(Upstream::HostConstSharedPtr host, Event::Dispatcher& dispatcher,
                          EncoderPtr&& encoder, DecoderFactory& decoder_factory,
                          const Config& config,
                          const RedisCommandStatsSharedPtr& redis_command_stats,
                          Stats::Scope& scope,
                          CachePtr&& cache);

  ClientImpl(Upstream::HostConstSharedPtr host, Event::Dispatcher& dispatcher, EncoderPtr&& encoder,
             DecoderFactory& decoder_factory, const Config& config,
             const RedisCommandStatsSharedPtr& redis_command_stats, Stats::Scope& scope, CachePtr&& cache);
  ~ClientImpl() override;

  // Client
  void addConnectionCallbacks(Network::ConnectionCallbacks& callbacks) override {
    connection_->addConnectionCallbacks(callbacks);
  }
  void close() override;
  PoolRequest* makeRequest(const RespValue& request, ClientCallbacks& callbacks) override;
  bool active() override { return !pending_requests_.empty(); }
  void flushBufferAndResetTimer();
  void initialize(const std::string& auth_username, const std::string& auth_password) override;

private:
  friend class RedisClientImplTest;

  struct UpstreamReadFilter : public Network::ReadFilterBaseImpl {
    UpstreamReadFilter(ClientImpl& parent) : parent_(parent) {}

    // Network::ReadFilter
    Network::FilterStatus onData(Buffer::Instance& data, bool) override {
      parent_.onData(data);
      return Network::FilterStatus::Continue;
    }

    ClientImpl& parent_;
  };

  struct PendingRequest : public PoolRequest {
    PendingRequest(ClientImpl& parent, ClientCallbacks& callbacks, Stats::StatName stat_name, const RespValue& request);
    ~PendingRequest() override;

    // PoolRequest
    void cancel() override;

    ClientImpl& parent_;
    ClientCallbacks& callbacks_;
    Stats::StatName command_;
    bool canceled_{};
    Stats::TimespanPtr aggregate_request_timer_;
    Stats::TimespanPtr command_request_timer_;
    const RespValue request_;
  };

  using PendingRequestPtr = std::unique_ptr<PendingRequest>;

  void onConnectOrOpTimeout();
  void onData(Buffer::Instance& data);
  void putOutlierEvent(Upstream::Outlier::Result result);

  // DecoderCallbacks
  void onRespValue(RespValuePtr&& value) override;

  // CacheCallbacks
  void onCacheResponse(RespValuePtr&& value) override;
  void onCacheClose() override;

  // Network::ConnectionCallbacks
  void onEvent(Network::ConnectionEvent event) override;
  void onAboveWriteBufferHighWatermark() override {}
  void onBelowWriteBufferLowWatermark() override {}

  Upstream::HostConstSharedPtr host_;
  Network::ClientConnectionPtr connection_;
  EncoderPtr encoder_;
  Buffer::OwnedImpl encoder_buffer_;
  DecoderPtr decoder_;
  const Config& config_;
  std::list<PendingRequestPtr> pending_requests_;
  std::list<PendingRequestPtr> pending_cache_requests_;
  Event::TimerPtr connect_or_op_timer_;
  bool connected_{};
  Event::TimerPtr flush_timer_;
  Envoy::TimeSource& time_source_;
  const RedisCommandStatsSharedPtr redis_command_stats_;
  Stats::Scope& scope_;
  CachePtr cache_;
};

class ClientFactoryImpl : public ClientFactory {
public:
  // RedisProxy::ConnPool::ClientFactoryImpl
  ClientPtr create(Upstream::HostConstSharedPtr host, Event::Dispatcher& dispatcher,
                   const Config& config, const RedisCommandStatsSharedPtr& redis_command_stats,
                   Stats::Scope& scope, const std::string& auth_username,
                   const std::string& auth_password, Upstream::HostConstSharedPtr cache_host) override;

  static ClientFactoryImpl instance_;

private:
  DecoderFactoryImpl decoder_factory_;
  CacheFactoryImpl cache_factory_;
};

} // namespace Client
} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
