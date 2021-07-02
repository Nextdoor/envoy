#include "common/common/assert.h"
#include "common/common/logger.h"
#include "common/network/address_impl.h"

#include "envoy/stream_info/stream_info.h"
#include "envoy/upstream/host_description.h"
#include "envoy/upstream/upstream.h"

#include "extensions/filters/network/common/redis/cache_impl.h"
#include "extensions/filters/network/common/redis/client.h"
#include "extensions/filters/network/common/redis/client_impl.h"
#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.h"
#include "extensions/filters/network/common/redis/codec.h"
#include "extensions/filters/network/common/redis/utility.h"
#include <cstddef>
#include <memory>
#include <optional>
#include <string>


namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {
namespace Client {
namespace {
// null_pool_callbacks is used for requests that must be filtered and not redirected such as
// "asking".
Common::Redis::Client::DoNothingPoolCallbacks null_pool_callbacks;
} // namespace

ConfigImpl::ConfigImpl(
    const envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings&
        config)
    : op_timeout_(PROTOBUF_GET_MS_REQUIRED(config, op_timeout)),
      enable_hashtagging_(config.enable_hashtagging()),
      enable_redirection_(config.enable_redirection()),
      max_buffer_size_before_flush_(
          config.max_buffer_size_before_flush()), // This is a scalar, so default is zero.
      buffer_flush_timeout_(PROTOBUF_GET_MS_OR_DEFAULT(
          config, buffer_flush_timeout,
          3)), // Default timeout is 3ms. If max_buffer_size_before_flush is zero, this is not used
               // as the buffer is flushed on each request immediately.
      max_upstream_unknown_connections_(
          PROTOBUF_GET_WRAPPED_OR_DEFAULT(config, max_upstream_unknown_connections, 100)),
      enable_command_stats_(config.enable_command_stats()) {
  switch (config.read_policy()) {
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::MASTER:
    read_policy_ = ReadPolicy::Primary;
    break;
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::
      PREFER_MASTER:
    read_policy_ = ReadPolicy::PreferPrimary;
    break;
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::REPLICA:
    read_policy_ = ReadPolicy::Replica;
    break;
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::
      PREFER_REPLICA:
    read_policy_ = ReadPolicy::PreferReplica;
    break;
  case envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::ANY:
    read_policy_ = ReadPolicy::Any;
    break;
  default:
    NOT_REACHED_GCOVR_EXCL_LINE;
    break;
  }
}

ClientPtr ClientImpl::create(Upstream::HostConstSharedPtr host, Event::Dispatcher& dispatcher,
                             EncoderPtr&& encoder, DecoderFactory& decoder_factory,
                             const Config& config,
                             const RedisCommandStatsSharedPtr& redis_command_stats,
                             Stats::Scope& scope,
                             CachePtr&& cache) {
  auto client = std::make_unique<ClientImpl>(host, dispatcher, std::move(encoder), decoder_factory,
                                             config, redis_command_stats, scope, std::move(cache));
  client->connection_ = host->createConnection(dispatcher, nullptr, nullptr).connection_;
  client->connection_->addConnectionCallbacks(*client);
  client->connection_->addReadFilter(Network::ReadFilterSharedPtr{new UpstreamReadFilter(*client)});
  client->connection_->connect();
  client->connection_->noDelay(true);
  return client;
}

ClientImpl::ClientImpl(Upstream::HostConstSharedPtr host, Event::Dispatcher& dispatcher,
                       EncoderPtr&& encoder, DecoderFactory& decoder_factory, const Config& config,
                       const RedisCommandStatsSharedPtr& redis_command_stats, Stats::Scope& scope,
                       CachePtr&& cache)
    : host_(host), encoder_(std::move(encoder)), decoder_(decoder_factory.create(*this)),
      config_(config),
      connect_or_op_timer_(dispatcher.createTimer([this]() { onConnectOrOpTimeout(); })),
      flush_timer_(dispatcher.createTimer([this]() { flushBufferAndResetTimer(); })),
      time_source_(dispatcher.timeSource()), redis_command_stats_(redis_command_stats),
      scope_(scope), cache_(std::move(cache)) {
  host->cluster().stats().upstream_cx_total_.inc();
  host->stats().cx_total_.inc();
  host->cluster().stats().upstream_cx_active_.inc();
  host->stats().cx_active_.inc();
  connect_or_op_timer_->enableTimer(host->cluster().connectTimeout());
}

ClientImpl::~ClientImpl() {
  ENVOY_LOG(info, "ClientImpl::~ClientImpl: {}", this->host_->address()->asString());
  ASSERT(pending_requests_.empty());
  ASSERT(connection_->state() == Network::Connection::State::Closed);
  host_->cluster().stats().upstream_cx_active_.dec();
  host_->stats().cx_active_.dec();
}

void ClientImpl::close() {
  ENVOY_LOG(info, "ClientImpl::close: {}", this->host_->address()->asString());
  connection_->close(Network::ConnectionCloseType::NoFlush);
}

void ClientImpl::flushBufferAndResetTimer() {
  if (flush_timer_->enabled()) {
    flush_timer_->disableTimer();
  }
  connection_->write(encoder_buffer_, false);
}

PoolRequest* ClientImpl::makeRequest(const RespValue& request, ClientCallbacks& callbacks) {
  ENVOY_LOG(info, "ClientImpl::makeRequest: {} {}", this->host_->address()->asString(), request.toString());
  ASSERT(connection_->state() == Network::Connection::State::Open);

  const bool empty_buffer = encoder_buffer_.length() == 0;

  Stats::StatName command;
  if (config_.enableCommandStats()) {
    // Only lowercase command and get StatName if we enable command stats
    command = redis_command_stats_->getCommandFromRequest(request);
  } else {
    // If disabled, we use a placeholder stat name "unused" that is not used
    command = redis_command_stats_->getUnusedStatName();
  }

  PendingRequestPtr prp{new PendingRequest(*this, callbacks, command, request)};

  if (cache_ && request.type() == RespType::Array &&
        absl::AsciiStrToLower(request.asArray()[0].asString()) == "get") {
    ENVOY_LOG(info, "ClientImpl::makeRequest: {} checking cache for {}", this->host_->address()->asString(), request.toString());
    pending_cache_requests_.push_back(std::move(prp));
    cache_->makeCacheRequest(request);
    return pending_cache_requests_.back().get();
  }

  if (config_.enableCommandStats()) {
    redis_command_stats_->updateStatsTotal(scope_, command);
  }

  pending_requests_.push_back(std::move(prp));
  encoder_->encode(request, encoder_buffer_);

  // If buffer is full, flush. If the buffer was empty before the request, start the timer.
  if (encoder_buffer_.length() >= config_.maxBufferSizeBeforeFlush()) {
    flushBufferAndResetTimer();
  } else if (empty_buffer) {
    flush_timer_->enableTimer(std::chrono::milliseconds(config_.bufferFlushTimeoutInMs()));
  }

  // Only boost the op timeout if:
  // - We are not already connected. Otherwise, we are governed by the connect timeout and the timer
  //   will be reset when/if connection occurs. This allows a relatively long connection spin up
  //   time for example if TLS is being used.
  // - This is the first request on the pipeline. Otherwise the timeout would effectively start on
  //   the last operation.
  if (connected_ && pending_requests_.size() == 1) {
    connect_or_op_timer_->enableTimer(config_.opTimeout());
  }

  return pending_requests_.back().get();
}

void ClientImpl::onConnectOrOpTimeout() {
  putOutlierEvent(Upstream::Outlier::Result::LocalOriginTimeout);
  if (connected_) {
    host_->cluster().stats().upstream_rq_timeout_.inc();
    host_->stats().rq_timeout_.inc();
  } else {
    host_->cluster().stats().upstream_cx_connect_timeout_.inc();
    host_->stats().cx_connect_fail_.inc();
  }

  connection_->close(Network::ConnectionCloseType::NoFlush);
}

void ClientImpl::onData(Buffer::Instance& data) {
  try {
    decoder_->decode(data);
  } catch (ProtocolError&) {
    putOutlierEvent(Upstream::Outlier::Result::ExtOriginRequestFailed);
    host_->cluster().stats().upstream_cx_protocol_error_.inc();
    host_->stats().rq_error_.inc();
    connection_->close(Network::ConnectionCloseType::NoFlush);
  }
}

void ClientImpl::putOutlierEvent(Upstream::Outlier::Result result) {
  if (!config_.disableOutlierEvents()) {
    host_->outlierDetector().putResult(result);
  }
}

void ClientImpl::onEvent(Network::ConnectionEvent event) {
  ENVOY_LOG(info, "ClientImpl::onEvent: {}", this->host_->address()->asString());
  if (event == Network::ConnectionEvent::RemoteClose ||
      event == Network::ConnectionEvent::LocalClose) {
    ENVOY_LOG(info, "ClientImpl::onEvent: close {}", this->host_->address()->asString());
    Upstream::reportUpstreamCxDestroy(host_, event);
    if (!pending_requests_.empty()) {
      Upstream::reportUpstreamCxDestroyActiveRequest(host_, event);
      if (event == Network::ConnectionEvent::RemoteClose) {
        putOutlierEvent(Upstream::Outlier::Result::LocalOriginConnectFailed);
      }
    }

    while (!pending_cache_requests_.empty()) {
      PendingRequestPtr& request = pending_cache_requests_.front();
      if (!request->canceled_) {
        request->callbacks_.onFailure();
      }
      pending_cache_requests_.pop_front();
    }

    while (!pending_requests_.empty()) {
      PendingRequestPtr& request = pending_requests_.front();
      if (!request->canceled_) {
        request->callbacks_.onFailure();
      } else {
        host_->cluster().stats().upstream_rq_cancelled_.inc();
      }
      pending_requests_.pop_front();
    }

    connect_or_op_timer_->disableTimer();
  } else if (event == Network::ConnectionEvent::Connected) {
    connected_ = true;
    ASSERT(!pending_requests_.empty());
    connect_or_op_timer_->enableTimer(config_.opTimeout());
  }

  if (event == Network::ConnectionEvent::RemoteClose && !connected_) {
    host_->cluster().stats().upstream_cx_connect_fail_.inc();
    host_->stats().cx_connect_fail_.inc();
  }
}

void ClientImpl::onCacheResponse(RespValuePtr&& value) {
  ASSERT(!pending_cache_requests_.empty());

  PendingRequestPtr pending_request = std::move(pending_cache_requests_.front());
  pending_cache_requests_.pop_front();
  const bool canceled = pending_request->canceled_;

  if (canceled) {
    host_->cluster().stats().upstream_rq_cancelled_.inc();
  }

  // Cache hit
  if (value != nullptr) {
    ENVOY_LOG(info, "ClientImpl::onCacheResponse: {} HIT {}", this->host_->address()->asString(), pending_request->request_.toString());
    if (config_.enableCommandStats()) {
      pending_request->command_request_timer_->complete();
    }
    pending_request->aggregate_request_timer_->complete();

    if (!canceled) {
      ClientCallbacks& callbacks = pending_request->callbacks_;
      callbacks.onResponse(std::move(value));
    }

    // If there are no remaining ops in the pipeline we need to disable the timer.
    // Otherwise we boost the timer since we are receiving responses and there are more to flush
    // out.
    if (pending_requests_.empty()) {
      connect_or_op_timer_->disableTimer();
    } else {
      connect_or_op_timer_->enableTimer(config_.opTimeout());
    }

    putOutlierEvent(Upstream::Outlier::Result::ExtOriginRequestSuccess);
  } else {
    ENVOY_LOG(info, "ClientImpl::onCacheResponse: {} MISS {}", this->host_->address()->asString(), pending_request->request_.toString());
    const bool empty_buffer = encoder_buffer_.length() == 0;

    if (canceled) {
      return;
    }

    if (config_.enableCommandStats()) {
      redis_command_stats_->updateStatsTotal(scope_, pending_request->command_);
    }

    RespValue request_val = pending_request->request_;
    pending_requests_.push_back(std::move(pending_request));
    encoder_->encode(request_val, encoder_buffer_);

    // If buffer is full, flush. If the buffer was empty before the request, start the timer.
    if (encoder_buffer_.length() >= config_.maxBufferSizeBeforeFlush()) {
      flushBufferAndResetTimer();
    } else if (empty_buffer) {
      flush_timer_->enableTimer(std::chrono::milliseconds(config_.bufferFlushTimeoutInMs()));
    }

    // Only boost the op timeout if:
    // - We are not already connected. Otherwise, we are governed by the connect timeout and the timer
    //   will be reset when/if connection occurs. This allows a relatively long connection spin up
    //   time for example if TLS is being used.
    // - This is the first request on the pipeline. Otherwise the timeout would effectively start on
    //   the last operation.
    if (connected_ && pending_requests_.size() == 1) {
      connect_or_op_timer_->enableTimer(config_.opTimeout());
    }
  }
}

void ClientImpl::onCacheClose() {
  ENVOY_LOG(info, "ClientImpl::onCacheClose: {}", this->host_->address()->asString());
  // Propagate the close
  this->close();
}

void ClientImpl::onRespValue(RespValuePtr&& value) {
  if (cache_ && value->type() == Common::Redis::RespType::Push && PushResponse::get().INVALIDATE == value->asArray()[0].asString()) {
    ENVOY_LOG(info, "ClientImpl::onRespValue: {} : invalidate {}", this->host_->address()->asString(), value->toString());

    if (value->asArray()[1].type() == Common::Redis::RespType::BulkString) {
      cache_->expire(value->asArray()[1].asArray()[0].asString());
    } else if (value->asArray()[1].type() == Common::Redis::RespType::Null) {
      // On Null it means the FLUSHALL was run on Redis and entire cache must
      // be cleared.
      cache_->clearCache(true);
    }

    return;
  }

  ASSERT(!pending_requests_.empty());
  PendingRequestPtr request = std::move(pending_requests_.front());
  const bool canceled = request->canceled_;
  ENVOY_LOG(info, "ClientImpl::onRespValue: {}", this->host_->address()->asString());

  if (config_.enableCommandStats()) {
    bool success = !canceled && (value->type() != Common::Redis::RespType::Error);
    redis_command_stats_->updateStats(scope_, request->command_, success);
    request->command_request_timer_->complete();
  }
  request->aggregate_request_timer_->complete();

  ClientCallbacks& callbacks = request->callbacks_;

  // We need to ensure the request is popped before calling the callback, since the callback might
  // result in closing the connection.
  pending_requests_.pop_front();

  if (canceled) {
    host_->cluster().stats().upstream_rq_cancelled_.inc();
  } else if (config_.enableRedirection() && (value->type() == Common::Redis::RespType::Error)) {
    std::vector<absl::string_view> err = StringUtil::splitToken(value->asString(), " ", false);
    bool redirected = false;
    if (err.size() == 3) {
      // MOVED and ASK redirection errors have the following substrings: MOVED or ASK (err[0]), hash
      // key slot (err[1]), and IP address and TCP port separated by a colon (err[2])
      if (err[0] == RedirectionResponse::get().MOVED || err[0] == RedirectionResponse::get().ASK) {
        ENVOY_LOG(info, "ClientImpl::onRespValue moved: {}", this->host_->address()->asString());
        redirected = true;
        bool redirect_succeeded = callbacks.onRedirection(std::move(value), std::string(err[2]),
                                                          err[0] == RedirectionResponse::get().ASK);
        if (redirect_succeeded) {
          host_->cluster().stats().upstream_internal_redirect_succeeded_total_.inc();
        } else {
          host_->cluster().stats().upstream_internal_redirect_failed_total_.inc();
        }
      }
    }
    if (!redirected) {
      if (err[0] == RedirectionResponse::get().CLUSTER_DOWN) {
        ENVOY_LOG(info, "ClientImpl::onRespValue cluster down: {}", this->host_->address()->asString());
        callbacks.onFailure();
      } else {
        ENVOY_LOG(info, "ClientImpl::onRespValue not redirect response: {}", this->host_->address()->asString());
        callbacks.onResponse(std::move(value));
      }
    }
  } else {
    // If request is a get then fire and forget cache set request
    if (cache_ &&
      request->request_.type() == RespType::Array &&
      absl::AsciiStrToLower(request->request_.asArray()[0].asString()) == "get" &&
      (value->type() == RespType::SimpleString || value->type() == RespType::BulkString)) {
      cache_->set(request->request_.asArray()[1].asString(), value->asString());
    } else {
      if (cache_) {
        ENVOY_LOG(info, "ClientImpl::onRespValue non-cachable: {} >>>>>>>>{}<<<<<<<<", this->host_->address()->asString(), request->request_.type());
      }
    }

    callbacks.onResponse(std::move(value));
  }

  // If there are no remaining ops in the pipeline we need to disable the timer.
  // Otherwise we boost the timer since we are receiving responses and there are more to flush
  // out.
  if (pending_requests_.empty()) {
    connect_or_op_timer_->disableTimer();
  } else {
    connect_or_op_timer_->enableTimer(config_.opTimeout());
  }

  putOutlierEvent(Upstream::Outlier::Result::ExtOriginRequestSuccess);
}

ClientImpl::PendingRequest::PendingRequest(ClientImpl& parent, ClientCallbacks& callbacks,
                                           Stats::StatName command, const RespValue& request)
    : parent_(parent), callbacks_(callbacks), command_{command},
      aggregate_request_timer_(parent_.redis_command_stats_->createAggregateTimer(
          parent_.scope_, parent_.time_source_)), request_(request) {
  if (parent_.config_.enableCommandStats()) {
    command_request_timer_ = parent_.redis_command_stats_->createCommandTimer(
        parent_.scope_, command_, parent_.time_source_);
  }
  parent.host_->cluster().stats().upstream_rq_total_.inc();
  parent.host_->stats().rq_total_.inc();
  parent.host_->cluster().stats().upstream_rq_active_.inc();
  parent.host_->stats().rq_active_.inc();
}

ClientImpl::PendingRequest::~PendingRequest() {
  parent_.host_->cluster().stats().upstream_rq_active_.dec();
  parent_.host_->stats().rq_active_.dec();
}

void ClientImpl::PendingRequest::cancel() {
  // If we get a cancellation, we just mark the pending request as cancelled, and then we drop
  // the response as it comes through. There is no reason to blow away the connection when the
  // remote is already responding as fast as possible.
  canceled_ = true;
}

void ClientImpl::initialize(const std::string& auth_username, const std::string& auth_password) {
  ENVOY_LOG(info, "ClientImpl::initialize: {}", this->host_->address()->asString());
  if (!auth_username.empty()) {
    // Send an AUTH command to the upstream server with username and password.
    Utility::AuthRequest auth_request(auth_username, auth_password);
    makeRequest(auth_request, null_pool_callbacks);
  } else if (!auth_password.empty()) {
    // Send an AUTH command to the upstream server.
    Utility::AuthRequest auth_request(auth_password);
    makeRequest(auth_request, null_pool_callbacks);
  }

  // Send a HELLO command to set client server protocol version.
  Utility::HelloRequest hello_request;
  makeRequest(hello_request, null_pool_callbacks);

  // Turn on client tracking
  if (cache_) {
    cache_->addCallbacks(*this);
    Utility::ClientTrackingRequest client_tracking_request;
    makeRequest(client_tracking_request, null_pool_callbacks);
  }

  // Any connection to replica requires the READONLY command in order to perform read.
  // Also the READONLY command is a no-opt for the primary.
  // We only need to send the READONLY command iff it's possible that the host is a replica.
  if (config_.readPolicy() != Common::Redis::Client::ReadPolicy::Primary) {
    makeRequest(Utility::ReadOnlyRequest::instance(), null_pool_callbacks);
  }
}

ClientFactoryImpl ClientFactoryImpl::instance_;

envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings createConnPoolSettings(
    int64_t millis = 20, bool hashtagging = true, bool redirection_support = true,
    uint32_t max_unknown_conns = 100,
    envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings::ReadPolicy
        read_policy = envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::
            ConnPoolSettings::MASTER) {
  envoy::extensions::filters::network::redis_proxy::v3::RedisProxy::ConnPoolSettings setting{};
  setting.mutable_op_timeout()->CopyFrom(Protobuf::util::TimeUtil::MillisecondsToDuration(millis));
  setting.set_enable_hashtagging(hashtagging);
  setting.set_enable_redirection(redirection_support);
  setting.mutable_max_upstream_unknown_connections()->set_value(max_unknown_conns);
  setting.set_read_policy(read_policy);
  return setting;
}

ClientPtr ClientFactoryImpl::create(Upstream::HostConstSharedPtr host,
                                    Event::Dispatcher& dispatcher, const Config& config,
                                    const RedisCommandStatsSharedPtr& redis_command_stats,
                                    Stats::Scope& scope, const std::string& auth_username,
                                    const std::string& auth_password,
                                    Upstream::HostConstSharedPtr cache_host) {
  CachePtr cp = nullptr;
  if (cache_host != nullptr) {
    auto cache_config = new ConfigImpl(createConnPoolSettings(20, true, false));
    ClientPtr cache_client = ClientImpl::create(cache_host, dispatcher, EncoderPtr{new EncoderImpl(RespVersion::Resp3)},
                                      decoder_factory_, *cache_config, redis_command_stats, cache_host->cluster().statsScope(), nullptr);
    cp = cache_factory_.create(std::move(cache_client));
    cp->initialize(auth_username, auth_password, true);
  }

  ClientPtr client = ClientImpl::create(host, dispatcher, EncoderPtr{new EncoderImpl(RespVersion::Resp3)},
                                        decoder_factory_, config, redis_command_stats, scope, std::move(cp));

  client->initialize(auth_username, auth_password);
  return client;
}

} // namespace Client
} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
