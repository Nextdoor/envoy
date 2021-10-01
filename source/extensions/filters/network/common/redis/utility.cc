#include "source/extensions/filters/network/common/redis/utility.h"

#include "codec.h"
#include "source/common/common/utility.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {
namespace Utility {

AuthRequest::AuthRequest(const std::string& password) {
  std::vector<RespValue> values(2);
  values[0].type(RespType::BulkString);
  values[0].asString() = "auth";
  values[1].type(RespType::BulkString);
  values[1].asString() = password;
  type(RespType::Array);
  asArray().swap(values);
}

AuthRequest::AuthRequest(const std::string& username, const std::string& password) {
  std::vector<RespValue> values(3);
  values[0].type(RespType::BulkString);
  values[0].asString() = "auth";
  values[1].type(RespType::BulkString);
  values[1].asString() = username;
  values[2].type(RespType::BulkString);
  values[2].asString() = password;
  type(RespType::Array);
  asArray().swap(values);
}

HelloRequest::HelloRequest(const RespVersion resp_version) {
  std::vector<RespValue> values(2);
  values[0].type(RespType::BulkString);
  values[0].asString() = "hello";
  values[1].type(RespType::BulkString);

  switch (resp_version) {
    case RespVersion::Resp2:
      values[1].asString() = "2";
      break;
    case RespVersion::Resp3:
      values[1].asString() = "3";
      break;
    default:
      NOT_REACHED_GCOVR_EXCL_LINE;
      break;
  }

  type(RespType::Array);
  asArray().swap(values);
}

ClientTrackingRequest::ClientTrackingRequest(bool enable_bcast_mode) {
  int val_size = 4;
  if (enable_bcast_mode) {
    val_size += 1;
  }

  std::vector<RespValue> values(val_size);
  values[0].type(RespType::BulkString);
  values[0].asString() = "client";
  values[1].type(RespType::BulkString);
  values[1].asString() = "tracking";
  values[2].type(RespType::BulkString);
  values[2].asString() = "on";
  values[3].type(RespType::BulkString);
  values[3].asString() = "noloop";

  if (enable_bcast_mode) {
    values[4].type(RespType::BulkString);
    values[4].asString() = "bcast";
  }

  type(RespType::Array);
  asArray().swap(values);
}

RespValuePtr makeError(const std::string& error) {
  Common::Redis::RespValuePtr response(new RespValue());
  response->type(Common::Redis::RespType::Error);
  response->asString() = error;
  return response;
}

ReadOnlyRequest::ReadOnlyRequest() {
  std::vector<RespValue> values(1);
  values[0].type(RespType::BulkString);
  values[0].asString() = "readonly";
  type(RespType::Array);
  asArray().swap(values);
}

const ReadOnlyRequest& ReadOnlyRequest::instance() {
  static const ReadOnlyRequest* instance = new ReadOnlyRequest{};
  return *instance;
}

AskingRequest::AskingRequest() {
  std::vector<RespValue> values(1);
  values[0].type(RespType::BulkString);
  values[0].asString() = "asking";
  type(RespType::Array);
  asArray().swap(values);
}

const AskingRequest& AskingRequest::instance() {
  static const AskingRequest* instance = new AskingRequest{};
  return *instance;
}

GetRequest::GetRequest() {
  type(RespType::BulkString);
  asString() = "get";
}

const GetRequest& GetRequest::instance() {
  static const GetRequest* instance = new GetRequest{};
  return *instance;
}

SetRequest::SetRequest() {
  type(RespType::BulkString);
  asString() = "set";
}

const SetRequest& SetRequest::instance() {
  static const SetRequest* instance = new SetRequest{};
  return *instance;
}
} // namespace Utility
} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
