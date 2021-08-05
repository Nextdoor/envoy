#pragma once

#include <string>

#include "extensions/filters/network/common/redis/codec.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Common {
namespace Redis {
namespace Utility {

class AuthRequest : public Redis::RespValue {
public:
  AuthRequest(const std::string& username, const std::string& password);
  AuthRequest(const std::string& password);
};

class HelloRequest : public Redis::RespValue {
public:
  HelloRequest();
};

class ClientTrackingRequest : public Redis::RespValue {
public:
  ClientTrackingRequest(bool broadcast);
};

RespValuePtr makeError(const std::string& error);

class ReadOnlyRequest : public Redis::RespValue {
public:
  ReadOnlyRequest();
  static const ReadOnlyRequest& instance();
};

class AskingRequest : public Redis::RespValue {
public:
  AskingRequest();
  static const AskingRequest& instance();
};

class GetRequest : public Redis::RespValue {
public:
  GetRequest();
  static const GetRequest& instance();
};

class SetRequest : public Redis::RespValue {
public:
  SetRequest();
  static const SetRequest& instance();
};

} // namespace Utility
} // namespace Redis
} // namespace Common
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
