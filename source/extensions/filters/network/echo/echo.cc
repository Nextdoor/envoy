#include "source/extensions/filters/network/echo/echo.h"

#include "envoy/buffer/buffer.h"
#include "envoy/network/connection.h"

#include "source/common/common/assert.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace Echo {

Network::FilterStatus EchoFilter::onData(Buffer::Instance& data, bool end_stream) {
  ENVOY_LOG(debug, "===== In echo filter on data");
  ENVOY_CONN_LOG(trace, "echo: got {} bytes", read_callbacks_->connection(), data.length());
  read_callbacks_->connection().write(data, end_stream);
  ASSERT(0 == data.length());
  ENVOY_LOG(debug, "===== In echo filter on data. continue with the filter iteration");
  return Network::FilterStatus::Continue;
}

Network::FilterStatus EchoFilter::onWrite(Buffer::Instance& data, bool end_stream){
  ENVOY_LOG(debug, "===== In echo filter on write");
  ENVOY_CONN_LOG(trace, "echo: got {} bytes", write_callbacks_->connection(), data.length());
  write_callbacks_->connection().write(data, end_stream);
  ENVOY_LOG(debug, "===== In echo filter on write. continue with the filter iteration");
  return Network::FilterStatus::Continue;
}

} // namespace Echo
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
