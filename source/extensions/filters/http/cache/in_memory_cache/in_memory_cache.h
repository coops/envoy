#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "envoy/extensions/filters/http/cache/cache.proto.h"
#include "envoy/extensions/filters/http/cache/cache_entry_utils.h"
#include "envoy/extensions/filters/http/cache/cache_headers_utils.h"
#include "envoy/extensions/filters/http/cache/http_cache.h"
#include "envoy/extensions/filters/http/cache/key.proto.h"
#include "envoy/extensions/filters/http/cache/range_utils.h"
#include "absl/base/thread_annotations.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "envoy/buffer/buffer.h"
#include "envoy/http/header_map.h"
#include "envoy/server/filter_config.h"
#include "source/common/buffer/buffer_impl.h"
#include "source/common/protobuf/protobuf.h"
#include "source/common/protobuf/utility.h"
#include "envoy/extensions/filters/http/cache/in_memory_cache/concurrent_lru_cache.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace Cache {

class InMemoryCache : public HttpCache {
 public:
  InMemoryCache(
      int shard_count, int64_t max_size_in_bytes,
      absl::Duration last_valid_update_min_interval);

  LookupContextPtr makeLookupContext(LookupRequestPtr&& request) override;
  InsertContextPtr makeInsertContext(
      LookupContextPtr&& lookup_context) override;
  void updateHeaders(const LookupContext& lookup_context,
                     const Envoy::Http::ResponseHeaderMap& headers,
                     const ResponseMetadata& metadata) override;
  CacheInfo cacheInfo() const override;

  int shard_count() const { return cache_.Shards(); }
  int64_t max_size_in_bytes() const { return max_size_in_bytes_; }
  absl::Duration last_valid_update_min_interval() const {
    return last_valid_update_min_interval_;
  }

 private:
  friend class LookupContextImpl;
  friend class InsertContextImpl;

  class LastValid {
   public:
    explicit LastValid(absl::Time value) : value_(value) {}
    absl::Time value() const {
      absl::MutexLock lock(&mutex_);
      return value_;
    }
    void AdvanceTo(absl::Time value) {
      absl::MutexLock lock(&mutex_);
      if (value > value_) {
        value_ = value;
      }
    }

   private:
    mutable absl::Mutex mutex_;
    absl::Time value_ ABSL_GUARDED_BY(mutex_);
  };

  struct NamedTagValues {
    absl::string_view name;
    absl::string_view value;
  };

  // TODO(capoferro): make Entry a class?
  struct Entry {
    const Envoy::Http::ResponseHeaderMapPtr headers;
    const std::string body;
    const Envoy::Http::ResponseTrailerMapPtr trailers;
    const ResponseMetadataPtr metadata;
    LastValid last_valid;
  };

  struct Hasher {
    size_t operator()(const Key& key) const;
  };

  struct Equaler {
    bool operator()(const Key& a, const Key& b) const;
  };

  class LookupContextImpl : public LookupContext {
   public:
    LookupContextImpl(InMemoryCache& cache, LookupRequestPtr&& request);

    void getHeaders(LookupHeadersCallback&& cb) override;
    void getBody(const AdjustedByteRange& range,
                 LookupBodyCallback&& cb) override;
    void getTrailers(LookupTrailersCallback&& cb) override;
    void onDestroy() override;

    LookupRequest& request() { return *request_; }
    LookupRequestPtr releaseRequest() && { return std::move(request_); }
    const Envoy::Http::RequestHeaderMap& request_headers() const override {
      return request_->request_headers();
    }

   private:
    InMemoryCache& cache_;
    LookupRequestPtr request_;
    std::shared_ptr<Entry> entry_;
    bool destroyed_ = false;
  };

  class InsertContextImpl : public InsertContext {
   public:
    InsertContextImpl(LookupRequestPtr&& lookup_request, InMemoryCache& cache)
        : lookup_request_(std::move(lookup_request)), cache_(&cache) {}
    ~InsertContextImpl() override {}

    void insertHeaders(const Envoy::Http::ResponseHeaderMap& headers,
                       ResponseMetadataPtr&& metadata,
                       bool end_stream) override;
    absl::Status insertBody(const Envoy::Buffer::Instance& chunk,
                            bool end_stream) override;
    void insertTrailers(
        const Envoy::Http::ResponseTrailerMap& trailers) override;
    void onDestroy() override {}
    void commit();

   private:
    LookupRequestPtr lookup_request_;
    InMemoryCache* cache_;

    Envoy::Http::ResponseHeaderMapPtr headers_ = nullptr;
    Envoy::Buffer::OwnedImpl body_;
    Envoy::Http::ResponseTrailerMapPtr trailers_ = nullptr;
    ResponseMetadataPtr metadata_ = nullptr;
    bool committed_ = false;
  };

  std::shared_ptr<Entry> lookup(const LookupRequest& request);
  std::shared_ptr<Entry> varyLookup(
      const LookupRequest& request,
      Envoy::Http::ResponseHeaderMap& response_headers);
  std::shared_ptr<InMemoryCache::Entry> makeEntrySharedPtrWithReleaser(
      const Key& key, InMemoryCache::Entry* entry);
  void insert(const Key& key, Envoy::Http::ResponseHeaderMapPtr&& headers,
              std::string&& body, Envoy::Http::ResponseTrailerMapPtr&& trailers,
              ResponseMetadataPtr&& metadata,
              absl::Time last_valid);
  void varyInsert(const Key& key,
                  const Envoy::Http::RequestHeaderMap& request_headers,
                  Envoy::Http::ResponseHeaderMapPtr&& response_headers,
                  std::string&& body,
                  Envoy::Http::ResponseTrailerMapPtr&& response_trailers,
                  ResponseMetadataPtr&& metadata,
                  const VaryHeader& vary_allow_list,
                  absl::Time last_valid);
  void release(const Key& key, Entry* entry);

  static size_t approximateEntrySize(const Key& key, const Entry& entry);
  static std::vector<InMemoryCache::NamedTagValues> extractNamedTags(
      const Envoy::Http::ResponseHeaderMap& headers);

  using Shard = SimpleLRUCache;

  ConcurrentLruCache<Key, std::shared_ptr<Entry>, Hasher> cache_;
  int max_size_in_bytes_;
  absl::Duration last_valid_update_min_interval_;
};

class InMemoryCacheFactory : public HttpCacheFactory {
 public:
  // From UntypedFactory
  std::string name() const override;
  // From TypedFactory
  Envoy::ProtobufTypes::MessagePtr createEmptyConfigProto() override;
  // From HttpCacheFactory
  HttpCacheSharedPtr getCache(
      const ::envoy::extensions::filters::http::cache::v3::
          CacheConfig& config,
      Envoy::Server::Configuration::FactoryContext& context) override;

  void resetCacheForTests() ABSL_LOCKS_EXCLUDED(create_cache_mutex_);

 private:
  HttpCacheSharedPtr cache_ = nullptr;
  absl::Mutex create_cache_mutex_;
};

} // namespace Cache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
