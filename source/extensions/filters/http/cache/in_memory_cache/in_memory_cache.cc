#include "envoy/extensions/filters/http/cache/in_memory_cache/in_memory_cache.h"

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "envoy/extensions/filters/http/cache/cache.proto.h"
#include "envoy/extensions/filters/http/cache/cache_entry_utils.h"
#include "envoy/extensions/filters/http/cache/cache_headers_utils.h"
#include "envoy/extensions/filters/http/cache/http_cache.h"
#include "envoy/extensions/filters/http/cache/in_memory_cache/config.proto.h"
#include "envoy/extensions/filters/http/cache/in_memory_cache/config.proto.validator.h"
#include "envoy/extensions/filters/http/cache/key.proto.h"
#include "envoy/extensions/filters/http/cache/range_utils.h"
#include "absl/container/btree_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "envoy/buffer/buffer.h"
#include "envoy/common/exception.h"
#include "envoy/http/header_map.h"
#include "envoy/registry/registry.h"
#include "envoy/server/filter_config.h"
#include "envoy/server/options.h"
#include "source/common/buffer/buffer_impl.h"
#include "source/common/common/assert.h"
#include "source/common/http/header_map_impl.h"
#include "source/common/http/headers.h"
#include "source/common/protobuf/protobuf.h"
#include "source/common/protobuf/utility.h"
#include "envoy/extensions/filters/http/cache/in_memory_cache/concurrent_lru_cache.h"

using ::Envoy::MessageUtil;
using ::envoy::extensions::filters::http::cache::v3::CacheConfig;

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace Cache {
namespace {

// Process vary-relevant headers to create a key containing a unique vary
// identifier to add to vary entries. Returns an absl::nullopt if no valid vary
// key can be created and the response should not be cached (eg. when
// disallowed vary headers are present in the response).
absl::optional<Key> createVaryKey(
    const VaryHeader& vary_allow_list,
    const Http::RequestHeaderMap& request_headers,
    const absl::btree_set<absl::string_view>& vary_header_values,
    const Key& key) {
  Key vary_key = key;
  absl::optional<std::string> variant_identifier =
      vary_allow_list.createVaryIdentifier(vary_header_values, request_headers);
  if (!variant_identifier.has_value()) {
    // nullopt means we can't create a vary key and so should not insert the
    // entry.
    return absl::nullopt;
  }
  vary_key.add_custom_fields(variant_identifier.value());
  return vary_key;
}

std::unique_ptr<InMemoryCache> CreateCache(
    const CacheConfig& config,
    Server::Configuration::FactoryContext& context) {
  InMemoryCacheConfig imc_config;
  MessageUtil::unpackTo(config.typed_config(), imc_config);
  if (absl::Status status = protobuf::contrib::validator::Validate(imc_config);
      !status.ok()) {
    throw EnvoyException(
        absl::StrCat("invalid InMemoryCacheConfig: ", status.ToString()));
  }
  return absl::make_unique<InMemoryCache>(
      imc_config.shard_count(), imc_config.max_size_in_bytes());
}

}  // namespace

InMemoryCache::InMemoryCache(int shard_count, int64_t max_size_in_bytes)
    : cache_(shard_count, max_size_in_bytes / shard_count),
      max_size_in_bytes_(max_size_in_bytes) {
  testing::testvalue::Adjust<void>("InMemoryCache::InMemoryCache", nullptr);
}

LookupContextPtr InMemoryCache::makeLookupContext(LookupRequestPtr&& request) {
  return std::make_unique<InMemoryCache::LookupContextImpl>(*this,
                                                            std::move(request));
}

InMemoryCache::LookupContextImpl::LookupContextImpl(InMemoryCache& cache,
                                                    LookupRequestPtr&& request)
    : cache_(cache),
      request_(std::move(request)),
      entry_(cache_.lookup(*request_)) {}

void InMemoryCache::LookupContextImpl::getHeaders(LookupHeadersCallback&& cb) {
  if (entry_ == nullptr) {
    cb(LookupResult{.cache_entry_status_ = CacheEntryStatus::Unusable});
    return;
  }

  LookupResult lookup_result = request_->makeLookupResult(
      Http::createHeaderMap<Http::ResponseHeaderMapImpl>(
          *entry_->headers),
      ResponseMetadata{entry_->metadata->response_time_},
      entry_->trailers != nullptr, entry_->body.size());

  if (lookup_result.cache_entry_status_ ==
      CacheEntryStatus::RequiresValidation) {
    // TODO (capoferro): InMemoryCache does not implement updateHeaders, so
    // treat validation results as misses.
    lookup_result.cache_entry_status_ = CacheEntryStatus::Unusable;
  }

  const Key& key = request_->key();
}

void InMemoryCache::LookupContextImpl::getBody(const AdjustedByteRange& range,
                                               LookupBodyCallback&& cb) {
  if (entry_ == nullptr) {
    ENVOY_BUG(false, "getBody() should not be called if the entry was not found.");
    cb(nullptr);
    return;
  }

  uint64_t begin = range.begin();
  uint64_t length = range.length();

  if (begin > entry_->body.size()) {
    ENVOY_BUG(false, "Attempted to begin read past end of the body.");
    cb(nullptr);
    return;
  } else if (range.end() > entry_->body.size()) {
    ENVOY_BUG(false, "Attempted to end read past the end of the body.");
    cb(nullptr);
    return;
  }

  // By manually creating BufferFragmentImpls here, we give the OwnedImpl slices
  // that do not own or copy the bytes, thus allowing us to serve directly from
  // the cached entry_.
  // TODO (capoferro): Tune fragment size.
  Buffer::BufferFragmentImpl* fragment =
      new Buffer::BufferFragmentImpl(
          &entry_->body[begin], length,
          [](const void*, size_t,
             const Buffer::BufferFragmentImpl* this_fragment) {
            // We only delete the fragment. We do not want to delete the data,
            // as it is not owned by the fragment.
            delete this_fragment;
          });
  auto buffer = absl::make_unique<Buffer::OwnedImpl>();
  buffer->addBufferFragment(*fragment);
  cb(std::move(buffer));
}

void InMemoryCache::LookupContextImpl::getTrailers(
    LookupTrailersCallback&& cb) {
  if (entry_ == nullptr) {
    ENVOY_BUG(false, "getTrailers() should not be called if the entry was not found.");
    cb(nullptr);
    return;
  }

  if (entry_->trailers == nullptr) {
    ENVOY_BUG(false, "Called getTrailers for an entry that does not actually have trailers.");
    cb(nullptr);
    return;
  }

  cb(Http::createHeaderMap<Http::ResponseTrailerMapImpl>(
      *entry_->trailers));
}

void InMemoryCache::LookupContextImpl::onDestroy() {
  if (destroyed_) {
    ENVOY_BUG(false, "We should not destroy a LookupContextImpl twice.");
  }
  // Envoy defers deleting filters, so we release the entry in onDestroy rather
  // than the destructor of LookupContextImpl to unpin the entry as soon as
  // we're done with it.
  entry_ = nullptr;
  destroyed_ = true;
}

InsertContextPtr InMemoryCache::makeInsertContext(
    LookupContextPtr&& lookup_context) {
  ASSERT(lookup_context != nullptr);
  LookupRequestPtr lookup_request =
      std::move(down_cast<InMemoryCache::LookupContextImpl&>(*lookup_context))
          .releaseRequest();
  auto insert_context = std::make_unique<InMemoryCache::InsertContextImpl>(
      std::move(lookup_request), *this);

  // We're done with the lookup context, so clean it up now rather than waiting
  // for the insert to complete.
  lookup_context->onDestroy();

  return insert_context;
}

void InMemoryCache::InsertContextImpl::insertHeaders(
    const Http::ResponseHeaderMap& headers,
    ResponseMetadataPtr&& metadata, bool end_stream) {
  if (committed_) {
    ENVOY_BUG(false, "Attempted to insertHeaders() after commit()");
    return;
  }

  headers_ =
      Http::createHeaderMap<Http::ResponseHeaderMapImpl>(headers);
  metadata_ = std::move(metadata);
  if (end_stream) {
    commit();
  }
}

absl::Status InMemoryCache::InsertContextImpl::insertBody(
    const Buffer::Instance& chunk, bool end_stream) {
  if (committed_) {
    ENVOY_BUG(false, "Attempted to insertBody() after commit()");
    return absl::FailedPreconditionError(
        "Attempted to insertBody() after commit()");
  }

  body_.add(chunk);
  if (end_stream) {
    commit();
  }
  return absl::OkStatus();
}

void InMemoryCache::InsertContextImpl::insertTrailers(
    const Http::ResponseTrailerMap& trailers) {
  if (committed_) {
    ENVOY_BUG(false, "Attempted to insertTrailers() after commit()");
    return;
  }
  trailers_ = Http::createHeaderMap<Http::ResponseTrailerMapImpl>(
      trailers);
  commit();
}

void InMemoryCache::InsertContextImpl::commit() {
  if (committed_) {
    ENVOY_BUG(false, "Multiple commit()");
    return;
  }
  committed_ = true;

  if (VaryHeader::hasVary(*headers_)) {
    cache_->varyInsert(lookup_request_->key(),
                       lookup_request_->request_headers(), std::move(headers_),
                       body_.toString(), std::move(trailers_),
                       std::move(metadata_), lookup_request_->vary_allow_list());
  } else {
    cache_->insert(lookup_request_->key(), std::move(headers_),
                   body_.toString(), std::move(trailers_), std::move(metadata_));
  }
}

void InMemoryCache::updateHeaders(const LookupContext& lookup_context,
                                  const Http::ResponseHeaderMap& headers,
                                  const ResponseMetadata& metadata) {
  // TODO (capoferro): Support entry validation
}

std::shared_ptr<InMemoryCache::Entry> InMemoryCache::lookup(
    const LookupRequest& request) {
  std::shared_ptr<InMemoryCache::Entry> entry = cache_.Lookup(request.key());
  if (entry == nullptr) {
    return nullptr;
  }
  if (VaryHeader::hasVary(*entry->headers)) {
    std::shared_ptr<InMemoryCache::Entry> vary_resolved_entry =
        varyLookup(request, *entry->headers.get());

    return vary_resolved_entry;
  }

  return entry;
}

std::shared_ptr<InMemoryCache::Entry> InMemoryCache::varyLookup(
    const LookupRequest& request,
    Http::ResponseHeaderMap& response_headers) {
  const absl::btree_set<absl::string_view> vary_header_values =
      VaryHeader::getVaryValues(response_headers);
  response_headers.get(Http::CustomHeaders::get().Vary);
  if (vary_header_values.empty()) {
    ENVOY_BUG(false, "Can't perform a vary lookup without a vary header.");
    return nullptr;
  }

  absl::optional<Key> vary_key =
      createVaryKey(request.vary_allow_list(), request.request_headers(),
                    vary_header_values, request.key());
  if (!vary_key.has_value()) {
    return nullptr;
  }
  return cache_.Lookup(vary_key.value());
}

void InMemoryCache::insert(
    const Key& key, Http::ResponseHeaderMapPtr&& headers,
    std::string&& body, Http::ResponseTrailerMapPtr&& trailers,
    ResponseMetadataPtr&& metadata) {
  std::shared_ptr<Entry> entry = std::make_shared<InMemoryCache::Entry>({
      .headers = std::move(headers),
      .body = std::move(body),
      .trailers = std::move(trailers),
      .metadata = std::move(metadata)});
  cache_.Insert(key, entry, approximateEntrySize(key, *entry));
}

void InMemoryCache::varyInsert(
    const Key& key, const Http::RequestHeaderMap& request_headers,
    Http::ResponseHeaderMapPtr&& response_headers, std::string&& body,
    Http::ResponseTrailerMapPtr&& response_trailers,
    ResponseMetadataPtr&& metadata, const VaryHeader& vary_allow_list) {
  const absl::btree_set<absl::string_view> vary_header_values =
      VaryHeader::getVaryValues(*response_headers);
  if (vary_header_values.empty()) {
    ENVOY_BUG(false, "Can't perform a vary insert without a vary header. varyInsert "
              "should not be called without a vary header present.");
    return;
  }

  bool should_insert_placeholder;
  if (std::shared_ptr<Entry> placeholder_entry = cache_.Lookup(key);
      placeholder_entry == nullptr) {
    should_insert_placeholder = true;
  } else {
    absl::btree_set<absl::string_view> placeholder_vary_header_values =
        VaryHeader::getVaryValues(*placeholder_entry->headers);
    if (placeholder_vary_header_values.empty()) {
      // We've encountered an invalid placeholder. Replace it.
      ENVOY_BUG(false, "A placeholder entry with no vary header has been retrieved "
                "during varyInsert. This shouldn't happen. \nKey:\n" + key.DebugString());
      should_insert_placeholder = true;
    } else {
      should_insert_placeholder =
          placeholder_vary_header_values != vary_header_values;
    }
    // TODO(capoferro): When a new vary header is received, We could also
    // proactively delete the existing vary-differentiated entries, as
    // subsequent lookups for those will miss due to the new placeholder.
    placeholder_entry == nullptr;
  }

  absl::optional<Key> vary_key =
      createVaryKey(vary_allow_list, request_headers, vary_header_values, key);
  if (!vary_key.has_value()) {
    // TODO(capoferro): Remove the vary placeholder and all associated vary
    // entries as we know that they will not be served from cache now that the
    // key cannot be generated.
    return;
  }
  std::shared_ptr<Entry> entry = std::make_shared<InMemoryCache::Entry>({
      .headers = std::move(response_headers),
      .body = std::move(body),
      .trailers = std::move(response_trailers),
      .metadata = std::move(metadata)});
  cache_.Insert(vary_key.value(), entry,
                approximateEntrySize(vary_key.value(), *entry));

  // Add a placeholder entry to indicate that this request generates
  // vary-differentiated responses and we should resolve the vary entry on
  // subsequent cache hits for this resource.
  if (should_insert_placeholder) {
    Http::ResponseHeaderMapPtr vary_only_map =
        Http::createHeaderMap<Http::ResponseHeaderMapImpl>({});
    vary_only_map->addCopy(Http::CustomHeaders::get().Vary,
                           absl::StrJoin(vary_header_values, ","));
    // TODO(cbdm): In a cache that evicts entries, we could maintain a list of
    // the "varykey"s that we have inserted as the body for this first lookup.
    // This way, we would know which keys we have inserted for that resource.
    // For the first entry simply use vary_key as the entry_list, for future
    // entries append vary_key to existing list.
    std::string entry_list;

    // TODO(capoferro): Formalize "placeholder" type, rather than having a
    // half empty Entry object with unrelated fields. Placeholder objects can
    // just store the vector of vary values rather than rejoining the vary
    // values to a string to fit into an HTTP header object.
    std::shared_ptr<Entry> new_placeholder_entry =
        std::make_shared<InMemoryCache::Entry>({.headers = std::move(vary_only_map),
                                 .body = std::move(entry_list),
                                 .trailers = nullptr,
                                 .metadata = nullptr});
    cache_.Insert(key, new_placeholder_entry,
                  approximateEntrySize(key, *new_placeholder_entry));
  }
}

CacheInfo InMemoryCache::cacheInfo() const {
  return {"Envoy InMemoryCache", /*support_range_request=*/true};
}

// static
size_t InMemoryCache::approximateEntrySize(const Key& key, const Entry& entry) {
  size_t entry_size = sizeof(key) + sizeof(Entry) + entry.body.size();
  if (entry.headers != nullptr) {
    entry_size += entry.headers->byteSize();
  }
  if (entry.trailers != nullptr) {
    entry_size += entry.trailers->byteSize();
  }
  if (entry.metadata != nullptr) {
    entry_size += sizeof(*entry.metadata);
  }
  return entry_size;
}

size_t InMemoryCache::Hasher::operator()(const Key& key) const {
  return stableHashKey(key);
}
bool InMemoryCache::Equaler::operator()(const Key& a, const Key& b) const {
  return ProtobufUtil::MessageDifferencer::Equivalent(a, b);
}

std::string InMemoryCacheFactory::name() const {
  return "envoy.in_memory_cache";
}

ProtobufTypes::MessagePtr
InMemoryCacheFactory::createEmptyConfigProto() {
  return std::make_unique<InMemoryCacheConfig>();
}

HttpCacheSharedPtr InMemoryCacheFactory::getCache(
    const ::envoy::extensions::filters::http::cache_fork::v3alpha::CacheConfig&
        config,
    Server::Configuration::FactoryContext& context) {
  // Common fast path. Snapshotting cache_ without explicit synchronization is
  // thread-safe due to shared_ptr refcount atomicity. However, any further
  // checks/returns must be performed using the snapshot, in case a racing
  // thread is resetting cache_.
  if (HttpCacheSharedPtr cache_snapshot = cache_; cache_snapshot != nullptr) {
    return cache_snapshot;
  }

  // cache_ was nullptr at time of snapshot, but could have changed in the
  // meantime. Perform cache instantiation under a mutex, to ensure it is only
  // performed once in the face of racing threads. This is effectively the
  // double-checked locking idiom.
  absl::MutexLock lock(&create_cache_mutex_);
  if (cache_ == nullptr) {
    cache_ = CreateCache(config, context);
  }
  return cache_;
}

void InMemoryCacheFactory::resetCacheForTests() {
  absl::MutexLock lock(&create_cache_mutex_);
  cache_ = nullptr;
}

static Registry::RegisterFactory<InMemoryCacheFactory, HttpCacheFactory>
    register_;  // NOLINT

} // namespace Cache
} // namespace HttpFilters
} // namespace Extensions
} // namespace Envoy
