#pragma once

#include <cstdint>

#include "absl/base/internal/spinlock.h"
#include "absl/base/thread_annotations.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/fixed_array.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/internal/raw_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/memory/memory.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"

namespace Envoy {
namespace Extensions {
namespace HttpFilters {
namespace Cache {

using absl::base_internal::SpinLockHolder;
using absl::base_internal::SpinLock;
using absl::base_internal::CompressedTuple;

// An LRU cache for highly concurrent use cases.
// - Does not support idle eviction.
// - It always copies the value on Lookup. To avoid copies of large structures,
//   consider using shared_ptr or ref-counted pointers.
template <typename Key, typename Value,
          typename Hash = typename absl::flat_hash_set<Key>::hasher,
          typename Eq = typename absl::flat_hash_set<Key, Hash>::key_equal>
class ConcurrentLruCache {
  template <typename K>
  using key_arg =
      typename absl::flat_hash_set<Key, Hash, Eq>::template key_arg<K>;

 public:
  // Creates an LRU cache with `next_power_of_2(num_shards)` of shards. Each
  // shard contains up to `max_per_shard` values.
  //
  // `max_per_shard` of 0 means unlimited capacity for this LRU cache (i.e., no
  // eviction).
  ConcurrentLruCache(uint32_t num_shards, uint32_t max_per_shard)
      : hash_and_max_per_shard_(Hash(), max_per_shard),
        shards_mask_(ShardsMask(num_shards)),
        shards_(shards_mask_ + 1) {}

  ConcurrentLruCache(const ConcurrentLruCache&) = delete;
  const ConcurrentLruCache& operator=(const ConcurrentLruCache&) = delete;
  ConcurrentLruCache(ConcurrentLruCache&&) = delete;
  const ConcurrentLruCache& operator=(ConcurrentLruCache&&) = delete;

  // Lookup the `key` and return the `value`.
  // Note that `value` is always copied.
  template <typename K = Key>
  absl::optional<Value> Lookup(const key_arg<K>& key) {
    Shard& shard = FindShard(key);

    absl::ReaderMutexLock lock(&shard.mu);
    auto it = shard.set.find(key);
    if (it == shard.set.end()) {
      return absl::nullopt;
    }

    // No need to contend on the spin lock to update the sorted list,
    // when there is only one entry in this shard or when the LRU is unlimited.
    if (shard.set.size() == 1 || max_per_shards() == 0) {
      return (**it).second;
    }

    SpinLockHolder spin_lock(&shard.spin_lock);
    auto list_it = *it;
    // Move the looked up element to the front of the list.
    shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list, list_it);
    return list_it->second;
  }

  // Inserts `key` and `value` in the cache.
  // Returns whether the `key` and `value` pair was successfully inserted.
  // TODO(soheil): Maybe add move and perfect forwarding capabilities if needed.
  bool Insert(const Key& key, const Value& value) {
    Shard& shard = FindShard(Key(key));

    absl::MutexLock lock(&shard.mu);
    bool inserted = false;
    shard.set.lazy_emplace(
        key, [&](const auto& ctor_func) ABSL_NO_THREAD_SAFETY_ANALYSIS {
          // Unfortunately, thread-safety analysis doesn't work well with
          // shard.mu locked outside the scope. So, we need to resort to runtime
          // assertions.
          DCHECK((shard.mu.AssertHeld(), true));
          inserted = true;
          SpinLockHolder spin_lock(&shard.spin_lock);
          shard.lru_list.push_front({key, value});
          ctor_func(shard.lru_list.begin());
          if (uint32_t cap = max_per_shards();
              cap != 0 && shard.lru_list.size() > cap) {
            RemoveOldestNode(&shard);
          }
        });
    return inserted;
  }

  // Lookup the `key` and return the `value` if it exits.  Otherwise, call
  // `func` to create the `value`, insert it in the cache, and return it.
  //
  // `func` must be a callable object, accepting no arguments and returning
  // Value, like std::function<Value()>.
  template <typename F, typename K = Key>
  Value LookupOrInsertNew(const key_arg<K>& key, F func) {
    Shard& shard = FindShard(key);

    absl::MutexLock lock(&shard.mu);
    auto it = shard.set.lazy_emplace(
        key, [&](const auto& ctor_func) ABSL_NO_THREAD_SAFETY_ANALYSIS {
          // Unfortunately, thread-safety analysis doesn't work well with
          // shard.mu locked outside the scope. So, we need to resort to runtime
          // assertions.
          DCHECK((shard.mu.AssertHeld(), true));
          SpinLockHolder spin_lock(&shard.spin_lock);
          shard.lru_list.push_front({Key(key), func()});
          ctor_func(shard.lru_list.begin());
          if (uint32_t cap = max_per_shards();
              cap != 0 && shard.lru_list.size() > cap) {
            RemoveOldestNode(&shard);
          }
        });
    return (*it)->second;
  }

  // Removes `key` from the LRU cache.
  bool Remove(const Key& key) {
    Shard& shard = FindShard(key);

    absl::MutexLock lock(&shard.mu);
    auto node = shard.set.extract(key);
    if (node.empty()) return false;

    SpinLockHolder spin_lock(&shard.spin_lock);
    shard.lru_list.erase(node.value());
    return true;
  }

  // DO NOT interact with the cache in the callable at the risk of deadlock.
  // Calls a non-modifying callable on each element in the cache. Useful for
  // saving the contents of the cache, computing statistics on the cached
  // elements, etc. Concurrent modifications may or may not be seen by this
  // function.  All calls to func are guaranteed to see values that were in the
  // table at one point in time, but no further guarantees are provided,
  // including the order in which elements are accessed.
  template <typename F>
  void ForEach(F func) const {
    for (const Shard& shard : shards_) {
      absl::ReaderMutexLock lock(&shard.mu);
      for (const auto& it : shard.set) func(it->first, it->second);
    }
  }

  // Clears the map. Note that this methods clears the cache shard-by-shard.
  // If keys are inserted in parallel to Clear(), it is not guaranteed that
  // the cache is empty when Clear() returns.
  void Clear() {
    for (Shard& shard : shards_) {
      absl::MutexLock lock(&shard.mu);
      SpinLockHolder spin_lock(&shard.spin_lock);
      shard.set.clear();
      shard.lru_list.clear();
    }
  }

  // Returns the approximate size of this cache. The returned value is just
  // estimate, because it reads the size of each shard separately. Thus, if the
  // cache is modified in parallel, the size can be inaccurate.
  size_t ApproximateSize() const {
    size_t sum = 0;
    for (const Shard& shard : shards_) {
      absl::ReaderMutexLock lock(&shard.mu);
      sum += shard.set.size();
    }
    return sum;
  }

 private:
  struct Shard {
    using Element = std::pair<const Key, Value>;
    using List = std::list<Element>;
    template <class Fn>
    struct Wrapped {
      using is_transparent = void;

      template <typename K>
      static const K& ToKey(const K& k) {
        return k;
      }
      static const Key& ToKey(typename List::const_iterator it) {
        return it->first;
      }
      static const Key& ToKey(typename List::iterator it) { return it->first; }

      Wrapped() = default;
      explicit Wrapped(Fn fn) : fn(std::move(fn)) {}

      Fn fn;

      template <class... Args>
      auto operator()(Args&&... args) const
          -> decltype(this->fn(ToKey(args)...)) {
        return fn(ToKey(args)...);
      }
    };
    using Set = absl::flat_hash_set<typename List::iterator, Wrapped<Hash>,
                                    Wrapped<Eq>>;

    mutable absl::Mutex mu;
    Set set ABSL_GUARDED_BY(mu);

    SpinLock spin_lock ABSL_ACQUIRED_AFTER(mu);
    // Ordered from most recently used at the front to least recently used at
    // the back.
    List lru_list ABSL_GUARDED_BY(spin_lock);
  };

  static uint32_t ShardsMask(uint32_t n) {
    DCHECK_GT(n, 0);
    if (n == 1) return 0;
    return ~uint32_t{} >> absl::countl_zero(n - 1);
  }

  template <typename K = Key>
  uint32_t ShardIndex(const key_arg<K>& k) const {
    const uint64_t h = hash_ref()(k);
    // Add a salt to force an extra round of mixing when selecting the shard.
    // This prevents shard selection from using the same bits as the hashtable
    // which has disastrous performance consequences.
    return absl::Hash<std::pair<uint64_t, uint64_t>>{}({h, 42}) & shards_mask_;
  }

  template <typename K = Key>
  Shard& FindShard(const key_arg<K>& k) {
    return shards_[ShardIndex(k)];
  }

  void RemoveOldestNode(Shard* shard)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(shard->mu, shard->spin_lock) {
    DCHECK(!shard->lru_list.empty());
    const auto& it = shard->lru_list.back();
    shard->set.erase(it.first);
    shard->lru_list.pop_back();
  }

  const Hash& hash_ref() const {
    return hash_and_max_per_shard_.template get<0>();
  }
  uint32_t max_per_shards() const {
    return hash_and_max_per_shard_.template get<1>();
  }

  CompressedTuple<Hash, uint32_t> hash_and_max_per_shard_;
  const uint32_t shards_mask_;
  absl::FixedArray<Shard> shards_;
};

} // namespace Envoy
} // namespace Extensions
} // namespace HttpFilters
} // namespace Cache
