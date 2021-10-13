#include "util/cache/concurrent_lru_cache.h"

#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/blocking_counter.h"
// TODO #include "thread/fiber/bundle.h"
// TODO #include "thread/threadpool.h"

namespace {

using testing::Eq;
using testing::Optional;
using testing::Pair;
using testing::SizeIs;
using testing::UnorderedElementsAre;
using Envoy::Extensions::HttpFilters::Cache::ConcurrentLruCache;

TEST(ConcurrentLruCache, SingleShard) {
  ConcurrentLruCache<std::string, std::string> cache(1, 1);
  cache.Insert("k1", "v1");
  ASSERT_THAT(cache.Lookup("k1"), Optional(std::string("v1")));

  cache.Insert("k2", "v2");
  ASSERT_THAT(cache.Lookup("k1"), Eq(absl::nullopt));
  ASSERT_THAT(cache.Lookup("k2"), Optional(std::string("v2")));

  cache.Insert("k1", "v3");
  ASSERT_THAT(cache.Lookup("k1"), Optional(std::string("v3")));
  ASSERT_THAT(cache.Lookup("k2"), Eq(absl::nullopt));
}

TEST(ConcurrentLruCache, Duplicates) {
  ConcurrentLruCache<std::string, std::string> cache(1024, 1024);

  ASSERT_TRUE(cache.Insert("k11", "v11"));
  ASSERT_FALSE(cache.Insert("k11", "v12"));
}

TEST(ConcurrentLruCache, Unlimited) {
  ConcurrentLruCache<std::string, std::string> cache(1024, 0);

  ASSERT_TRUE(cache.Insert("k11", "v11"));
  ASSERT_FALSE(cache.Insert("k11", "v12"));
}

TEST(ConcurrentLruCache, HetergenousLookup) {
  ConcurrentLruCache<std::string, std::string> cache(1024, 1024);

  ASSERT_TRUE(cache.Insert("k11", "v11"));
  ASSERT_THAT(cache.Lookup(absl::string_view("k11")), Eq(std::string("v11")));
}

TEST(ConcurrentLruCache, LookupOrInsertNew) {
  ConcurrentLruCache<std::string, std::string> cache(1024, 1024);

  ASSERT_TRUE(cache.Insert("k11", "v11"));
  ASSERT_THAT(cache.LookupOrInsertNew(absl::string_view("k11"),
                                      [] { return "invalid"; }),
              Eq(std::string("v11")));
  ASSERT_THAT(cache.LookupOrInsertNew(absl::string_view("k12"),
                                      [] { return "v12"; }),
              Eq(std::string("v12")));
}

TEST(ConcurrentLruCache, ApplyFunc) {
  ConcurrentLruCache<int, int> cache(1024, 1024);
  ASSERT_TRUE(cache.Insert(5, 55));
  ASSERT_TRUE(cache.Insert(1, 11));
  ASSERT_EQ(cache.ApproximateSize(), 2);
  std::vector<std::pair<int, int>> kvs;
  cache.ForEach([&kvs](int k, int v) { kvs.emplace_back(k, v); });
  EXPECT_THAT(kvs, UnorderedElementsAre(Pair(5, 55), Pair(1, 11)));
}

TEST(ConcurrentLruCache, Removal) {
  ConcurrentLruCache<std::string, std::string> cache(1024, 1024);

  ASSERT_FALSE(cache.Remove("k1"));
  ASSERT_TRUE(cache.Insert("k1", "v1"));
  ASSERT_TRUE(cache.Remove("k1"));
  ASSERT_FALSE(cache.Remove("k1"));
  ASSERT_EQ(cache.ApproximateSize(), 0);
}

TEST(ConcurrentLruCache, Clear) {
  ConcurrentLruCache<std::string, std::string> cache(1024, 1024);

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(cache.Insert(absl::StrCat("k", i), "v"));
  }
  cache.Clear();
  ASSERT_EQ(cache.ApproximateSize(), 0);
}

struct TestKey {
  using is_transparent = void;

  bool operator==(const TestKey& o) const { return val == o.val; }

  int shard;
  int val;
};

struct DeterministicHash {
  size_t operator()(const TestKey& key) const { return key.shard; }
};

TEST(ConcurrentLruCache, ShardLimit) {
  // We use keys that always lands on the same shard.  We should have at most
  // two entries in the LRU cache.
  ConcurrentLruCache<TestKey, std::string, DeterministicHash,
                     std::equal_to<TestKey>>
      cache(2, 2);

  TestKey k1{1, 1};
  TestKey k2{1, 2};
  TestKey k3{1, 3};

  cache.Insert(k1, "v1");
  ASSERT_THAT(cache.Lookup(k1), Optional(std::string("v1")));
  ASSERT_EQ(cache.ApproximateSize(), 1);

  cache.Insert(k2, "v2");
  ASSERT_THAT(cache.Lookup(k1), Optional(std::string("v1")));
  ASSERT_THAT(cache.Lookup(k2), Optional(std::string("v2")));
  ASSERT_EQ(cache.ApproximateSize(), 2);

  cache.Insert(k3, "v3");
  ASSERT_THAT(cache.Lookup(k1), Eq(absl::nullopt));
  ASSERT_THAT(cache.Lookup(k2), Optional(std::string("v2")));
  ASSERT_THAT(cache.Lookup(k3), Optional(std::string("v3")));
  ASSERT_EQ(cache.ApproximateSize(), 2);

  // Access k2 so that it becomes the most recently used element.
  cache.Lookup(k2);
  cache.Insert(k1, "v1");
  ASSERT_THAT(cache.Lookup(k1), Optional(std::string("v1")));
  ASSERT_THAT(cache.Lookup(k2), Optional(std::string("v2")));
  ASSERT_THAT(cache.Lookup(k3), Eq(absl::nullopt));
}

TEST(ConcurrentLruCache, Capacity) {
  auto insert_random_elements = [](ConcurrentLruCache<int, int>* cache) {
    for (int i = 0; i < 1024; ++i) {
      cache->Insert(i, i);
    }
    std::vector<int> elements;
    for (int i = 0; i < 1024; ++i) {
      if (!cache->Lookup(i).has_value()) continue;
      elements.push_back(i);
    }
    return elements;
  };

  {
    ConcurrentLruCache<int, int> cache(2, 2);
    ASSERT_THAT(insert_random_elements(&cache), SizeIs(4));
    ASSERT_EQ(cache.ApproximateSize(), 4);
  }
  {
    // Number of shards is rounded to the next power of 2.
    // So the capacity is 4x2.
    ConcurrentLruCache<int, int> cache(3, 2);
    ASSERT_THAT(insert_random_elements(&cache), SizeIs(8));
    ASSERT_EQ(cache.ApproximateSize(), 8);
  }
}

TEST(ConcurrentLruCache, MultiThreaded) {
  // Do various operations in different threads so that races can be detected by
  // TSAN.
  ConcurrentLruCache<std::string, std::string> cache(8, 8);

  // TODO(capoferro): Find alternative
  ThreadPool thread_pool(32);
  thread_pool.StartWorkers();
  for (int i = 0; i < 4096; ++i) {
    thread_pool.Schedule([&cache, i] {
      cache.Insert(absl::StrCat("k", i), absl::StrCat("v", i));
    });
    thread_pool.Schedule([&cache, i] {
      absl::optional<std::string> v = cache.Lookup(absl::StrCat("k", i));
      // If we happen to find the value in the cache, it must be "v${i}".
      if (v.has_value()) ASSERT_EQ(v.value(), absl::StrCat("v", i));
    });
    thread_pool.Schedule([&cache, i] { cache.Remove(absl::StrCat("k", i)); });
  }
}

TEST(ConcurrentLruCache, Lru) {
  // Only one shard with up to 4 elements.
  ConcurrentLruCache<int, int> cache(1, 4);
  for (int i = 0; i < 8; ++i) {
    cache.Insert(i, i);
  }
  EXPECT_EQ(cache.ApproximateSize(), 4);
  for (int i = 0; i < 4; ++i) {
    EXPECT_THAT(cache.Lookup(i), Eq(absl::nullopt));
  }
  for (int i = 4; i < 8; ++i) {
    EXPECT_THAT(cache.Lookup(i), Optional(i));
  }

  EXPECT_THAT(cache.Lookup(5), Optional(5));

  // Right now, LRU list should be [5, 7, 6, 4].
  cache.Insert(8, 8);
  cache.Insert(9, 9);
  cache.Insert(10, 10);
  // [7, 6, 4] should have been evicted.
  for (int i = 0; i < 5; ++i) {
    EXPECT_THAT(cache.Lookup(i), Eq(absl::nullopt));
  }
  EXPECT_THAT(cache.Lookup(5), Optional(5));
  for (int i = 6; i < 8; ++i) {
    EXPECT_THAT(cache.Lookup(i), Eq(absl::nullopt));
  }
  for (int i = 8; i < 11; ++i) {
    EXPECT_THAT(cache.Lookup(i), Optional(i));
  }
}

void BM_Insert(benchmark::State& state) {
  const int num_shards = state.range(0);
  const int max_per_shards = state.range(1);
  ConcurrentLruCache<int64_t, int64_t> cache(num_shards, max_per_shards);

  int64_t key = state.thread_index() * num_shards;
  for (auto _ : state) {
    cache.Insert(key, key);
    ++key;
  }
}
BENCHMARK(BM_Insert)->ThreadRange(1, 64)->RangePair(1, 128,    // num_shards
                                                    0, 1024);  // max_per_shards

void BM_Lookup(benchmark::State& state) {
  const int num_shards = state.range(0);
  const int max_per_shards = state.range(1);
  ConcurrentLruCache<int64_t, int64_t> cache(num_shards, max_per_shards);

  // This doesn't mean the LRU cache is completely full, but will make it
  // reasonably full.
  for (int i = 0; i < num_shards * max_per_shards; ++i) {
    cache.Insert(i, i);
  }

  int64_t key = state.thread_index() * num_shards;
  for (auto _ : state) {
    cache.Lookup(key);
    if (++key >= num_shards * max_per_shards) key = 0;
  }
}
BENCHMARK(BM_Lookup)->ThreadRange(1, 64)->RangePair(1, 128,    // num_shards
                                                    0, 1024);  // max_per_shards

void BM_InsertRemove(benchmark::State& state) {
  const int num_shards = state.range(0);
  const int max_per_shards = state.range(1);
  ConcurrentLruCache<int64_t, int64_t> cache(num_shards, max_per_shards);

  int64_t key = state.thread_index() * num_shards;
  for (auto _ : state) {
    cache.Insert(key, key);
    cache.Remove(key);
    ++key;
  }
}
BENCHMARK(BM_InsertRemove)
    ->ThreadRange(1, 64)
    ->RangePair(1, 128,    // num_shards
                0, 1024);  // max_per_shards

}  // namespace
