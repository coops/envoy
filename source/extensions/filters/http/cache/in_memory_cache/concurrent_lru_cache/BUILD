load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_cc_library",
    "envoy_extension_package",
    "envoy_proto_library",
)

licenses(["notice"])  # Apache 2

## Concurrent LRU Cache

envoy_extension_package()

envoy_cc_library(
    name = "concurrent_lru_cache_lib",
    hdrs = ["concurrent_lru_cache.h"],
    deps = [
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:fixed_array",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/types:optional",
    ],
)
