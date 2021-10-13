load(
    "//bazel:envoy_build_system.bzl",
    "envoy_cc_extension",
    "envoy_cc_library",
    "envoy_extension_package",
    "envoy_proto_library",
)

licenses(["notice"])  # Apache 2

## CacheFilter InMemoryCache

envoy_extension_package()

envoy_cc_library(
    name = "in_memory_cache_lib",
    hdrs = ["in_memory_cache.h"],
    deps = [
        "@com_google_absl//absl/base/thread_annotations.h"
        "@com_google_absl//absl/memory/memory.h",
        "@com_google_absl//absl/strings/string_view.h",
        "@com_google_absl//absl/synchronization/mutex.h",
        "@com_google_absl//absl/time/time.h",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:fixed_array",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/types:optional",
    ],
)
