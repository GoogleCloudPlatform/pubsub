# Description:
#   A load test framework for Cloud Pub/Sub

package(default_visibility = ["//third_party/java_src/cloud/pubsub:src"])

licenses(["notice"])  # Apache License 2.0 posted at //third_party/java_src/cloud/pubsub/LICENSE

proto_library(
    name = "loadtest_proto",
    srcs = ["load-test-framework/src/main/proto/loadtest.proto"],
    has_services = 1,
    java_api_version = 2,
    deps = [
        "//google/protobuf:duration",
        "//google/protobuf:timestamp",
    ],
)

load("//third_party/java/grpc:build_defs.bzl", "java_grpc_library")

java_grpc_library(
    name = "loadtest_grpc",
    srcs = [":loadtest_proto"],
    deps = [":loadtest_proto"],
)

java_library(
    name = "loadtest_library",
    srcs = glob([
        "load-test-framework/src/main/java/com/google/pubsub/flic/**/*.java",
        "load-test-framework/src/main/java/com/google/pubsub/clients/adapter/*.java",
        "load-test-framework/src/main/java/com/google/pubsub/clients/common/*.java",
        "load-test-framework/src/main/java/com/google/pubsub/clients/experimental/*.java",
    ]),
    data = glob([
        "load-test-framework/src/main/resources/*",
        "load-test-framework/src/main/resources/**/*",
    ]),
    deps = [
        ":client",
        ":loadtest_grpc",
        ":loadtest_proto",
        "//apiserving/discoverydata/compute:compute.v1",
        "//apiserving/discoverydata/storage",
        "//google/apps/sheets:sheets_external_appengine_java_lib",
        "//google/monitoring:monitoring_java_lib",
        "//google/protobuf:duration",
        "//google/protobuf:timestamp",
        "//google/pubsub:pubsub-v1-java-public",
        "//google/pubsub/v1:pubsub_proto",
        "//java/com/google/api/client/auth/oauth2",
        "//java/com/google/api/client/extensions/java6/auth/oauth2",
        "//java/com/google/api/client/extensions/jetty/auth/oauth2",
        "//java/com/google/api/client/googleapis/auth/oauth2",
        "//java/com/google/api/client/googleapis/javanet",
        "//java/com/google/api/client/googleapis/json",
        "//java/com/google/api/client/http",
        "//java/com/google/api/client/json",
        "//java/com/google/api/client/json/jackson2",
        "//java/com/google/api/client/util",
        "//java/com/google/api/client/util/store",
        "//java/com/google/common/base",
        "//java/com/google/common/collect",
        "//java/com/google/common/math",
        "//java/com/google/common/util/concurrent",
        "//java/com/google/protobuf/util:time",
        "//third_party/java/apache_commons_lang3",
        "//third_party/java/apache_httpclient",
        "//third_party/java/apache_httpcore",
        "//third_party/java/grpc",
        "//third_party/java/jakarta_commons_codec",
        "//third_party/java/jcommander",
        "//third_party/java/joda_time",
        "//third_party/java/log4j",
        "//third_party/java/slf4j_api",
        "//third_party/java/slf4j_jdk14",
    ],
)

java_binary(
    name = "driver",
    main_class = "com.google.pubsub.flic.Driver",
    runtime_deps = [
        ":loadtest_library",
    ],
)

java_library(
    name = "client",
    srcs = glob(
        ["client/src/main/java/com/google/cloud/pubsub/**/*.java"],
    ),
    runtime_deps = [
        "//third_party/java/netty_tcnative",
    ],
    deps = [
        "//google/pubsub:pubsub-internal-v1-grpc-java",
        "//google/pubsub/v1:pubsub_proto",
        "//java/com/google/common/annotations",
        "//java/com/google/common/base",
        "//java/com/google/common/collect",
        "//java/com/google/common/math",
        "//java/com/google/common/primitives",
        "//java/com/google/common/util/concurrent",
        "//third_party/java/auth_kit",
        "//third_party/java/grpc",
        "//third_party/java/joda_time",
        "//third_party/java/jsr305_annotations",
        "//third_party/java/log4j",
        "//third_party/java/slf4j_api",
        "//third_party/java/slf4j_jdk14",
    ],
)

java_binary(
    name = "client_publisher_samples",
    srcs = ["client-samples/src/main/java/com/google/cloud/pubsub/PublisherSamples.java"],
    main_class = "com.google.cloud.pubsub.PublisherSamples",
    deps = [
        ":client",
        "//google/pubsub/v1:pubsub_proto",
        "//java/com/google/common/util/concurrent",
        "//java/com/google/protobuf:protobuf_lite",
        "//third_party/java/joda_time",
    ],
)

java_binary(
    name = "client_subscriber_samples",
    srcs = ["client-samples/src/main/java/com/google/cloud/pubsub/SubscriberSamples.java"],
    main_class = "com.google.cloud.pubsub.SubscriberSamples",
    deps = [
        ":client",
        "//google/pubsub/v1:pubsub_proto",
        "//java/com/google/common/collect",
        "//java/com/google/common/util/concurrent",
        "//third_party/java/auth_kit",
    ],
)

########################################################################
# Test targets for validating Cloud Pub/Sub components.

TEST_UTIL_SOURCES = [
    "client/src/test/java/com/google/cloud/pubsub/FakeScheduledExecutorService.java",
    "client/src/test/java/com/google/cloud/pubsub/FakeCredentials.java",
    "client/src/test/java/com/google/cloud/pubsub/FakePublisherServiceImpl.java",
    "client/src/test/java/com/google/cloud/pubsub/FakeSubscriberServiceImpl.java",
]

java_library(
    name = "client_tests_lib",
    testonly = 1,
    srcs = glob(
        [
            "client/src/test/java/**/*Test.java",
        ],
    ) + TEST_UTIL_SOURCES,
    deps = [
        ":client",
        "//google/pubsub:pubsub-internal-v1-grpc-java",
        "//google/pubsub/v1:pubsub_proto",
        "//java/com/google/api/client/util",
        "//java/com/google/common/annotations",
        "//java/com/google/common/base",
        "//java/com/google/common/collect",
        "//java/com/google/common/primitives",
        "//java/com/google/common/util/concurrent",
        "//java/com/google/protobuf:protobuf_lite",
        "//third_party/java/auth_kit",
        "//third_party/java/grpc",
        "//third_party/java/joda_time",
        "//third_party/java/jsr305_annotations",
        "//third_party/java/junit",
        "//third_party/java/mockito",
    ],
)

java_test(
    name = "publisher_test",
    size = "small",
    test_class = "com.google.cloud.pubsub.PublisherImplTest",
    runtime_deps = [":client_tests_lib"],
)

java_test(
    name = "subscriber_test",
    size = "small",
    test_class = "com.google.cloud.pubsub.SubscriberImplTest",
    runtime_deps = [":client_tests_lib"],
)

java_test(
    name = "flow_controller_test",
    size = "small",
    test_class = "com.google.cloud.pubsub.FlowControllerTest",
    runtime_deps = [":client_tests_lib"],
)

java_test(
    name = "message_waiter_test",
    size = "small",
    test_class = "com.google.cloud.pubsub.MessagesWaiterTest",
    runtime_deps = [":client_tests_lib"],
)

filegroup(
    name = "opensource_filegroup",
    srcs = glob(
        [
            "*",
            "**/*",
        ],
        exclude = [
            "BUILD",
            "**/target/**",
        ],
    ),
)
