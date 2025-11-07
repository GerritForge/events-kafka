load("//tools/bzl:junit.bzl", "junit_tests")
load(
    "//tools/bzl:plugin.bzl",
    "PLUGIN_DEPS",
    "PLUGIN_TEST_DEPS",
    "gerrit_plugin",
)

gerrit_plugin(
    name = "events-kafka",
    srcs = glob(["src/main/java/**/*.java"]),
    manifest_entries = [
        "Gerrit-PluginName: events-kafka",
        "Gerrit-InitStep: plugins.events-kafka.src.main.java.com.gerritforge.gerrit.plugins.kafka.InitConfig",
        "Gerrit-Module: com.gerritforge.gerrit.plugins.kafka.Module",
        "Implementation-Title: Gerrit Apache Kafka plugin",
        "Implementation-URL: https://gerrit.googlesource.com/plugins/events-kafka",
    ],
    resources = glob(["src/main/resources/**/*"]),
    deps = [
        ":events-broker-neverlink",
        "//lib/httpcomponents:httpclient",
        "@httpasyncclient//jar",
        "@httpcore-nio//jar",
        "@kafka-client//jar",
    ],
)

junit_tests(
    name = "events_kafka_tests",
    timeout = "long",
    srcs = glob(["src/test/java/**/*.java"]),
    resources = glob(["src/test/resources/**/*"]),
    tags = ["events-kafka"],
    deps = [
        ":events-kafka__plugin_test_deps",
        "//plugins/events-broker",
        "@kafka-client//jar",
        "@testcontainers-kafka//jar",
        "@testcontainers//jar",
    ],
)

java_library(
    name = "events-kafka__plugin_test_deps",
    testonly = 1,
    visibility = ["//visibility:public"],
    exports = PLUGIN_DEPS + PLUGIN_TEST_DEPS + [
        ":events-kafka__plugin",
        "@docker-java-api//jar",
        "@docker-java-transport//jar",
        "@duct-tape//jar",
        "@jackson-annotations//jar",
        "@jna//jar",
        "@testcontainers-kafka//jar",
        "@testcontainers//jar",
        "@visible-assertions//jar",
    ],
)

java_library(
    name = "events-broker-neverlink",
    neverlink = 1,
    exports = ["//plugins/events-broker"],
)
