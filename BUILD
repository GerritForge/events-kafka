load(
    "@com_googlesource_gerrit_bazlets//:gerrit_plugin.bzl",
    "gerrit_plugin",
    "gerrit_plugin_dependency_tests",
    "gerrit_plugin_tests",
)
load("@rules_java//java:defs.bzl", "java_library")

PLUGIN_DEPS = [
    ":events-broker-neverlink",
    ":httpcomponents-neverlink",
    "@events-kafka_plugin_deps//:org_apache_httpcomponents_httpasyncclient",
    "@events-kafka_plugin_deps//:org_apache_httpcomponents_httpcore_nio",
    "@events-kafka_plugin_deps//:org_apache_kafka_kafka_clients",
]

gerrit_plugin(
    name = "events-kafka",
    srcs = glob(["src/main/java/**/*.java"]),
    manifest_entries = [
        "Gerrit-PluginName: events-kafka",
        "Gerrit-InitStep: com.gerritforge.gerrit.plugins.kafka.InitConfig",
        "Gerrit-Module: com.gerritforge.gerrit.plugins.kafka.Module",
        "Implementation-Title: Gerrit Apache Kafka plugin",
        "Implementation-URL: https://github.com/gerritforge/events-kafka",
    ],
    resources = glob(["src/main/resources/**/*"]),
    deps = PLUGIN_DEPS,
)

gerrit_plugin_tests(
    name = "events_kafka_tests",
    timeout = "long",
    srcs = glob(["src/test/java/**/*.java"]),
    resources = glob(["src/test/resources/**/*"]),
    tags = ["events-kafka"],
    deps = [
        ":events-kafka__plugin_test_deps",
        "//plugins/events-broker",
        "@events-kafka_plugin_deps//:org_apache_kafka_kafka_clients",
        "@events-kafka_plugin_deps//:org_testcontainers_kafka",
        "@events-kafka_plugin_deps//:org_testcontainers_testcontainers",
    ],
)

java_library(
    name = "events-kafka__plugin_test_deps",
    testonly = 1,
    visibility = ["//visibility:public"],
    exports = PLUGIN_DEPS + [
        ":events-kafka__plugin",
        "//plugins:plugin-lib-neverlink",
        "@events-kafka_plugin_deps//:com_fasterxml_jackson_core_jackson_annotations",
        "@events-kafka_plugin_deps//:com_github_docker_java_docker_java_api",
        "@events-kafka_plugin_deps//:com_github_docker_java_docker_java_transport",
        "@events-kafka_plugin_deps//:net_java_dev_jna_jna",
        "@events-kafka_plugin_deps//:org_rnorth_duct_tape_duct_tape",
        "@events-kafka_plugin_deps//:org_rnorth_visible_assertions_visible_assertions",
        "@events-kafka_plugin_deps//:org_testcontainers_kafka",
        "@events-kafka_plugin_deps//:org_testcontainers_testcontainers",
    ],
)

java_library(
    name = "events-broker-neverlink",
    neverlink = 1,
    exports = ["//plugins/events-broker"],
)

java_library(
    name = "httpcomponents-neverlink",
    neverlink = 1,
    exports = [
        "//lib/httpcomponents:httpclient",
        "//lib/httpcomponents:httpcore",
    ],
)

gerrit_plugin_dependency_tests(plugin = "events-kafka")
