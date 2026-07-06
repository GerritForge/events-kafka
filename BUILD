load(
    "@com_googlesource_gerrit_bazlets//:gerrit_plugin.bzl",
    "gerrit_plugin",
    "gerrit_plugin_tests",
)
load("@rules_java//java:defs.bzl", "java_library")

PLUGIN = "events-kafka"

# External Maven deps bundled into the plugin JAR at runtime.
EXT_DEPS = [
    "org.apache.httpcomponents:httpasyncclient",
    "org.apache.httpcomponents:httpcore-nio",
    "org.apache.kafka:kafka-clients",
]

# External Maven deps required on the test classpath but not bundled into
# the plugin JAR (testcontainers + transitives used by the Kafka container
# setup in `*IT` tests).
TEST_EXT_DEPS = EXT_DEPS + [
    "com.fasterxml.jackson.core:jackson-annotations",
    "com.github.docker-java:docker-java-api",
    "com.github.docker-java:docker-java-transport",
    "net.java.dev.jna:jna",
    "org.rnorth.duct-tape:duct-tape",
    "org.rnorth.visible-assertions:visible-assertions",
    "org.testcontainers:kafka",
    "org.testcontainers:testcontainers",
]

gerrit_plugin(
    srcs = glob(["src/main/java/**/*.java"]),
    ext_deps = EXT_DEPS,
    manifest_entries = [
        "Gerrit-PluginName: events-kafka",
        "Gerrit-InitStep: com.gerritforge.gerrit.plugins.kafka.InitConfig",
        "Gerrit-Module: com.gerritforge.gerrit.plugins.kafka.Module",
        "Gerrit-HttpModule: com.gerritforge.gerrit.plugins.bsl.HttpModule",
        "Implementation-Title: Gerrit Apache Kafka plugin",
        "Implementation-URL: https://github.com/gerritforge/events-kafka",
    ],
    plugin = PLUGIN,
    resources = glob(["src/main/resources/**/*"]),
    deps = [
        ":events-broker-neverlink",
        ":httpcomponents-neverlink",
    ],
)

gerrit_plugin_tests(
    name = "events_kafka_tests",
    timeout = "long",
    srcs = glob(["src/test/java/**/*.java"]),
    ext_deps = TEST_EXT_DEPS,
    plugin = PLUGIN,
    resources = glob(["src/test/resources/**/*"]),
    deps = ["//plugins/events-broker"],
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
