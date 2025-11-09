Kafka: Gerrit event producer for Apache Kafka
=======================

* Author: GerritForge
* Repository: https://github.com/GerritForge/events-kafka
* CI/Release: https://gerrit-ci.gerritforge.com/search/?q=events-kafka

[![Build Status](https://gerrit-ci.gerritforge.com/job/plugin-events-kafka-bazel-master/lastBuild/badge/icon)](https://gerrit-ci.gerritforge.com/job/plugin-events-kafka-bazel-master/lastBuild/)

Synopsis
----------------------

This plugins allows to define a distributed stream of events
published by Gerrit.

Events can be anything, from the traditional stream events
to the Gerrit metrics.

This plugin requires Gerrit 2.13 or later.

Environments
---------------------

* `linux`
* `java-1.8`
* `Bazel`

Build
---------------------
Kafka plugin can be build as a regular 'in-tree' plugin. That means that is required to
clone a Gerrit source tree first and then to have the Kafka plugin source directory into
the /plugins path. The plugin depends on [events-broker](https://gerrit.googlesource.com/modules/events-broker)
which is linked directly from source with the same 'in-tree' plugin structure.

Additionally, the plugins/external_plugin_deps.bzl file needs to be
updated to match the Kafka plugin one.

    git clone --recursive https://gerrit.googlesource.com/gerrit
    git clone https://github.com/GerritForge/events-kafka gerrit/plugins/events-kafka
    git clone https://gerrit.googlesource.com/modules/events-broker gerrit/plugins/events-broker
    cd gerrit
    rm plugins/external_plugin_deps.bzl
    ln -s ./events-kafka/external_plugin_deps.bzl plugins/.

To build the events-kafka plugins, issue the command from the Gerrit source path:

    bazel build plugins/events-kafka

The output is created in

    bazel-genfiles/plugins/events-kafka/events-kafka.jar
    bazel-genfiles/plugins/events-broker/events-broker.jar

Minimum Configuration
---------------------
Assuming a running Kafka broker on the same Gerrit host, add the following
settings to gerrit.config:

```
  [plugin "events-kafka"]
    bootstrapServers = localhost:9092
```

