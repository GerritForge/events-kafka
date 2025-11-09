This plugin publishes gerrit stream events to an Apache Kafka topic.

It also provides a Kafka-based implementation of a generic
[Events Broker Api](https://github.com/GerritForge/events-broker) which can be used by
Gerrit and other plugins.

Use-cases
=========

CI/CD Validation
----------------

Gerrit stream events can be published to the internal network where other subscribers
can trigger automated jobs (e.g. CI/CD validation) for fetching the changes and validating
them through build and testing.

__NOTE__: This use-case would require a CI/CD system (e.g. Jenkins, Zuul or other) and
the development of a Kafka-based subscriber to receive the event and trigger the build.

Events replication
------------------

Multiple Gerrit masters in a multi-site setup can be informed on the stream events
happening on every node thanks to the notification to a Kafka pub/sub topic.

__NOTE__: This use-case would require the [multi-site plugin](https://github.com/GerritForge/multi-site)
on each of the Gerrit masters that are part of the same multi-site cluster.
