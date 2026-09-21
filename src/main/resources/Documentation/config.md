Apache Kafka Configuration
======================

Some parameters can be configured using Gerrit config file.

Sample config
---------------------

```
[plugin "@PLUGIN@"]
        bootstrapServers = localhost:9092
```

All the Apache Kafka properties configuration needs to
be defined in gerrit.config using a lower camel-case notation.

Example: bootstrapServers correspond to the Apache Kafka property
bootstrap.servers.

See [Apache Kafka Producer Config](http://kafka.apache.org/documentation.html#producerconfigs)
for a full list of available settings and the values allowed.

Default Values
-----------------

|name                 | value
|:--------------------|:------------------
| acks                | all
| retries             | 0
| batchSize           | 16384
| lingerMs            | 1
| bufferMemory        | 33554432
| keySerializer       | org.apache.kafka.common.serialization.StringSerializer
| valueSerializer     | org.apache.kafka.common.serialization.StringSerializer

Additional properties
---------------------

`plugin.@PLUGIN@.clientType`
:	Client stack for connecting to Kafka broker:
    - `NATIVE` for using the Kafka client to connect to the broker directory
    - `REST` is not supported anymore, as its adoption was very limited

`plugin.@PLUGIN@.groupId`
:	Kafka consumer group for receiving messages.
	Default: Gerrit instance-id

`plugin.@PLUGIN@.pollingIntervalMs`
:	Polling interval in msec for receiving messages from Kafka topic subscription.
	Default: 1000

`plugin.@PLUGIN@.enableAutoCommit`
:	Enable automatic offset commits for consumed messages.
	When set to `false`, consumers can explicitly acknowledge messages by calling `ack()`.
	Explicit acknowledgements are supported only by the `NATIVE` client type.
	`ack()` performs a Kafka offset commit on the underlying Kafka consumer.
	Kafka consumers are not thread-safe, so callers must invoke `ack()` synchronously from the
	consumer callback thread.
	`ack()` commits the next offset for the acknowledged record immediately. If multiple records
	from the same partition are processed out of order, acknowledging a later record can commit past
	earlier records that have not been processed successfully yet. Clients using explicit
	acknowledgement must therefore process and acknowledge records from the same partition in poll
	order.
	Default: true

`plugin.@PLUGIN@.numberOfSubscribers`
:   The number of consumers that are expected to be executed. This number will
    be used to allocate a thread pool of a suitable size.
    Default to `7`. This is to allow enough resources to consume all relevant
    gerrit topics in a multi-site with pull-replication deployment: `batchIndexEventTopic`
    `streamEventTopic`, `gerritTopic`, `projectListEventTopic`,
    `cacheEventTopic`, `indexEventTopic`

`plugin.@PLUGIN@.sendAsync`
:	Send messages to Kafka asynchronously, detaching the calling process from the
	acknowledge of the message being sent.
	Default: true

`plugin.@PLUGIN@.topic`
:   Send all gerrit stream events to this topic (when `sendStreamEvents` is set
    to `true`).
    Default: gerrit

`plugin.@PLUGIN@.sendStreamEvents`
:   Whether to send stream events to the `topic` topic.
    Default: false

Gerrit init integration
-----------------------

The @PLUGIN@ plugin provides an init step that helps to set up the configuration.

```shell
*** events-kafka plugin
***

Should send stream events?     [y/N]? y
Stream events topic            [gerrit]: gerrit_stream_events
Should send messages asynchronously? [Y/n]? y
Polling interval (ms)          [1000]: 3000
Number of subscribers          [6]: 6
Consumer group                 [my_group_id]: my_group_id
```
