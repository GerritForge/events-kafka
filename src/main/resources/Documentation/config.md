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
    - `REST` for using a simple HTTP client to connect to
      [Confluent REST-API Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html).
      **NOTE**: `plugin.@PLUGIN@.restApiUri` is mandatory when using a `REST` client type.
      **NOTE**: explicit offset commits are supported only by the `NATIVE` client type.
      **NOTE**: partition-aware subscriptions are supported by the `NATIVE` client type.
      With the `REST` client type, consumed messages are acknowledged automatically by the
      REST proxy.
	Default: `NATIVE`

`plugin.@PLUGIN@.groupId`
:	Kafka consumer group for receiving messages.
	Default: Gerrit instance-id

`plugin.@PLUGIN@.httpWireLog`
:	Enable the HTTP wire protocol logging in error_log for all the communication with
	the [Confluent REST-API Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html).
	**NOTE**: when `plugin.@PLUGIN@.restApiUri` is unset or set to `NATIVE`, this setting is ignored.
	Default: false

`plugin.@PLUGIN@.pollingIntervalMs`
:	Polling interval in msec for receiving messages from Kafka topic subscription.
	Default: 1000

`plugin.@PLUGIN@.enableAutoCommit`
:	Enable automatic offset commits for consumed messages.
	When set to `false`, consumers can explicitly acknowledge messages by calling `ack()`.
	Explicit acknowledgements are supported only by the `NATIVE` client type. Configuring
	`enableAutoCommit=false` with the `REST` client type prevents the plugin from loading,
	because the `REST` client type always uses automatic acknowledgement.
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

`plugin.@PLUGIN@.restApiUri`
:	URL of the
	[Confluent REST-API Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html)
	for sending/receiving messages through REST-API instead of using the native Kafka client.
	The value can use the `${kafka_rest_id}` placeholder which will be replaced at runtime using
	the `KAFKA_REST_ID` associated with the Kafka REST-API Proxy that is answering the call,
	typically needed when having multiple proxies behind a workload balancer and sticky session
	allocation isn't an option.
	**NOTE**: when `plugin.@PLUGIN@.restApiUri` is unset or set to `NATIVE`, this setting is ignored.
	Default: unset

`plugin.@PLUGIN@.restApiThreads`
:	Maximum number of concurrent client calls to the
	[Confluent REST-API Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html)
	for sending/receiving messages.
	**NOTE**: when `plugin.@PLUGIN@.restApiUri` is unset or set to `NATIVE`, this setting is ignored.
	Default: 10

`plugin.@PLUGIN@.restApiTimeout`
:	Maximum time to wait for a client call to
	[Confluent REST-API Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html)
	to complete. This setting is also applied as TCP socket connection and read/write timeout
	for the outgoing HTTP calls.
	The value is expressed using the `N unit` format of all other Gerrit time expressions, using
	one of the following units:
	- s, sec, second, seconds
	- m, min, minute, minutes
	- h, hr, hour, hours
	**NOTE**: when `plugin.@PLUGIN@.restApiUri` is unset or set to `NATIVE`, this setting is ignored.
	Default: 60 sec

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

secure.config
--------------------

`plugin.@PLUGIN@.restApiUsername`
:	Username used for the authentication to the
	[Confluent REST-API Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html)
	for sending/receiving messages through REST-API instead of using the native Kafka client.
	**NOTE**: when `plugin.@PLUGIN@.restApiUri` is unset or set to `NATIVE`, this setting is ignored.
	Default: unset

`plugin.@PLUGIN@.restApiPassword`
:	Password used for the authentication to the
	[Confluent REST-API Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html)
	**NOTE**: when `plugin.@PLUGIN@.restApiUri` is unset or set to `NATIVE`, this setting is ignored.
	Default: unset
