# Kafka Broker Publisher Metrics

The following Gerrit metrics are exposed to monitor the performance and
reliability of the Kafka broker publisher. All metrics are recorded as rate
counters.

### `kafka/broker/broker_message_publisher_counter`

* **Description**: Number of successfully published messages by the broker
  publisher.
* **Type**: Counter (Rate)
* **Unit**: messages

### `kafka/broker/broker_message_publisher_failure_counter`

* **Description**: Number of messages failed to publish by the broker
  publisher.
* **Type**: Counter (Rate)
* **Unit**: errors

### `kafka/broker/broker_message_requeue_counter`

* **Description**: Number of successfully requeued messages by the broker
  publisher.
* **Type**: Counter (Rate)
* **Unit**: messages

### `kafka/broker/broker_message_requeue_failure_counter`

* **Description**: Number of messages failed to requeue by the broker
  publisher.
* **Type**: Counter (Rate)
* **Unit**: errors
