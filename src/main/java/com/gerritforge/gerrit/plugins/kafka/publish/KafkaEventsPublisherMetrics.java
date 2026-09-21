// Copyright (C) 2025 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.kafka.publish;

import com.gerritforge.gerrit.plugins.kafka.KafkaEventsMetrics;
import com.google.gerrit.metrics.Counter1;
import com.google.gerrit.metrics.Description;
import com.google.gerrit.metrics.MetricMaker;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class KafkaEventsPublisherMetrics extends KafkaEventsMetrics {
  private static final String PUBLISHER_SUCCESS_COUNTER = "broker_msg_publisher_success_counter";
  private static final String PUBLISHER_FAILURE_COUNTER = "broker_msg_publisher_failure_counter";
  private static final String PUBLISHER_REQUEUE_COUNTER = "broker_msg_requeue_success_counter";
  private static final String PUBLISHER_REQUEUE_FAILURE_COUNTER =
      "broker_msg_requeue_failure_counter";

  private final Counter1<String> brokerPublisherSuccessCounter;
  private final Counter1<String> brokerRequeueSuccessCounter;
  private final Counter1<String> brokerRequeueFailureCounter;
  private final Counter1<String> brokerPublisherFailureCounter;

  @Inject
  public KafkaEventsPublisherMetrics(MetricMaker metricMaker) {

    this.brokerPublisherSuccessCounter =
        metricMaker.newCounter(
            "kafka/broker/broker_message_publisher_counter",
            new Description("Number of successfully published messages by the broker publisher")
                .setRate()
                .setUnit("messages"),
            stringField(PUBLISHER_SUCCESS_COUNTER, "Broker message published count"));
    this.brokerPublisherFailureCounter =
        metricMaker.newCounter(
            "kafka/broker/broker_message_publisher_failure_counter",
            new Description("Number of messages failed to publish by the broker publisher")
                .setRate()
                .setUnit("errors"),
            stringField(PUBLISHER_FAILURE_COUNTER, "Broker failed to publish message count"));
    this.brokerRequeueSuccessCounter =
        metricMaker.newCounter(
            "kafka/broker/broker_message_requeue_counter",
            new Description("Number of successfully requeued messages by the broker publisher")
                .setRate()
                .setUnit("messages"),
            stringField(PUBLISHER_REQUEUE_COUNTER, "Broker message requeue count"));
    this.brokerRequeueFailureCounter =
        metricMaker.newCounter(
            "kafka/broker/broker_message_requeue_failure_counter",
            new Description("Number of messages failed to requeue by the broker publisher")
                .setRate()
                .setUnit("messages"),
            stringField(PUBLISHER_REQUEUE_COUNTER, "Broker failed to requeue message count"));
  }

  public void incrementBrokerPublishedMessage() {
    brokerPublisherSuccessCounter.increment(PUBLISHER_SUCCESS_COUNTER);
  }

  public void incrementBrokerFailedToPublishMessage() {
    brokerPublisherFailureCounter.increment(PUBLISHER_FAILURE_COUNTER);
  }

  public void incrementBrokerRequeuedMessage() {
    brokerRequeueSuccessCounter.increment(PUBLISHER_REQUEUE_COUNTER);
  }

  public void incrementBrokerFailedToRequeueMessage() {
    brokerRequeueFailureCounter.increment(PUBLISHER_REQUEUE_FAILURE_COUNTER);
  }
}
