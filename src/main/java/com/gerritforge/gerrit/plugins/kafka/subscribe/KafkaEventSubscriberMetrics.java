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
package com.gerritforge.gerrit.plugins.kafka.subscribe;

import com.gerritforge.gerrit.plugins.kafka.KafkaEventsMetrics;
import com.google.gerrit.metrics.Counter1;
import com.google.gerrit.metrics.Description;
import com.google.gerrit.metrics.MetricMaker;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
class KafkaEventSubscriberMetrics extends KafkaEventsMetrics {

  private static final String SUBSCRIBER_POLL_FAILURE_COUNTER =
      "subscriber_msg_consumer_poll_failure_counter";
  private static final String SUBSCRIBER_FAILURE_COUNTER =
      "subscriber_msg_consumer_failure_counter";

  private final Counter1<String> subscriberPollFailureCounter;
  private final Counter1<String> subscriberFailureCounter;

  @Inject
  public KafkaEventSubscriberMetrics(MetricMaker metricMaker) {
    this.subscriberPollFailureCounter =
        metricMaker.newCounter(
            "kafka/subscriber/subscriber_message_consumer_poll_failure_counter",
            new Description("Number of failed attempts to poll messages by the subscriber")
                .setRate()
                .setUnit("errors"),
            stringField(
                SUBSCRIBER_POLL_FAILURE_COUNTER, "Subscriber failed to poll messages count"));
    this.subscriberFailureCounter =
        metricMaker.newCounter(
            "kafka/subscriber/subscriber_message_consumer_failure_counter",
            new Description("Number of messages failed to consume by the subscriber consumer")
                .setRate()
                .setUnit("errors"),
            stringField(SUBSCRIBER_FAILURE_COUNTER, "Subscriber failed to consume messages count"));
  }

  public void incrementSubscriberFailedToPollMessages() {
    subscriberPollFailureCounter.increment(SUBSCRIBER_POLL_FAILURE_COUNTER);
  }

  public void incrementSubscriberFailedToConsumeMessage() {
    subscriberFailureCounter.increment(SUBSCRIBER_FAILURE_COUNTER);
  }
}
