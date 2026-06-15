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

import com.gerritforge.gerrit.eventbroker.AckAwareConsumer;
import com.google.gerrit.server.events.Event;
import com.google.inject.assistedinject.Assisted;
import java.util.Optional;

/** Generic interface to a Kafka topic subscriber. */
public interface KafkaEventSubscriber {

  public interface Factory {
    KafkaEventSubscriber create(
        @Assisted("externalGroupId") Optional<String> externalGroupId,
        @Assisted("partition") Optional<Integer> partition);
  }

  /**
   * Subscribe to a topic and receive messages asynchronously.
   *
   * @param topic Kafka topic name
   * @param messageProcessor consumer function for processing incoming messages
   */
  void subscribe(String topic, AckAwareConsumer<Event> messageProcessor);

  /** Shutdown Kafka consumer. */
  void shutdown();

  /**
   * Returns the current Ack aware consumer function for the subscribed topic.
   *
   * @return the default topic consumer function.
   */
  AckAwareConsumer<Event> getMessageProcessor();

  /**
   * Returns the current subscribed topic name.
   *
   * @return Kafka topic name.
   */
  String getTopic();

  /** Reset the offset for reading incoming Kafka messages of the topic. */
  void resetOffset();

  /**
   * Returns the external consumer's group id when it is defined.
   *
   * @return Optional instance with external consumer's group id otherwise an empty Optional
   *     instance
   */
  Optional<String> getExternalGroupId();
}
