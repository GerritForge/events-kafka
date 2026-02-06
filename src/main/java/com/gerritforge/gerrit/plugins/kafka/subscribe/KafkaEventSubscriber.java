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

import com.gerritforge.gerrit.eventbroker.MessageContext;
import com.google.gerrit.server.events.Event;
import java.util.Optional;
import java.util.function.BiConsumer;

/** Generic interface to a Kafka topic subscriber. */
public interface KafkaEventSubscriber {

  public interface Factory {
    KafkaEventSubscriber create(Optional<String> externalGroupId);
  }

  /**
   * Subscribe to a topic and receive messages asynchronously.
   *
   * @param topic Kafka topic name
   * @param messageProcessor consumer function for processing incoming messages
   */
  void subscribe(String topic, java.util.function.Consumer<Event> messageProcessor);

  /**
   * Subscribe to a topic and receive messages asynchronously with message context.
   *
   * <p>Default implementation preserves existing behavior and provides a no-op context.
   *
   * @param topic Kafka topic name
   * @param messageProcessor consumer function for processing incoming messages with context
   */
  default void subscribe(String topic, BiConsumer<Event, MessageContext> messageProcessor) {
    subscribe(topic, event -> messageProcessor.accept(event, MessageContext.noop()));
  }

  /** Shutdown Kafka consumer. */
  void shutdown();

  /**
   * Returns the current consumer function for the subscribed topic.
   *
   * @return the default topic consumer function.
   */
  java.util.function.Consumer<Event> getMessageProcessor();

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
