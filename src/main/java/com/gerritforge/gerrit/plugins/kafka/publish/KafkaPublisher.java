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

import com.gerritforge.gerrit.eventbroker.EventsBrokerConfiguration;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.session.KafkaSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.EventGson;
import com.google.gerrit.server.events.EventListener;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import java.util.Optional;

@Singleton
public class KafkaPublisher implements EventListener {

  private final KafkaSession session;
  private final Gson gson;
  private final KafkaProperties properties;
  private final EventsBrokerConfiguration eventsBrokerConfiguration;

  @Inject
  public KafkaPublisher(
      KafkaSession kafkaSession,
      @EventGson Gson gson,
      KafkaProperties properties,
      EventsBrokerConfiguration eventsBrokerConfiguration) {
    this.session = kafkaSession;
    this.gson = gson;
    this.properties = properties;
    this.eventsBrokerConfiguration = eventsBrokerConfiguration;
  }

  public void start() {
    if (!session.isOpen()) {
      session.connect();
    }
  }

  public void stop() {
    session.disconnect();
  }

  @Override
  public void onEvent(Event event) {
    if (session.isOpen()) {
      publish(properties.getTopic(), event);
    }
  }

  public ListenableFuture<Boolean> publish(String topic, Event event) {
    return session.publish(topic, resolvePartition(topic, event), getPayload(event));
  }

  private String getPayload(Event event) {
    return gson.toJson(event);
  }

  private Optional<Integer> resolvePartition(String topic, Event event) {
    List<String> partitions = eventsBrokerConfiguration.getPartitionsForTopic(topic);
    if (partitions.isEmpty()) {
      return Optional.empty();
    }

    Optional<String> property = eventsBrokerConfiguration.getEventPropertyForTopic(topic);
    if (property.isPresent()) {
      JsonObject eventJson = eventToJson(event);
      if (eventJson.has(property.get())) {
        int partition = partitions.indexOf(eventJson.get(property.get()).getAsString());
        if (partition >= 0) {
          return Optional.of(partition);
        }
      }
    }

    throw new IllegalArgumentException(
        String.format("Cannot resolve Kafka partition for topic %s", topic));
  }

  @VisibleForTesting
  public JsonObject eventToJson(Event event) {
    return gson.toJsonTree(event).getAsJsonObject();
  }
}
