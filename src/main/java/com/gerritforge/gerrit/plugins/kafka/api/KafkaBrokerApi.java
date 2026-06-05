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

package com.gerritforge.gerrit.plugins.kafka.api;

import com.gerritforge.gerrit.eventbroker.AckAwareConsumer;
import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.EventsBrokerConfiguration;
import com.gerritforge.gerrit.eventbroker.TopicSubscriber;
import com.gerritforge.gerrit.eventbroker.TopicSubscriberWithGroupId;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.ClientType;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.gerritforge.gerrit.plugins.kafka.publish.KafkaPublisher;
import com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gerrit.common.Nullable;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.EventGson;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaBrokerApi implements BrokerApi {

  private final KafkaPublisher publisher;
  private final KafkaEventSubscriber.Factory kafkaEventSubscriberFactory;
  private final EventsBrokerConfiguration eventsBrokerConfiguration;
  private final Gson gson;
  private final ClientType clientType;
  private final boolean autoAck;
  private List<KafkaEventSubscriber> subscribers;

  @Inject
  public KafkaBrokerApi(
      KafkaPublisher publisher,
      KafkaEventSubscriber.Factory kafkaEventSubscriberFactory,
      KafkaSubscriberProperties subscriberProperties,
      EventsBrokerConfiguration eventsBrokerConfiguration,
      @EventGson Gson gson) {
    this.publisher = publisher;
    this.kafkaEventSubscriberFactory = kafkaEventSubscriberFactory;
    this.eventsBrokerConfiguration = eventsBrokerConfiguration;
    this.gson = gson;
    this.clientType = subscriberProperties.getClientType();
    this.autoAck = subscriberProperties.isAutoCommitEnabled();
    subscribers = Collections.synchronizedList(new ArrayList<>());
  }

  @Override
  public ListenableFuture<Boolean> send(String topic, Event event) {
    return publisher.publish(topic, resolvePartitionFromEvent(topic, event), event);
  }

  @Override
  public void receiveAsync(String topic, AckAwareConsumer<Event> eventConsumer) {
    receiveAsync(topic, eventConsumer, Optional.empty());
  }

  @Override
  public void receiveAsync(String topic, String groupId, AckAwareConsumer<Event> eventConsumer) {
    receiveAsync(topic, eventConsumer, Optional.ofNullable(groupId));
  }

  @Override
  public void receiveAsyncWithPartition(
      String topic, String partition, String groupId, AckAwareConsumer<Event> consumer) {
    if (clientType == ClientType.REST) {
      throw new UnsupportedOperationException(
          "Partition-aware subscriptions are not supported with clientType=REST");
    }
    KafkaEventSubscriber subscriber =
        kafkaEventSubscriberFactory.create(
            Optional.of(groupId), Optional.of(resolvePartition(topic, partition)));
    synchronized (subscribers) {
      subscribers.add(subscriber);
    }
    subscriber.subscribe(topic, consumer);
  }

  @Override
  public void disconnect() {
    for (KafkaEventSubscriber subscriber : subscribers) {
      subscriber.shutdown();
    }
    subscribers.clear();
  }

  @Override
  public void disconnect(String topic, @Nullable String groupId) {
    Set<KafkaEventSubscriber> subscribersToDisconnect =
        subscribers.stream()
            .filter(s -> topic.equals(s.getTopic()))
            .filter(
                s -> groupId == null || s.getExternalGroupId().stream().anyMatch(groupId::equals))
            .collect(Collectors.toSet());
    subscribersToDisconnect.forEach(KafkaEventSubscriber::shutdown);
    subscribers.removeAll(subscribersToDisconnect);
  }

  @Override
  public Set<TopicSubscriber> topicSubscribers() {
    return subscribers.stream()
        .filter(s -> !s.getExternalGroupId().isPresent())
        .map(s -> TopicSubscriber.topicSubscriber(s.getTopic(), s.getMessageProcessor()))
        .collect(Collectors.toSet());
  }

  @Override
  public Set<TopicSubscriberWithGroupId> topicSubscribersWithGroupId() {
    return subscribers.stream()
        .filter(s -> s.getExternalGroupId().isPresent())
        .map(
            s ->
                TopicSubscriberWithGroupId.topicSubscriberWithGroupId(
                    s.getExternalGroupId().get(),
                    TopicSubscriber.topicSubscriber(s.getTopic(), s.getMessageProcessor())))
        .collect(Collectors.toSet());
  }

  @Override
  public void replayAllEvents(String topic) {
    subscribers.stream()
        .filter(subscriber -> topic.equals(subscriber.getTopic()))
        .forEach(subscriber -> subscriber.resetOffset());
  }

  @Override
  public boolean isAutoAck() {
    return autoAck;
  }

  private int resolvePartition(String topic, String partitionValue) {
    List<String> partitions = eventsBrokerConfiguration.getPartitionsForTopic(topic);
    if (partitions == null) {
      throw new IllegalArgumentException("No partitions configured for topic " + topic);
    }

    int partition = partitions.indexOf(partitionValue);
    if (partition < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Logical partition value %s is not configured for topic %s", partitionValue, topic));
    }

    return partition;
  }

  private Optional<Integer> resolvePartitionFromEvent(String topic, Event event) {
    List<String> partitions = eventsBrokerConfiguration.getPartitionsForTopic(topic);
    if (partitions == null || partitions.isEmpty()) {
      return Optional.empty();
    }

    String eventProperty = eventsBrokerConfiguration.getEventPropertyForTopic(topic);
    JsonObject eventJson = gson.toJsonTree(event).getAsJsonObject();
    if (!eventJson.has(eventProperty)) {
      throw new IllegalArgumentException(
          String.format("Event does not contain partition property %s", eventProperty));
    }

    return Optional.of(resolvePartition(topic, eventJson.get(eventProperty).getAsString()));
  }

  private void receiveAsync(
      String topic, AckAwareConsumer<Event> eventConsumer, Optional<String> externalGroupId) {
    KafkaEventSubscriber subscriber =
        kafkaEventSubscriberFactory.create(externalGroupId, Optional.empty());
    synchronized (subscribers) {
      subscribers.add(subscriber);
    }
    subscriber.subscribe(topic, eventConsumer);
  }
}
