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

package com.gerritforge.gerrit.plugins.kafka;

import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.TopicSubscriberWithContext;
import com.gerritforge.gerrit.eventbroker.TopicSubscriberWithContextWithGroupId;
import com.gerritforge.gerrit.plugins.kafka.publish.KafkaPublisher;
import com.google.gerrit.extensions.events.LifecycleListener;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Set;

@Singleton
public class Manager implements LifecycleListener {

  private final KafkaPublisher publisher;
  private final Set<TopicSubscriberWithContext> consumers;
  private final Set<TopicSubscriberWithContextWithGroupId> consumersWithGroupId;
  private final BrokerApi brokerApi;

  @Inject
  public Manager(
      KafkaPublisher publisher,
      Set<TopicSubscriberWithContext> consumers,
      Set<TopicSubscriberWithContextWithGroupId> consumersWithGroupId,
      BrokerApi brokerApi) {
    this.publisher = publisher;
    this.consumers = consumers;
    this.brokerApi = brokerApi;
    this.consumersWithGroupId = consumersWithGroupId;
  }

  @Override
  public void start() {
    publisher.start();
    consumers.forEach(
        topicSubscriber ->
            brokerApi.receiveAsyncWithContext(topicSubscriber.topic(), topicSubscriber.consumer()));

    consumersWithGroupId.forEach(
        topicSubscriberWithGroupId -> {
          TopicSubscriberWithContext topicSubscriber =
              topicSubscriberWithGroupId.topicSubscriberWithContext();
          brokerApi.receiveAsyncWithContext(
              topicSubscriber.topic(),
              topicSubscriberWithGroupId.groupId(),
              topicSubscriber.consumer());
        });
  }

  @Override
  public void stop() {
    publisher.stop();
    brokerApi.disconnect();
  }
}
