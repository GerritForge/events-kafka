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

import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.TopicSubscriberWithContext;
import com.gerritforge.gerrit.eventbroker.TopicSubscriberWithContextWithGroupId;
import com.gerritforge.gerrit.plugins.kafka.broker.ConsumerExecutor;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.ClientType;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventDeserializer;
import com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventNativeSubscriber;
import com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventRestSubscriber;
import com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber;
import com.google.common.collect.Sets;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.lifecycle.LifecycleModule;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.git.WorkQueue;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

@Singleton
public class KafkaApiModule extends LifecycleModule {
  private Set<TopicSubscriberWithContext> activeConsumers = Sets.newHashSet();
  private Set<TopicSubscriberWithContextWithGroupId> activeConsumersWithGroupId = Sets.newHashSet();
  private WorkQueue workQueue;
  private KafkaSubscriberProperties configuration;

  @Inject
  public KafkaApiModule(WorkQueue workQueue, KafkaSubscriberProperties configuration) {
    this.workQueue = workQueue;
    this.configuration = configuration;
  }

  @Inject(optional = true)
  public void setPreviousBrokerApi(DynamicItem<BrokerApi> previousBrokerApi) {
    if (previousBrokerApi != null && previousBrokerApi.get() != null) {
      BrokerApi api = previousBrokerApi.get();
      this.activeConsumersWithGroupId = api.topicSubscribersWithContextAndGroupId();
      this.activeConsumers = api.topicSubscribersWithContext();
    }
  }

  @Override
  protected void configure() {
    ClientType clientType = configuration.getClientType();
    switch (clientType) {
      case NATIVE:
        install(
            new FactoryModuleBuilder()
                .implement(KafkaEventSubscriber.class, KafkaEventNativeSubscriber.class)
                .build(KafkaEventSubscriber.Factory.class));
        break;
      case REST:
        install(
            new FactoryModuleBuilder()
                .implement(KafkaEventSubscriber.class, KafkaEventRestSubscriber.class)
                .build(KafkaEventSubscriber.Factory.class));
        break;
      default:
        throw new IllegalArgumentException("Unsupported Kafka client type " + clientType);
    }

    bind(ExecutorService.class)
        .annotatedWith(ConsumerExecutor.class)
        .toInstance(
            workQueue.createQueue(configuration.getNumberOfSubscribers(), "kafka-subscriber"));

    bind(new TypeLiteral<Deserializer<byte[]>>() {}).toInstance(new ByteArrayDeserializer());
    bind(new TypeLiteral<Deserializer<Event>>() {}).to(KafkaEventDeserializer.class);
    bind(new TypeLiteral<Set<TopicSubscriberWithContext>>() {}).toInstance(activeConsumers);
    bind(new TypeLiteral<Set<TopicSubscriberWithContextWithGroupId>>() {})
        .toInstance(activeConsumersWithGroupId);

    DynamicItem.bind(binder(), BrokerApi.class).to(KafkaBrokerApi.class).in(Scopes.SINGLETON);
  }
}
