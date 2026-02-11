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

import com.gerritforge.gerrit.plugins.kafka.api.KafkaApiModule;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.ClientType;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaPublisherProperties;
import com.gerritforge.gerrit.plugins.kafka.publish.KafkaPublisher;
import com.gerritforge.gerrit.plugins.kafka.publish.KafkaRestProducer;
import com.gerritforge.gerrit.plugins.kafka.rest.FutureExecutor;
import com.gerritforge.gerrit.plugins.kafka.rest.HttpHostProxy;
import com.gerritforge.gerrit.plugins.kafka.rest.HttpHostProxyProvider;
import com.gerritforge.gerrit.plugins.kafka.rest.KafkaRestClient;
import com.gerritforge.gerrit.plugins.kafka.session.KafkaProducerProvider;
import com.google.gerrit.extensions.events.LifecycleListener;
import com.google.gerrit.extensions.registration.DynamicSet;
import com.google.gerrit.server.events.EventListener;
import com.google.gerrit.server.git.WorkQueue;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.clients.producer.Producer;

class Module extends AbstractModule {
  private final KafkaApiModule kafkaBrokerModule;
  private final KafkaProperties kafkaConf;
  private final WorkQueue workQueue;
  private final KafkaPublisherProperties configuration;

  @Inject
  public Module(
      KafkaApiModule kafkaBrokerModule,
      KafkaPublisherProperties configuration,
      KafkaProperties kafkaConf,
      WorkQueue workQueue) {
    this.kafkaBrokerModule = kafkaBrokerModule;
    this.configuration = configuration;
    this.kafkaConf = kafkaConf;
    this.workQueue = workQueue;
  }

  @Override
  protected void configure() {
    DynamicSet.bind(binder(), LifecycleListener.class).to(Manager.class);

    if (configuration.isSendStreamEvents()) {
      DynamicSet.bind(binder(), EventListener.class).to(KafkaPublisher.class);
    }

    ClientType clientType = kafkaConf.getClientType();
    switch (clientType) {
      case NATIVE:
        bind(new TypeLiteral<Producer<String, String>>() {})
            .toProvider(KafkaProducerProvider.class);
        break;
      case REST:
        bind(ExecutorService.class)
            .annotatedWith(FutureExecutor.class)
            .toInstance(
                workQueue.createQueue(
                    kafkaConf.getRestApiThreads(), "KafkaRestClientThreadPool", true));
        bind(HttpHostProxy.class).toProvider(HttpHostProxyProvider.class).in(Scopes.SINGLETON);
        bind(new TypeLiteral<Producer<String, String>>() {}).to(KafkaRestProducer.class);
        install(new FactoryModuleBuilder().build(KafkaRestClient.Factory.class));
        break;
      default:
        throw new IllegalArgumentException("Unsupported Kafka client type " + clientType);
    }

    install(kafkaBrokerModule);
  }
}
