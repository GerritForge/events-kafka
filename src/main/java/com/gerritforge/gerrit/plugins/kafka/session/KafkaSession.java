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

package com.gerritforge.gerrit.plugins.kafka.session;

import com.gerritforge.gerrit.eventbroker.BrokerApiMessageListener;
import com.gerritforge.gerrit.eventbroker.log.MessageLogger.Direction;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.publish.KafkaEventsPublisherMetrics;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSession.class);
  private final KafkaProperties properties;
  private final Provider<Producer<String, String>> producerProvider;
  private final KafkaEventsPublisherMetrics publisherMetrics;
  private final BrokerApiMessageListener messageListener;
  private final Set<TopicPartition> validatedPartitions = ConcurrentHashMap.newKeySet();
  private volatile Producer<String, String> producer;

  @Inject
  public KafkaSession(
      Provider<Producer<String, String>> producerProvider,
      KafkaProperties properties,
      KafkaEventsPublisherMetrics publisherMetrics,
      BrokerApiMessageListener messageListener) {
    this.producerProvider = producerProvider;
    this.properties = properties;
    this.publisherMetrics = publisherMetrics;
    this.messageListener = messageListener;
  }

  public boolean isOpen() {
    if (producer != null) {
      return true;
    }
    return false;
  }

  public void connect() {
    if (isOpen()) {
      LOGGER.debug("Already connected.");
      return;
    }

    switch (properties.getClientType()) {
      case NATIVE:
        String bootstrapServers = properties.getProperty("bootstrap.servers");
        if (bootstrapServers == null) {
          LOGGER.warn("No Kafka bootstrap.servers property defined: session not started.");
          return;
        }

        LOGGER.info("Connect to {}...", bootstrapServers);
        /* Need to make sure that the thread of the running connection uses
         * the correct class loader otherwise you can end up with hard to debug
         * ClassNotFoundExceptions
         */
        setConnectionClassLoader();
        break;

      case REST:
        URI kafkaProxyUri;
        try {
          kafkaProxyUri = properties.getRestApiUri();
        } catch (URISyntaxException e) {
          LOGGER.error("Invalid Kafka Proxy URI: session not started", e);
          return;
        }
        if (kafkaProxyUri == null) {
          LOGGER.warn("No Kafka Proxy URL property defined: session not started.");
          return;
        }

        LOGGER.info("Connect to {}...", kafkaProxyUri);
        break;

      default:
        LOGGER.error("Unsupported Kafka Client Type %s", properties.getClientType());
        return;
    }

    producer = producerProvider.get();
    LOGGER.info("Connection established.");
  }

  private void setConnectionClassLoader() {
    Thread.currentThread().setContextClassLoader(KafkaSession.class.getClassLoader());
  }

  public void disconnect() {
    LOGGER.info("Disconnecting...");
    if (producer != null) {
      LOGGER.info("Closing Producer {}...", producer);
      producer.close();
    }
    producer = null;
    validatedPartitions.clear();
  }

  public ListenableFuture<Boolean> publish(String messageBody) {
    return publish(properties.getTopic(), messageBody);
  }

  public ListenableFuture<Boolean> publish(
      String topic, String messageBody) {
    return publish(topic, Optional.empty(), messageBody);
  }

  public ListenableFuture<Boolean> publish(
      String topic, Optional<Integer> partition, String messageBody) {
    partition.ifPresent(partitionNumber -> validatePartition(topic, partitionNumber));
    if (properties.isSendAsync()) {
      return publishAsync(topic, partition, messageBody);
    }
    return publishSync(topic, partition, messageBody);
  }

  private void validatePartition(String topic, int partition) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    if (validatedPartitions.contains(topicPartition)) {
      return;
    }
    if (producer.partitionsFor(topic).stream()
        .noneMatch(partitionInfo -> partitionInfo.partition() == partition)) {
      throw new IllegalArgumentException(
          String.format("Kafka partition %d does not exist for topic %s", partition, topic));
    }
    validatedPartitions.add(topicPartition);
  }

  private ListenableFuture<Boolean> publishSync(
      String topic, Optional<Integer> partition, String messageBody) {
    SettableFuture<Boolean> resultF = SettableFuture.create();
    try {
      Future<RecordMetadata> future =
          producer.send(
              new ProducerRecord<>(
                  topic, partition.orElse(null), "" + System.nanoTime(), messageBody));
      RecordMetadata metadata = future.get();
      LOGGER.debug("The offset of the record we just sent is: {}", metadata.offset());
      publisherMetrics.incrementBrokerPublishedMessage();
      messageListener.messageProcessed(Direction.PUBLISH, topic, messageBody);
      resultF.set(true);
      return resultF;
    } catch (Throwable e) {
      publisherMetrics.incrementBrokerFailedToPublishMessage();
      messageListener.messageFailed(Direction.PUBLISH, topic, messageBody, e);
      return Futures.immediateFailedFuture(e);
    }
  }

  private ListenableFuture<Boolean> publishAsync(
      String topic, Optional<Integer> partition, String messageBody) {
    try {
      Future<RecordMetadata> future =
          producer.send(
              new ProducerRecord<>(
                  topic, partition.orElse(null), Long.toString(System.nanoTime()), messageBody),
              (metadata, e) -> {
                if (metadata != null && e == null) {
                  LOGGER.debug("The offset of the record we just sent is: {}", metadata.offset());
                  publisherMetrics.incrementBrokerPublishedMessage();
                  messageListener.messageProcessed(Direction.PUBLISH, topic, messageBody);
                } else {
                  publisherMetrics.incrementBrokerFailedToPublishMessage();
                  messageListener.messageFailed(Direction.PUBLISH, topic, messageBody, e);
                }
              });

      // The transformation is lightweight, so we can afford using a directExecutor
      return Futures.transform(
          JdkFutureAdapters.listenInPoolThread(future),
          Objects::nonNull,
          MoreExecutors.directExecutor());
    } catch (Throwable e) {
      publisherMetrics.incrementBrokerFailedToPublishMessage();
      messageListener.messageFailed(Direction.PUBLISH, topic, messageBody, e);
      return Futures.immediateFailedFuture(e);
    }
  }
}
