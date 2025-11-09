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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.publish.KafkaEventsPublisherMetrics;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSession.class);
  private final KafkaProperties properties;
  private final Provider<Producer<String, String>> producerProvider;
  private final KafkaEventsPublisherMetrics publisherMetrics;
  private final Log4JKafkaMessageLogger msgLog;
  private volatile Producer<String, String> producer;

  @Inject
  public KafkaSession(
      Provider<Producer<String, String>> producerProvider,
      KafkaProperties properties,
      KafkaEventsPublisherMetrics publisherMetrics,
      Log4JKafkaMessageLogger msgLog) {
    this.producerProvider = producerProvider;
    this.properties = properties;
    this.publisherMetrics = publisherMetrics;
    this.msgLog = msgLog;
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
  }

  public ListenableFuture<Boolean> publish(String messageBody) {
    return publish(properties.getTopic(), messageBody);
  }

  public ListenableFuture<Boolean> publish(String topic, String messageBody) {
    if (properties.isSendAsync()) {
      return publishAsync(topic, messageBody);
    }
    return publishSync(topic, messageBody);
  }

  private ListenableFuture<Boolean> publishSync(String topic, String messageBody) {
    SettableFuture<Boolean> resultF = SettableFuture.create();
    try {
      Future<RecordMetadata> future =
          producer.send(new ProducerRecord<>(topic, "" + System.nanoTime(), messageBody));
      RecordMetadata metadata = future.get();
      LOGGER.debug("The offset of the record we just sent is: {}", metadata.offset());
      publisherMetrics.incrementBrokerPublishedMessage();
      msgLog.log(topic, messageBody);
      resultF.set(true);
      return resultF;
    } catch (Throwable e) {
      LOGGER.error("Cannot send the message", e);
      publisherMetrics.incrementBrokerFailedToPublishMessage();
      return Futures.immediateFailedFuture(e);
    }
  }

  private ListenableFuture<Boolean> publishAsync(String topic, String messageBody) {
    try {
      Future<RecordMetadata> future =
          producer.send(
              new ProducerRecord<>(topic, Long.toString(System.nanoTime()), messageBody),
              (metadata, e) -> {
                if (metadata != null && e == null) {
                  LOGGER.debug("The offset of the record we just sent is: {}", metadata.offset());
                  msgLog.log(topic, messageBody);
                  publisherMetrics.incrementBrokerPublishedMessage();
                } else {
                  LOGGER.error("Cannot send the message", e);
                  publisherMetrics.incrementBrokerFailedToPublishMessage();
                }
              });

      // The transformation is lightweight, so we can afford using a directExecutor
      return Futures.transform(
          JdkFutureAdapters.listenInPoolThread(future),
          Objects::nonNull,
          MoreExecutors.directExecutor());
    } catch (Throwable e) {
      LOGGER.error("Cannot send the message", e);
      publisherMetrics.incrementBrokerFailedToPublishMessage();
      return Futures.immediateFailedFuture(e);
    }
  }
}
