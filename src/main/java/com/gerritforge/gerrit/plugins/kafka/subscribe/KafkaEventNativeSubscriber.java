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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.gerritforge.gerrit.eventbroker.ContextAwareConsumer;
import com.gerritforge.gerrit.plugins.kafka.broker.ConsumerExecutor;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.google.common.flogger.FluentLogger;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.util.ManualRequestContext;
import com.google.gerrit.server.util.OneOffRequestContext;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaEventNativeSubscriber implements KafkaEventSubscriber {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final int DELAY_RECONNECT_AFTER_FAILURE_MSEC = 1000;

  private final OneOffRequestContext oneOffCtx;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final Deserializer<Event> valueDeserializer;
  private final KafkaSubscriberProperties configuration;
  private final ExecutorService executor;
  private final KafkaEventSubscriberMetrics subscriberMetrics;
  private final KafkaConsumerFactory consumerFactory;
  private final Deserializer<byte[]> keyDeserializer;
  private final boolean autoCommitEnabled;

  private ContextAwareConsumer<Event> messageProcessor;
  private String topic;
  private AtomicBoolean resetOffset = new AtomicBoolean(false);

  private volatile ReceiverJob receiver;
  private final Optional<String> externalGroupId;

  @Inject
  public KafkaEventNativeSubscriber(
      KafkaSubscriberProperties configuration,
      KafkaConsumerFactory consumerFactory,
      Deserializer<byte[]> keyDeserializer,
      Deserializer<Event> valueDeserializer,
      OneOffRequestContext oneOffCtx,
      @ConsumerExecutor ExecutorService executor,
      KafkaEventSubscriberMetrics subscriberMetrics,
      @Assisted Optional<String> externalGroupId) {

    this.oneOffCtx = oneOffCtx;
    this.executor = executor;
    this.subscriberMetrics = subscriberMetrics;
    this.consumerFactory = consumerFactory;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.externalGroupId = externalGroupId;
    this.configuration = (KafkaSubscriberProperties) configuration.clone();
    externalGroupId.ifPresent(gid -> this.configuration.setProperty("group.id", gid));
    this.autoCommitEnabled = this.configuration.isAutoCommitEnabled();
  }

  @Override
  public void subscribe(String topic, ContextAwareConsumer<Event> contextAwareConsumer) {
    this.topic = topic;
    this.messageProcessor = contextAwareConsumer;
    logger.atInfo().log(
        "Kafka consumer subscribing to topic alias [%s] for event topic [%s] with groupId [%s]",
        topic, topic, configuration.getGroupId());
    runReceiver(consumerFactory.create(configuration, keyDeserializer));
  }

  private void runReceiver(Consumer<byte[], byte[]> consumer) {
    final ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread()
          .setContextClassLoader(KafkaEventNativeSubscriber.class.getClassLoader());
      consumer.subscribe(Collections.singleton(topic));
      receiver = new ReceiverJob(consumer);
      executor.execute(receiver);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#shutdown()
   */
  @Override
  public void shutdown() {
    closed.set(true);
    receiver.wakeup();
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#getMessageProcessor()
   */
  @Override
  public ContextAwareConsumer<Event> getMessageProcessor() {
    return messageProcessor;
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#getTopic()
   */
  @Override
  public String getTopic() {
    return topic;
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#resetOffset()
   */
  @Override
  public void resetOffset() {
    resetOffset.set(true);
  }

  @Override
  public Optional<String> getExternalGroupId() {
    return externalGroupId;
  }

  private class ReceiverJob implements Runnable {
    private final Consumer<byte[], byte[]> consumer;

    public ReceiverJob(Consumer<byte[], byte[]> consumer) {
      this.consumer = consumer;
    }

    public void wakeup() {
      consumer.wakeup();
    }

    @Override
    public void run() {
      try {
        consume();
      } catch (Exception e) {
        logger.atSevere().withCause(e).log("Consumer loop of topic %s ended", topic);
      }
    }

    private void consume() throws InterruptedException {
      try {
        while (!closed.get()) {
          if (resetOffset.getAndSet(false)) {
            // Make sure there is an assignment for this consumer
            while (consumer.assignment().isEmpty() && !closed.get()) {
              logger.atInfo().log(
                  "Resetting offset: no partitions assigned to the consumer, request assignment.");
              consumer.poll(Duration.ofMillis(configuration.getPollingInterval()));
            }
            consumer.seekToBeginning(consumer.assignment());
          }
          ConsumerRecords<byte[], byte[]> consumerRecords =
              consumer.poll(Duration.ofMillis(configuration.getPollingInterval()));
          consumerRecords.forEach(
              consumerRecord -> {
                try (ManualRequestContext ctx = oneOffCtx.open()) {
                  Event event =
                      valueDeserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
                  messageProcessor.accept(
                      event,
                      new KafkaCommitMessageContext(autoCommitEnabled, consumerRecord, consumer));
                } catch (Exception e) {
                  logger.atSevere().withCause(e).log(
                      "Malformed event '%s': [Exception: %s]",
                      new String(consumerRecord.value(), UTF_8), e.toString());
                  subscriberMetrics.incrementSubscriberFailedToConsumeMessage();
                }
              });
        }
      } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) {
          logger.atSevere().withCause(e).log("Consumer loop of topic %s interrupted", topic);
          reconnectAfterFailure();
        }
      } catch (Exception e) {
        subscriberMetrics.incrementSubscriberFailedToPollMessages();
        logger.atSevere().withCause(e).log(
            "Existing consumer loop of topic %s because of a non-recoverable exception", topic);
        reconnectAfterFailure();
      } finally {
        consumer.close();
      }
    }

    private void reconnectAfterFailure() throws InterruptedException {
      // Random delay with average of DELAY_RECONNECT_AFTER_FAILURE_MSEC
      // for avoiding hammering exactly at the same interval in case of failure
      long reconnectDelay =
          DELAY_RECONNECT_AFTER_FAILURE_MSEC / 2
              + new Random().nextInt(DELAY_RECONNECT_AFTER_FAILURE_MSEC);
      Thread.sleep(reconnectDelay);
      runReceiver(consumerFactory.create(configuration, keyDeserializer));
    }
  }
}
