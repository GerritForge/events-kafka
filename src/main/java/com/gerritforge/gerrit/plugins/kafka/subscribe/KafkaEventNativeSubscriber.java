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

import com.gerritforge.gerrit.eventbroker.AckAwareConsumer;
import com.gerritforge.gerrit.eventbroker.MessageAcknowledgementException;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaEventNativeSubscriber implements KafkaEventSubscriber {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final int DELAY_RECONNECT_AFTER_FAILURE_MSEC = 1000;

  private final OneOffRequestContext oneOffCtx;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final Deserializer<Event> valueDeserializer;
  private final KafkaSubscriberProperties configuration;
  private final Optional<Integer> partition;
  private final ExecutorService executor;
  private final KafkaEventSubscriberMetrics subscriberMetrics;
  private final KafkaConsumerFactory consumerFactory;
  private final Deserializer<byte[]> keyDeserializer;

  private AckAwareConsumer<Event> messageProcessor;
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
      @Assisted("externalGroupId") Optional<String> externalGroupId,
      @Assisted("partition") Optional<Integer> partition) {

    this.oneOffCtx = oneOffCtx;
    this.executor = executor;
    this.subscriberMetrics = subscriberMetrics;
    this.consumerFactory = consumerFactory;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.externalGroupId = externalGroupId;
    this.configuration = (KafkaSubscriberProperties) configuration.clone();
    this.partition = partition;
    externalGroupId.ifPresent(gid -> this.configuration.setProperty("group.id", gid));
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#subscribe(java.lang.String, AckAwareConsumer)
   */
  @Override
  public void subscribe(String topic, AckAwareConsumer<Event> acknowledgementConsumer) {
    this.topic = topic;
    this.messageProcessor = acknowledgementConsumer;
    logger.atInfo().log(
        "Kafka consumer subscribing to topic alias [%s]%s for event topic [%s] with groupId [%s]",
        topic,
        partition.map(p -> String.format("[partition:%s]", p)).orElse(""),
        topic,
        configuration.getGroupId());
    runReceiver(consumerFactory.create(configuration, keyDeserializer));
  }

  private void runReceiver(Consumer<byte[], byte[]> consumer) {
    final ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread()
          .setContextClassLoader(KafkaEventNativeSubscriber.class.getClassLoader());
      partition.ifPresent(partitionNumber -> assignPartition(consumer, partitionNumber));
      if (partition.isEmpty()) {
        consumer.subscribe(Collections.singleton(topic));
      }
      receiver = new ReceiverJob(consumer);
      executor.execute(receiver);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  private void assignPartition(Consumer<byte[], byte[]> consumer, int partitionNumber) {
    if (consumer.partitionsFor(topic).stream()
        .noneMatch(partitionInfo -> partitionInfo.partition() == partitionNumber)) {
      consumer.close();
      throw new IllegalArgumentException(
          String.format("Kafka partition %d does not exist for topic %s", partitionNumber, topic));
    }
    consumer.assign(Set.of(new TopicPartition(topic, partitionNumber)));
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
  public AckAwareConsumer<Event> getMessageProcessor() {
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
    private final HashMap<Event, ConsumerRecord<byte[], byte[]>> ackRecords = new HashMap<>();

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

    private void kafkaAck(Event event) {
      ConsumerRecord<byte[], byte[]> consumerRecord = ackRecords.get(event);
      if (consumerRecord == null) {
        throw new MessageAcknowledgementException("Invalid or already acked Event");
      }

      TopicPartition tp = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
      long offset = consumerRecord.offset() + 1;
      try {
        consumer.commitSync(Map.of(tp, new OffsetAndMetadata(offset)));
        logger.atFine().log("Committed offset %d on %s", offset, tp);
        ackRecords.remove(event);
      } catch (KafkaException e) {
        throw new MessageAcknowledgementException(
            String.format("Failed to acknowledge offset %d on %s", offset, tp), e);
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
                  ackRecords.put(event, consumerRecord);
                  messageProcessor.accept(
                      event,
                      configuration.isAutoCommitEnabled()
                          ? KafkaAutoAcknowledgement.INSTANCE
                          : this::kafkaAck);
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
