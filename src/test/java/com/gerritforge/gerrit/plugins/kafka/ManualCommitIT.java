// Copyright (C) 2026 GerritForge, Inc.
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

import static com.google.common.truth.Truth.assertThat;

import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.MessageAcknowledgement;
import com.gerritforge.gerrit.eventbroker.MessageAcknowledgementException;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.google.gerrit.acceptance.LightweightPluginDaemonTest;
import com.google.gerrit.acceptance.NoHttpd;
import com.google.gerrit.acceptance.TestPlugin;
import com.google.gerrit.acceptance.config.GerritConfig;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.EventGsonProvider;
import com.google.gerrit.server.events.ProjectCreatedEvent;
import com.google.gson.Gson;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

@NoHttpd
@TestPlugin(name = "events-kafka", sysModule = "com.gerritforge.gerrit.plugins.kafka.Module")
public class ManualCommitIT extends LightweightPluginDaemonTest {
  private static final Duration WAIT_FOR_POLL_TIMEOUT = Duration.ofSeconds(1);
  private static final int PARTITION_0 = 0;
  private static final int PARTITION_1 = 1;
  private static final long PARTITION_0_EVENT_CREATED_ON = 10L;
  private static final long PARTITION_1_EVENT_CREATED_ON = 20L;
  private final Gson gson = new EventGsonProvider().get();
  private KafkaContainer kafka;

  @Override
  public void setUpTestPlugin() throws Exception {
    try {
      kafka = KafkaContainerProvider.get();
      kafka.start();
      System.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    } catch (IllegalStateException e) {
      throw new AssertionError("Cannot start container.", e);
    }
    super.setUpTestPlugin();
  }

  @Override
  public void tearDownTestPlugin() {
    super.tearDownTestPlugin();
    if (kafka != null) {
      kafka.stop();
    }
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  public void shouldSetEnableAutoCommitFalseForConsumer() {
    shouldSetEnableAutoCommitForConsumerAndReadAsSubscriberProperty(false);
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "true")
  public void shouldSetEnableAutoCommitTrueForConsumer() {
    shouldSetEnableAutoCommitForConsumerAndReadAsSubscriberProperty(true);
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  public void shouldCommitOffsetWhenAckIsCalled() throws InterruptedException {
    String topic = "manual_commit_topic";
    Event event = newProjectCreatedEvent(instanceId);
    consumeOneMessage(event, topic, (ev, msgAck) -> msgAck.ack(ev));

    assertThat(getCommittedOffset(topic).offset()).isEqualTo(1L);
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  public void shouldRemovePreviousRecordsFromSamePartitionWhenAckIsCalled() throws Exception {
    String topic = "manual_commit_ack_removes_previous_records_topic";
    CountDownLatch latch = new CountDownLatch(3);
    AtomicInteger consumedMessages = new AtomicInteger();
    AtomicReference<Event> firstEvent = new AtomicReference<>();
    AtomicReference<MessageAcknowledgement<Event>> firstAcknowledgement = new AtomicReference<>();
    AtomicReference<MessageAcknowledgementException> thrown = new AtomicReference<>();
    BrokerApi brokerApi = kafkaBrokerApi();

    try {
      brokerApi.send(topic, newProjectCreatedEvent(instanceId, 1L));
      brokerApi.send(topic, newProjectCreatedEvent(instanceId, 2L));
      brokerApi.send(topic, newProjectCreatedEvent(instanceId, 3L));
      brokerApi.receiveAsync(
          topic,
          (ev, acknowledgement) -> {
            int messageNumber = consumedMessages.incrementAndGet();
            if (messageNumber == 1) {
              firstEvent.set(ev);
              firstAcknowledgement.set(acknowledgement);
            }
            if (messageNumber == 2) {
              acknowledgement.ack(ev);
              try {
                firstAcknowledgement.get().ack(firstEvent.get());
              } catch (MessageAcknowledgementException e) {
                thrown.set(e);
              }
            }
            latch.countDown();
          });

      assertThat(latch.await(WAIT_FOR_POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
    } finally {
      brokerApi.disconnect(topic, null);
    }

    assertThat(getCommittedOffset(topic).offset()).isEqualTo(2L);
    assertThat(thrown.get()).hasMessageThat().contains("Invalid or already acked Event");
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  public void shouldRejectAckForRemovedEventWhenLaterRecordsFromSamePartitionArePending()
      throws Exception {
    String topic = "manual_commit_reject_removed_event_ack_with_pending_records_topic";
    CountDownLatch latch = new CountDownLatch(3);
    AtomicInteger consumedMessages = new AtomicInteger();
    AtomicReference<Event> firstEvent = new AtomicReference<>();
    AtomicReference<MessageAcknowledgement<Event>> firstAcknowledgement = new AtomicReference<>();
    AtomicReference<Event> secondEvent = new AtomicReference<>();
    AtomicReference<MessageAcknowledgement<Event>> secondAcknowledgement = new AtomicReference<>();
    AtomicReference<MessageAcknowledgementException> thrown = new AtomicReference<>();
    BrokerApi brokerApi = kafkaBrokerApi();

    try {
      brokerApi.send(topic, newProjectCreatedEvent(instanceId, 1L));
      brokerApi.send(topic, newProjectCreatedEvent(instanceId, 2L));
      brokerApi.send(topic, newProjectCreatedEvent(instanceId, 3L));
      brokerApi.receiveAsync(
          topic,
          (ev, acknowledgement) -> {
            int messageNumber = consumedMessages.incrementAndGet();
            if (messageNumber == 1) {
              firstEvent.set(ev);
              firstAcknowledgement.set(acknowledgement);
            }
            if (messageNumber == 2) {
              secondEvent.set(ev);
              secondAcknowledgement.set(acknowledgement);
            }
            if (messageNumber == 3) {
              secondAcknowledgement.get().ack(secondEvent.get());
              try {
                firstAcknowledgement.get().ack(firstEvent.get());
              } catch (MessageAcknowledgementException e) {
                thrown.set(e);
              }
            }
            latch.countDown();
          });

      assertThat(latch.await(WAIT_FOR_POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
    } finally {
      brokerApi.disconnect(topic, null);
    }

    assertThat(getCommittedOffset(topic).offset()).isEqualTo(2L);
    assertThat(consumedMessages.get()).isEqualTo(3);
    assertThat(thrown.get()).hasMessageThat().contains("Invalid or already acked Event");
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  public void shouldKeepPendingRecordsFromOtherPartitionsWhenAckIsCalled() throws Exception {
    String topic = createTopicWithPartitions(2);
    CountDownLatch latch = new CountDownLatch(2);
    AtomicReference<Event> partition0Event = new AtomicReference<>();
    AtomicReference<MessageAcknowledgement<Event>> partition0Ack = new AtomicReference<>();
    AtomicReference<Event> partition1Event = new AtomicReference<>();
    AtomicReference<MessageAcknowledgement<Event>> partition1Ack = new AtomicReference<>();
    BrokerApi brokerApi = kafkaBrokerApi();

    try {
      sendToPartition(
          topic, PARTITION_0, newProjectCreatedEvent(instanceId, PARTITION_0_EVENT_CREATED_ON));
      sendToPartition(
          topic, PARTITION_1, newProjectCreatedEvent(instanceId, PARTITION_1_EVENT_CREATED_ON));
      brokerApi.receiveAsync(
          topic,
          (ev, acknowledgement) -> {
            if (ev.eventCreatedOn == PARTITION_0_EVENT_CREATED_ON) {
              partition0Event.set(ev);
              partition0Ack.set(acknowledgement);
            }
            if (ev.eventCreatedOn == PARTITION_1_EVENT_CREATED_ON) {
              partition1Event.set(ev);
              partition1Ack.set(acknowledgement);
            }
            if (partition0Ack.get() != null && partition1Ack.get() != null) {
              partition1Ack.get().ack(partition1Event.get());
              partition0Ack.get().ack(partition0Event.get());
            }
            latch.countDown();
          });

      assertThat(latch.await(WAIT_FOR_POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
    } finally {
      brokerApi.disconnect(topic, null);
    }

    assertThat(getCommittedOffset(topic, PARTITION_0).offset()).isEqualTo(1L);
    assertThat(getCommittedOffset(topic, PARTITION_1).offset()).isEqualTo(1L);
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  public void shouldNotCommitOffsetWhenAckIsNotCalled() throws Exception {
    String topic = "manual_commit_without_ack_topic";
    Event event = newProjectCreatedEvent(instanceId);
    consumeOneMessage(event, topic, (ignoredEvent, ignoredAck) -> {});

    assertThat(getCommittedOffset(topic)).isNull();
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "true")
  public void shouldFailWhenAckIsCalledWithAutoCommitEnabled() throws Exception {
    String topic = "auto_commit_ack_failure_topic";
    AtomicReference<IllegalStateException> thrown = new AtomicReference<>();
    Event event = newProjectCreatedEvent(instanceId);
    consumeOneMessage(
        event,
        topic,
        (ev, acknowledgement) -> {
          try {
            acknowledgement.ack(ev);
          } catch (IllegalStateException e) {
            thrown.set(e);
          }
        });

    assertThat(thrown.get()).hasMessageThat().contains("already acknowledged automatically");
  }

  private void shouldSetEnableAutoCommitForConsumerAndReadAsSubscriberProperty(
      Boolean expectedAutoCommit) {
    KafkaSubscriberProperties kafkaSubscriberProperties =
        plugin.getSysInjector().getInstance(KafkaSubscriberProperties.class);

    assertThat(kafkaSubscriberProperties.isAutoCommitEnabled()).isEqualTo(expectedAutoCommit);
    assertThat(kafkaSubscriberProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
        .isEqualTo(expectedAutoCommit.toString());
  }

  private void consumeOneMessage(
      Event event,
      String topic,
      BiConsumer<Event, MessageAcknowledgement<Event>> acknowledgementConsumer)
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    BrokerApi brokerApi = kafkaBrokerApi();

    try {
      brokerApi.send(topic, event);
      brokerApi.receiveAsync(
          topic,
          (ev, msgAck) -> {
            acknowledgementConsumer.accept(ev, msgAck);
            latch.countDown();
          });

      assertThat(latch.await(WAIT_FOR_POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
    } finally {
      brokerApi.disconnect(topic, null);
    }
  }

  private Event newProjectCreatedEvent(String instanceId) {
    Event eventMessage = new ProjectCreatedEvent();
    eventMessage.instanceId = instanceId;
    return eventMessage;
  }

  private Event newProjectCreatedEvent(String instanceId, long eventCreatedOn) {
    Event event = newProjectCreatedEvent(instanceId);
    event.eventCreatedOn = eventCreatedOn;
    return event;
  }

  private OffsetAndMetadata getCommittedOffset(String topic) {
    return getCommittedOffset(topic, PARTITION_0);
  }

  private OffsetAndMetadata getCommittedOffset(String topic, int partition) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaSubscriberProperties().getGroupId());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (KafkaConsumer<byte[], byte[]> verifierConsumer = new KafkaConsumer<>(props)) {
      TopicPartition tp = new TopicPartition(topic, partition);
      Map<TopicPartition, OffsetAndMetadata> committed = verifierConsumer.committed(Set.of(tp));
      return committed.get(tp);
    }
  }

  private String createTopicWithPartitions(int partitions) throws Exception {
    String topic = "manual_commit_partitioned_topic_" + System.nanoTime();
    try (Admin admin = Admin.create(kafkaProperties())) {
      admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
    }
    return topic;
  }

  private void sendToPartition(String topic, int partition, Event event) throws Exception {
    try (Producer<String, String> producer = new KafkaProducer<>(kafkaProperties())) {
      producer.send(new ProducerRecord<>(topic, partition, null, gson.toJson(event))).get();
    }
  }

  private BrokerApi kafkaBrokerApi() {
    return plugin.getSysInjector().getInstance(BrokerApi.class);
  }

  private KafkaProperties kafkaProperties() {
    return plugin.getSysInjector().getInstance(KafkaProperties.class);
  }

  private KafkaSubscriberProperties kafkaSubscriberProperties() {
    return plugin.getSysInjector().getInstance(KafkaSubscriberProperties.class);
  }
}
