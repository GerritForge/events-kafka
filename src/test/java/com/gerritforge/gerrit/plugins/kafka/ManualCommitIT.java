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
import static com.google.gerrit.acceptance.WaitUtil.waitUntil;

import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.MessageContext;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.google.gerrit.acceptance.LightweightPluginDaemonTest;
import com.google.gerrit.acceptance.NoHttpd;
import com.google.gerrit.acceptance.TestPlugin;
import com.google.gerrit.acceptance.UseLocalDisk;
import com.google.gerrit.acceptance.config.GerritConfig;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.ProjectCreatedEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

@NoHttpd
@TestPlugin(name = "events-kafka", sysModule = "com.gerritforge.gerrit.plugins.kafka.Module")
public class ManualCommitIT extends LightweightPluginDaemonTest {
  private static final Duration WAIT_FOR_POLL_TIMEOUT = Duration.ofSeconds(30);
  private static final boolean DO_NOT_ACK = false;
  private static final boolean ACK = true;
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
  @UseLocalDisk
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  public void shouldSetEnableAutoCommitFalseForConsumer() {
    KafkaSubscriberProperties kafkaSubscriberProperties =
        plugin.getSysInjector().getInstance(KafkaSubscriberProperties.class);

    assertThat(kafkaSubscriberProperties.isAutoCommitEnabled()).isFalse();
    assertThat(kafkaSubscriberProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
        .isEqualTo("false");
  }

  @Test
  @UseLocalDisk
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "true")
  public void shouldSetEnableAutoCommitTrueForConsumer() {
    KafkaSubscriberProperties kafkaSubscriberProperties =
        plugin.getSysInjector().getInstance(KafkaSubscriberProperties.class);

    assertThat(kafkaSubscriberProperties.isAutoCommitEnabled()).isTrue();
    assertThat(kafkaSubscriberProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
        .isEqualTo("true");
  }

  @Test
  @UseLocalDisk
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  @GerritConfig(name = "plugin.events-kafka.autoCommitIntervalMs", value = "200")
  public void shouldCommitManually() throws InterruptedException {
    String topic = "manual_commit_topic";
    OffsetAndMetadata committedOffset =
        consumeOneMessageAndGetOffset(
            topic, "instance-ack", ACK, () -> getCommittedOffset(topic) != null);

    assertThat(committedOffset.offset()).isEqualTo(1L);
  }

  @Test
  @UseLocalDisk
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  @GerritConfig(name = "plugin.events-kafka.autoCommitIntervalMs", value = "200")
  public void shouldNotCommitOffsetWithoutAck() throws Exception {
    String topic = "manual_commit_without_ack_topic";
    assertThat(
            consumeOneMessageAndGetOffset(
                topic, "instance-no-ack", DO_NOT_ACK, () -> getCommittedOffset(topic) == null))
        .isNull();
  }

  @Test
  @UseLocalDisk
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  @GerritConfig(name = "plugin.events-kafka.autoCommitIntervalMs", value = "500")
  public void shouldCommitOnAckAfterIntervalElapsed() throws Exception {
    String topic = "manual_commit_interval_topic";
    CountDownLatch secondAckDone = new CountDownLatch(1);
    AtomicInteger messageIndex = new AtomicInteger(0);

    BrokerApi brokerApi = kafkaBrokerApi();
    // Ack #1 immediately, then delay ack #2 beyond commit interval.
    brokerApi.receiveAsyncWithContext(
        topic,
        (event, ctx) -> {
          int index = messageIndex.incrementAndGet();
          if (index == 1) {
            ctx.ack();
            return;
          }
          sleep(600);
          ctx.ack();
          secondAckDone.countDown();
        });
    brokerApi.send(topic, newProjectCreatedEvent("ev-1"));
    brokerApi.send(topic, newProjectCreatedEvent("ev-2"));

    try {
      // Once ack #2 happens, the staged offsets are committed together to offset 2.
      await(secondAckDone);
      waitUntil(() -> getCommittedOffset(topic).offset() == 2L, WAIT_FOR_POLL_TIMEOUT);
    } finally {
      brokerApi.disconnect(topic, null);
    }
  }

  @Test
  @UseLocalDisk
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  @GerritConfig(name = "plugin.events-kafka.autoCommitIntervalMs", value = "500")
  public void shouldCommitOnlyContiguousAckedOffsets() throws Exception {
    String topic = "manual_commit_contiguous_topic";
    CountDownLatch thirdAckDone = new CountDownLatch(1);
    CountDownLatch secondAckDone = new CountDownLatch(1);
    AtomicInteger messageIndex = new AtomicInteger(0);
    AtomicReference<MessageContext> secondContext = new AtomicReference<>();

    BrokerApi brokerApi = kafkaBrokerApi();
    // Ack order is 1, 3, then 2. This creates a gap before acking #2.
    brokerApi.receiveAsyncWithContext(
        topic,
        (event, ctx) -> {
          int index = messageIndex.incrementAndGet();
          if (index == 1) {
            ctx.ack();
            return;
          }

          if (index == 2) {
            secondContext.set(ctx);
            return;
          }

          sleep(600);
          ctx.ack();
          thirdAckDone.countDown();
          sleep(600);
          secondContext.get().ack();
          secondAckDone.countDown();
        });
    brokerApi.send(topic, newProjectCreatedEvent("ev-1"));
    brokerApi.send(topic, newProjectCreatedEvent("ev-2"));
    brokerApi.send(topic, newProjectCreatedEvent("ev-3"));

    try {
      // With acked offsets 1 and 3 (gap at 2), only offset 1 is committable.
      await(thirdAckDone);
      waitUntil(() -> getCommittedOffset(topic) != null, WAIT_FOR_POLL_TIMEOUT);
      assertThat(getCommittedOffset(topic).offset()).isEqualTo(1L);

      // Once the gap is filled (ack #2), commit advances to offset 3.
      await(secondAckDone);
      waitUntil(() -> getCommittedOffset(topic).offset() == 3L, WAIT_FOR_POLL_TIMEOUT);
    } finally {
      brokerApi.disconnect(topic, null);
    }
  }

  private OffsetAndMetadata consumeOneMessageAndGetOffset(
      String topic, String instanceId, boolean ack, java.util.function.Supplier<Boolean> condition)
      throws InterruptedException {
    List<Event> receivedEvents = new ArrayList<>();
    BrokerApi brokerApi = kafkaBrokerApi();

    brokerApi.send(topic, newProjectCreatedEvent(instanceId));
    brokerApi.receiveAsyncWithContext(
        topic,
        (event, ctx) -> {
          receivedEvents.add(event);
          if (ack) {
            ctx.ack();
          }
        });

    waitUntil(() -> receivedEvents.size() == 1, WAIT_FOR_POLL_TIMEOUT);
    waitUntil(condition, WAIT_FOR_POLL_TIMEOUT);
    brokerApi.disconnect(topic, null);
    return getCommittedOffset(topic);
  }

  private Event newProjectCreatedEvent(String instanceId) {
    Event eventMessage = new ProjectCreatedEvent();
    eventMessage.instanceId = instanceId;
    return eventMessage;
  }

  private OffsetAndMetadata getCommittedOffset(String topic) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaSubscriberProperties().getGroupId());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (KafkaConsumer<byte[], byte[]> verifierConsumer = new KafkaConsumer<>(props)) {
      TopicPartition tp = new TopicPartition(topic, 0);
      Map<TopicPartition, OffsetAndMetadata> committed = verifierConsumer.committed(Set.of(tp));
      return committed.get(tp);
    }
  }

  private BrokerApi kafkaBrokerApi() {
    return plugin.getSysInjector().getInstance(BrokerApi.class);
  }

  private KafkaSubscriberProperties kafkaSubscriberProperties() {
    return plugin.getSysInjector().getInstance(KafkaSubscriberProperties.class);
  }

  private void await(CountDownLatch latch) {
    try {
      assertThat(latch.await(WAIT_FOR_POLL_TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while waiting for test latch", e);
    }
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while sleeping in test", e);
    }
  }
}
