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
import com.gerritforge.gerrit.eventbroker.MessageAcknowledgement;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.google.gerrit.acceptance.LightweightPluginDaemonTest;
import com.google.gerrit.acceptance.NoHttpd;
import com.google.gerrit.acceptance.TestPlugin;
import com.google.gerrit.acceptance.config.GerritConfig;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.ProjectCreatedEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
  private static final Duration WAIT_FOR_POLL_TIMEOUT = Duration.ofSeconds(10);
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
    KafkaSubscriberProperties kafkaSubscriberProperties =
        plugin.getSysInjector().getInstance(KafkaSubscriberProperties.class);

    assertThat(kafkaSubscriberProperties.isAutoCommitEnabled()).isFalse();
    assertThat(kafkaSubscriberProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
        .isEqualTo("false");
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "true")
  public void shouldSetEnableAutoCommitTrueForConsumer() {
    KafkaSubscriberProperties kafkaSubscriberProperties =
        plugin.getSysInjector().getInstance(KafkaSubscriberProperties.class);

    assertThat(kafkaSubscriberProperties.isAutoCommitEnabled()).isTrue();
    assertThat(kafkaSubscriberProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
        .isEqualTo("true");
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  public void shouldCommitOffSetWhenAckIsCalled() throws InterruptedException {
    String topic = "manual_commit_topic";
    consumeOneMessage(topic, MessageAcknowledgement::ack, () -> true);

    assertThat(getCommittedOffset(topic).offset()).isEqualTo(1L);
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  public void shouldNotCommitOffsetWhenAckIsNotCalled() throws Exception {
    String topic = "manual_commit_without_ack_topic";
    consumeOneMessage(topic, m -> {}, () -> true);

    assertThat(getCommittedOffset(topic)).isNull();
  }

  @Test
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "true")
  public void shouldFailWhenAckIsCalledWithAutoCommitEnabled() throws Exception {
    String topic = "auto_commit_ack_failure_topic";
    AtomicReference<IllegalStateException> thrown = new AtomicReference<>();
    consumeOneMessage(
        topic,
        acknowledgement -> {
          try {
            acknowledgement.ack();
          } catch (IllegalStateException e) {
            thrown.set(e);
          }
        },
        () -> true);

    assertThat(thrown.get()).hasMessageThat().contains("already acknowledged automatically");
  }

  private void consumeOneMessage(
      String topic,
      Consumer<MessageAcknowledgement> acknowledgementConsumer,
      Supplier<Boolean> condition)
      throws InterruptedException {
    List<Event> receivedEvents = new ArrayList<>();
    BrokerApi brokerApi = kafkaBrokerApi();

    try {
      brokerApi.send(topic, newProjectCreatedEvent(instanceId));
      brokerApi.receiveAsync(
          topic,
          (event, msgAck) -> {
            receivedEvents.add(event);
            acknowledgementConsumer.accept(msgAck);
          });

      waitUntil(() -> receivedEvents.size() == 1, WAIT_FOR_POLL_TIMEOUT);
      waitUntil(condition, WAIT_FOR_POLL_TIMEOUT);
    } finally {
      brokerApi.disconnect(topic, null);
    }
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
}
