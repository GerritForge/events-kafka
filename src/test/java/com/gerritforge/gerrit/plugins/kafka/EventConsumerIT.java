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

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.gerrit.acceptance.LightweightPluginDaemonTest;
import com.google.gerrit.acceptance.NoHttpd;
import com.google.gerrit.acceptance.PushOneCommit;
import com.google.gerrit.acceptance.TestPlugin;
import com.google.gerrit.acceptance.UseLocalDisk;
import com.google.gerrit.acceptance.config.GerritConfig;
import com.google.gerrit.extensions.api.changes.ReviewInput;
import com.google.gerrit.extensions.common.ChangeMessageInfo;
import com.google.gerrit.server.events.CommentAddedEvent;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.EventGsonProvider;
import com.google.gerrit.server.events.ProjectCreatedEvent;
import com.google.gson.Gson;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

@NoHttpd
@TestPlugin(name = "events-kafka", sysModule = "com.gerritforge.gerrit.plugins.kafka.Module")
public class EventConsumerIT extends LightweightPluginDaemonTest {
  static final Duration KAFKA_POLL_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration WAIT_FOR_POLL_TIMEOUT = Duration.ofSeconds(30);
  private KafkaContainer kafka;
  private final Gson gson = new EventGsonProvider().get();

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
  @GerritConfig(name = "plugin.events-kafka.groupId", value = "test-consumer-group")
  @GerritConfig(
      name = "plugin.events-kafka.keyDeserializer",
      value = "org.apache.kafka.common.serialization.StringDeserializer")
  @GerritConfig(
      name = "plugin.events-kafka.valueDeserializer",
      value = "org.apache.kafka.common.serialization.StringDeserializer")
  @GerritConfig(name = "plugin.events-kafka.sendStreamEvents", value = "true")
  public void consumeEvents() throws Exception {
    List<String> events = reviewNewChangeAndGetStreamEvents();

    // There are 6 events are received in the following order:
    // 1. refUpdate:        ref: refs/sequences/changes
    // 2. refUpdate:        ref: refs/changes/01/1/1
    // 3. refUpdate:        ref: refs/changes/01/1/meta
    // 4. patchset-created: ref: refs/changes/01/1/1
    // 5. refUpdate:        ref: refs/changes/01/1/meta
    // 6. comment-added:    ref: refs/heads/master

    assertThat(events).hasSize(6);
    String commentAddedEventJson = Iterables.getLast(events);

    Event event = gson.fromJson(commentAddedEventJson, Event.class);
    assertThat(event).isInstanceOf(CommentAddedEvent.class);

    CommentAddedEvent commentAddedEvent = (CommentAddedEvent) event;
    assertThat(commentAddedEvent.comment).isEqualTo("Patch Set 1: Code-Review+1\n\nLGTM");
  }

  @Test
  @UseLocalDisk
  @GerritConfig(name = "plugin.events-kafka.groupId", value = "test-consumer-group")
  @GerritConfig(
      name = "plugin.events-kafka.keyDeserializer",
      value = "org.apache.kafka.common.serialization.StringDeserializer")
  @GerritConfig(
      name = "plugin.events-kafka.valueDeserializer",
      value = "org.apache.kafka.common.serialization.StringDeserializer")
  @GerritConfig(name = "plugin.events-kafka.sendStreamEvents", value = "false")
  public void shouldNotSendStreamEventsWhenDisabled() throws Exception {
    List<String> events = reviewNewChangeAndGetStreamEvents();

    assertThat(events).isEmpty();
  }

  @Test
  @UseLocalDisk
  @GerritConfig(name = "plugin.events-kafka.groupId", value = "test-consumer-group")
  @GerritConfig(
      name = "plugin.events-kafka.keyDeserializer",
      value = "org.apache.kafka.common.serialization.StringDeserializer")
  @GerritConfig(
      name = "plugin.events-kafka.valueDeserializer",
      value = "org.apache.kafka.common.serialization.StringDeserializer")
  @GerritConfig(name = "plugin.events-kafka.pollingIntervalMs", value = "500")
  public void consumeEventsWithExternalGroupId() throws Exception {
    String topic = "a_topic";
    String consumerGroup1 = "consumer-group-1";
    Event eventMessage = new ProjectCreatedEvent();
    eventMessage.instanceId = "test-instance-id-1";
    List<Event> receivedEventsWithGroupId1 = new ArrayList<>();

    BrokerApi kafkaBrokerApi = kafkaBrokerApi();
    kafkaBrokerApi.send(topic, eventMessage);
    kafkaBrokerApi.receiveAsync(topic, consumerGroup1, receivedEventsWithGroupId1::add);

    waitUntil(() -> receivedEventsWithGroupId1.size() == 1, WAIT_FOR_POLL_TIMEOUT);
    assertThat(gson.toJson(receivedEventsWithGroupId1.get(0))).isEqualTo(gson.toJson(eventMessage));
  }

  @Test
  @UseLocalDisk
  @GerritConfig(name = "plugin.events-kafka.groupId", value = "test-consumer-group")
  @GerritConfig(
      name = "plugin.events-kafka.keyDeserializer",
      value = "org.apache.kafka.common.serialization.StringDeserializer")
  @GerritConfig(
      name = "plugin.events-kafka.valueDeserializer",
      value = "org.apache.kafka.common.serialization.StringDeserializer")
  @GerritConfig(name = "plugin.events-kafka.pollingIntervalMs", value = "500")
  public void shouldReplayAllEvents() throws InterruptedException {
    String topic = "a_topic";
    Event eventMessage = new ProjectCreatedEvent();
    eventMessage.instanceId = "test-instance-id";

    List<Event> receivedEvents = new ArrayList<>();

    BrokerApi kafkaBrokerApi = kafkaBrokerApi();
    kafkaBrokerApi.send(topic, eventMessage);

    kafkaBrokerApi.receiveAsync(topic, receivedEvents::add);

    waitUntil(() -> receivedEvents.size() == 1, WAIT_FOR_POLL_TIMEOUT);

    assertThat(receivedEvents.get(0).instanceId).isEqualTo(eventMessage.instanceId);

    kafkaBrokerApi.replayAllEvents(topic);
    waitUntil(() -> receivedEvents.size() == 2, WAIT_FOR_POLL_TIMEOUT);

    assertThat(receivedEvents.get(1).instanceId).isEqualTo(eventMessage.instanceId);
  }

  private BrokerApi kafkaBrokerApi() {
    return plugin.getSysInjector().getInstance(BrokerApi.class);
  }

  private KafkaProperties kafkaProperties() {
    return plugin.getSysInjector().getInstance(KafkaProperties.class);
  }

  private List<String> reviewNewChangeAndGetStreamEvents() throws Exception {
    PushOneCommit.Result r = createChange();

    ReviewInput in = ReviewInput.recommend();
    in.message = "LGTM";
    gApi.changes().id(r.getChangeId()).revision("current").review(in);
    List<ChangeMessageInfo> messages =
        new ArrayList<>(gApi.changes().id(r.getChangeId()).get().messages);
    assertThat(messages).hasSize(2);
    String expectedMessage = "Patch Set 1: Code-Review+1\n\nLGTM";
    assertThat(messages.get(1).message).isEqualTo(expectedMessage);

    List<String> events = new ArrayList<>();
    KafkaProperties kafkaProperties = kafkaProperties();
    try (Consumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties)) {
      consumer.subscribe(Collections.singleton(kafkaProperties.getTopic()));
      ConsumerRecords<String, String> records = consumer.poll(KAFKA_POLL_TIMEOUT);
      for (ConsumerRecord<String, String> record : records) {
        events.add(record.value());
      }
    }
    return events;
  }

  // XXX: Remove this method when merging into stable-3.3, since waitUntil is
  // available in Gerrit core.
  public static void waitUntil(Supplier<Boolean> waitCondition, Duration timeout)
      throws InterruptedException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (!waitCondition.get()) {
      if (stopwatch.elapsed().compareTo(timeout) > 0) {
        throw new InterruptedException();
      }
      MILLISECONDS.sleep(50);
    }
  }
}
