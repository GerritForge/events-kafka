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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.ContextAwareConsumer;
import com.google.common.base.Stopwatch;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

@NoHttpd
@TestPlugin(name = "events-kafka", sysModule = "com.gerritforge.gerrit.plugins.kafka.Module")
public class ManualCommitIT extends LightweightPluginDaemonTest {
  private static final Duration WAIT_FOR_POLL_TIMEOUT = Duration.ofSeconds(30);
  private KafkaContainer kafka;

  @Override
  public void setUpTestPlugin() throws Exception {
    try {
      kafka = KafkaContainerProvider.get();
      kafka.start();
      System.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    } catch (IllegalStateException e) {
      fail("Cannot start container. Is docker daemon running?");
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
  @GerritConfig(name = "plugin.events-kafka.groupId", value = "manual-commit-group")
  @GerritConfig(name = "plugin.events-kafka.enableAutoCommit", value = "false")
  @GerritConfig(name = "plugin.events-kafka.pollingIntervalMs", value = "500")
  public void shouldCommitManually() throws InterruptedException {
    String topic = "manual_commit_topic";
    Event eventMessage = new ProjectCreatedEvent();
    eventMessage.instanceId = "instance-1";

    List<Event> receivedEvents = new ArrayList<>();
    AtomicInteger commitCount = new AtomicInteger(0);

    BrokerApi kafkaBrokerApi = plugin.getSysInjector().getInstance(BrokerApi.class);

    // Publish message
    kafkaBrokerApi.send(topic, eventMessage);

    // Consume and Commit
    ContextAwareConsumer<Event> consumer =
        (event, ctx) -> {
          receivedEvents.add(event);
          ctx.commit();
          commitCount.incrementAndGet();
        };

    kafkaBrokerApi.receiveAsync(topic, consumer);

    waitUntil(() -> receivedEvents.size() == 1, WAIT_FOR_POLL_TIMEOUT);
    assertThat(commitCount.get()).isEqualTo(1);

    // We cannot easily verify that the offset is committed on the broker without
    // restarting consumer
    // or using AdminClient, but checking that ctx.commit() was called and didn't
    // fail is a good start.
  }

  // Helper method similar to EventConsumerIT
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
