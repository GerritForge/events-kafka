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

import static com.gerritforge.gerrit.eventbroker.TopicSubscriber.topicSubscriber;
import static com.gerritforge.gerrit.eventbroker.TopicSubscriberWithGroupId.topicSubscriberWithGroupId;
import static org.mockito.Mockito.verify;

import com.gerritforge.gerrit.eventbroker.AckAwareConsumer;
import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.TopicSubscriber;
import com.gerritforge.gerrit.eventbroker.TopicSubscriberWithGroupId;
import com.gerritforge.gerrit.plugins.kafka.publish.KafkaPublisher;
import com.google.gerrit.server.events.Event;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ManagerTest {
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String GROUP_ID = "group-id";

  @Mock private KafkaPublisher publisher;
  @Mock private BrokerApi brokerApi;
  @Mock private AckAwareConsumer<Event> consumer;

  @Test
  public void shouldPreservePartitionWhenRebindingConsumer() {
    TopicSubscriber topicSubscriber = topicSubscriber(TOPIC, consumer);
    TopicSubscriberWithGroupId partitionSubscriber =
        topicSubscriberWithGroupId(GROUP_ID, topicSubscriber, Optional.of(PARTITION));
    Manager manager = new Manager(publisher, Set.of(), Set.of(partitionSubscriber), brokerApi);

    manager.start();

    verify(brokerApi).receiveAsyncWithPartition(TOPIC, PARTITION, GROUP_ID, consumer);
  }
}
