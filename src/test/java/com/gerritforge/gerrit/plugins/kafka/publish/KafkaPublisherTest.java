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

package com.gerritforge.gerrit.plugins.kafka.publish;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.gerritforge.gerrit.eventbroker.EventsBrokerConfiguration;
import com.gerritforge.gerrit.eventbroker.log.MessageLogger.Direction;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.session.KafkaSession;
import com.google.gerrit.common.Nullable;
import com.google.gerrit.server.events.Event;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaPublisherTest {
  private static final String TOPIC = "the-topic";

  @Mock private KafkaSession session;
  @Mock private KafkaProperties properties;
  @Mock private EventsBrokerConfiguration configuration;

  private final JsonObject eventJson = new JsonObject();
  private KafkaPublisher publisher;

  @Before
  public void setUp() {
    publisher =
        new KafkaPublisher(session, new Gson(), properties, configuration) {
          @Override
          public JsonObject eventToJson(Event event) {
            return eventJson;
          }
        };
    when(configuration.getPartitionsForTopic(TOPIC)).thenReturn(List.of("configured"));
  }

  @Test
  public void shouldFailWhenPartitionPropertyIsNotConfigured() {
    assertPushFailure(null, String.format("No partition property configured for topic %s", TOPIC));
  }

  @Test
  public void shouldFailWhenEventHasNoPartitionProperty() {
    String missingPartitionProperty = "missing";
    assertPushFailure(
        missingPartitionProperty,
        String.format(
            "Event has no partition property %s for topic %s", missingPartitionProperty, TOPIC));
  }

  @Test
  public void shouldFailWhenPartitionPropertyIsNotPrimitive() {
    String arrayPartitionProperty = "values";
    eventJson.add(arrayPartitionProperty, new JsonArray());
    assertPushFailure(
        arrayPartitionProperty,
        String.format(
            "Event partition property %s is not a primitive for topic %s",
            arrayPartitionProperty, TOPIC));
  }

  @Test
  public void shouldFailWhenPartitionValueIsNotConfigured() {
    String partitionProperty = "partition";
    String unknownPartitionValue = "unknown";
    eventJson.addProperty(partitionProperty, unknownPartitionValue);
    assertPushFailure(
        partitionProperty,
        String.format(
            "Partition value %s is not configured for topic %s", unknownPartitionValue, TOPIC));
  }

  private void assertPushFailure(@Nullable String property, String message) {
    when(configuration.getEventPropertyForTopic(TOPIC)).thenReturn(Optional.ofNullable(property));

    assertThat(
            assertThrows(
                IllegalArgumentException.class, () -> publisher.publish(TOPIC, new TestEvent(), Direction.PUBLISH)))
        .hasMessageThat()
        .isEqualTo(message);
  }

  private static class TestEvent extends Event {
    private TestEvent() {
      super("kafka-publisher-test");
    }
  }
}
