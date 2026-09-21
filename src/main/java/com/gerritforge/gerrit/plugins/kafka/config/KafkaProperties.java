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

package com.gerritforge.gerrit.plugins.kafka.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.gerrit.extensions.annotations.PluginName;
import com.google.gerrit.server.config.PluginConfig;
import com.google.gerrit.server.config.PluginConfigFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

@Singleton
public class KafkaProperties extends java.util.Properties {
  private static final String PROPERTY_CLIENT_TYPE = "clientType";
  private static final ClientType DEFAULT_CLIENT_TYPE = ClientType.NATIVE;
  private static final String PROPERTY_SEND_ASYNC = "sendAsync";
  private static final boolean DEFAULT_SEND_ASYNC = true;
  private static final String PROPERTY_STREAM_EVENTS_TOPIC_NAME = "topic";
  private static final String DEFAULT_STREAM_EVENTS_TOPIC_NAME = "gerrit";

  private static final long serialVersionUID = 0L;
  public static final String SEND_STREAM_EVENTS_FIELD = "sendStreamEvents";
  public static final String STREAM_EVENTS_TOPIC_FIELD = "topic";
  public static final String SEND_ASYNC_FIELD = "sendAsync";

  public static final Boolean SEND_STREAM_EVENTS_DEFAULT = false;
  public static final String STREAM_EVENTS_TOPIC_DEFAULT = "gerrit";
  public static final Boolean SEND_ASYNC_DEFAULT = true;

  public static final String KAFKA_STRING_SERIALIZER = StringSerializer.class.getName();

  public enum ClientType {
    NATIVE,
    REST // Unsupported
  }

  private final String topic;
  private final boolean sendAsync;
  private final boolean sendStreamEvents;
  private final ClientType clientType;

  @Inject
  public KafkaProperties(PluginConfigFactory configFactory, @PluginName String pluginName) {
    super();
    setDefaults();
    PluginConfig fromGerritConfig = configFactory.getFromGerritConfig(pluginName);
    sendStreamEvents =
        fromGerritConfig.getBoolean(SEND_STREAM_EVENTS_FIELD, SEND_STREAM_EVENTS_DEFAULT);
    topic =
        fromGerritConfig.getString(
            PROPERTY_STREAM_EVENTS_TOPIC_NAME, DEFAULT_STREAM_EVENTS_TOPIC_NAME);
    sendAsync = fromGerritConfig.getBoolean(PROPERTY_SEND_ASYNC, DEFAULT_SEND_ASYNC);
    clientType = fromGerritConfig.getEnum(PROPERTY_CLIENT_TYPE, DEFAULT_CLIENT_TYPE);

    applyConfig(fromGerritConfig);
    initDockerizedKafkaServer();
  }

  @VisibleForTesting
  public KafkaProperties(boolean sendAsync, ClientType clientType) {
    super();
    setDefaults();
    topic = DEFAULT_STREAM_EVENTS_TOPIC_NAME;
    this.sendAsync = sendAsync;
    this.sendStreamEvents = true;
    this.clientType = clientType;
    initDockerizedKafkaServer();
  }

  private void setDefaults() {
    put("acks", "all");
    put("retries", 0);
    put("batch.size", 16384);
    put("linger.ms", 1);
    put("buffer.memory", 33554432);
    put("key.serializer", KAFKA_STRING_SERIALIZER);
    put("value.serializer", KAFKA_STRING_SERIALIZER);
    put("reconnect.backoff.ms", 5000L);
  }

  private void applyConfig(PluginConfig config) {
    for (String name : config.getNames()) {
      Object value = config.getString(name);
      String propName =
          CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, name).replaceAll("-", ".");
      put(propName, value);
    }
  }

  /** Bootstrap initialization of dockerized Kafka server environment */
  private void initDockerizedKafkaServer() {
    String testBootstrapServer = System.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    if (!Strings.isNullOrEmpty(testBootstrapServer)) {
      this.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testBootstrapServer);
      this.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
      this.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID());
      this.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
      this.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }
  }

  public String getTopic() {
    return topic;
  }

  public boolean isSendAsync() {
    return sendAsync;
  }

  public String getBootstrapServers() {
    return getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
  }

  public boolean isSendStreamEvents() {
    return sendStreamEvents;
  }

  public ClientType getClientType() {
    return clientType;
  }
}
