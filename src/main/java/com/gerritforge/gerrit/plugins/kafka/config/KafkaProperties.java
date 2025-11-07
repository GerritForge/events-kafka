// Copyright (C) 2016 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.gerrit.common.Nullable;
import com.google.gerrit.extensions.annotations.PluginName;
import com.google.gerrit.server.config.ConfigUtil;
import com.google.gerrit.server.config.PluginConfig;
import com.google.gerrit.server.config.PluginConfigFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

@Singleton
public class KafkaProperties extends java.util.Properties {
  public static final String REST_API_URI_ID_PLACEHOLDER = "${kafka_rest_id}";

  private static final String PROPERTY_HTTP_WIRE_LOG = "httpWireLog";
  private static final boolean DEFAULT_HTTP_WIRE_LOG = false;
  private static final String PROPERTY_REST_API_URI = "restApiUri";
  private static final String PROPERTY_REST_API_USERNAME = "restApiUsername";
  private static final String PROPERTY_REST_API_PASSWORD = "restApiPassword";
  private static final String PROPERTY_REST_API_TIMEOUT = "restApiTimeout";
  private static final Duration DEFAULT_REST_API_TIMEOUT = Duration.ofSeconds(60);
  private static final String PROPERTY_REST_API_THREADS = "restApiThreads";
  private static final int DEFAULT_REST_API_THREADS = 10;
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
    REST;
  }

  private final String topic;
  private final boolean sendAsync;
  private final boolean sendStreamEvents;
  private final ClientType clientType;
  private final String restApiUriString;
  private final String restApiUsername;
  private final String restApiPassword;
  private final boolean httpWireLog;
  private final Duration restApiTimeout;
  private final int restApiThreads;

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

    switch (clientType) {
      case REST:
        restApiUriString = fromGerritConfig.getString(PROPERTY_REST_API_URI);
        if (Strings.isNullOrEmpty(restApiUriString)) {
          throw new IllegalArgumentException("Missing REST API URI in Kafka properties");
        }

        restApiUsername = fromGerritConfig.getString(PROPERTY_REST_API_USERNAME);
        restApiPassword = fromGerritConfig.getString(PROPERTY_REST_API_PASSWORD);
        if (!Strings.isNullOrEmpty(restApiUsername) && Strings.isNullOrEmpty(restApiPassword)) {
          throw new IllegalArgumentException("Missing REST API password in kafka properties");
        }

        httpWireLog = fromGerritConfig.getBoolean(PROPERTY_HTTP_WIRE_LOG, DEFAULT_HTTP_WIRE_LOG);
        restApiTimeout =
            Duration.ofMillis(
                ConfigUtil.getTimeUnit(
                    fromGerritConfig.getString(PROPERTY_REST_API_TIMEOUT),
                    DEFAULT_REST_API_TIMEOUT.toMillis(),
                    TimeUnit.MILLISECONDS));
        restApiThreads =
            fromGerritConfig.getInt(PROPERTY_REST_API_THREADS, DEFAULT_REST_API_THREADS);
        break;
      case NATIVE:
      default:
        restApiUriString = null;
        restApiUsername = null;
        restApiPassword = null;
        httpWireLog = false;
        restApiTimeout = null;
        restApiThreads = 0;
        break;
    }

    applyConfig(fromGerritConfig);
    initDockerizedKafkaServer();
  }

  @VisibleForTesting
  public KafkaProperties(
      boolean sendAsync,
      ClientType clientType,
      @Nullable String restApiUriString,
      @Nullable String restApiUsername,
      @Nullable String restApiPassword) {
    super();
    setDefaults();
    topic = DEFAULT_STREAM_EVENTS_TOPIC_NAME;
    this.sendAsync = sendAsync;
    this.sendStreamEvents = true;
    this.clientType = clientType;
    this.restApiUriString = restApiUriString;
    initDockerizedKafkaServer();
    this.httpWireLog = false;
    restApiTimeout = DEFAULT_REST_API_TIMEOUT;
    restApiThreads = DEFAULT_REST_API_THREADS;
    this.restApiUsername = restApiUsername;
    this.restApiPassword = restApiPassword;
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

  public URI getRestApiUri() throws URISyntaxException {
    return getRestApiUri("");
  }

  public String getRestApiUsername() {
    return restApiUsername;
  }

  public String getRestApiPassword() {
    return restApiPassword;
  }

  public URI getRestApiUri(String kafkaRestId) throws URISyntaxException {
    return new URI(restApiUriString.replace(REST_API_URI_ID_PLACEHOLDER, kafkaRestId));
  }

  public boolean isHttpWireLog() {
    return httpWireLog;
  }

  public Duration getRestApiTimeout() {
    return restApiTimeout;
  }

  public int getRestApiThreads() {
    return restApiThreads;
  }
}
