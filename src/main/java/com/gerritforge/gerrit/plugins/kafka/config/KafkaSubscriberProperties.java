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
import com.google.gerrit.extensions.annotations.PluginName;
import com.google.gerrit.server.config.PluginConfigFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerConfig;

@Singleton
public class KafkaSubscriberProperties extends KafkaProperties {
  private static final long serialVersionUID = 1L;
  public static final String DEFAULT_POLLING_INTERVAL_MS = "1000";
  public static final String DEFAULT_NUMBER_OF_SUBSCRIBERS = "7";
  public static final String DEFAULT_COMMIT_INTERVAL_MS = "5000";
  private static final String DEFAULT_ENABLE_AUTO_COMMIT = "true";

  private final Integer pollingInterval;
  private final String groupId;
  private final Integer numberOfSubscribers;
  private final long commitIntervalMs;
  private final boolean autoCommitEnabled;

  @Inject
  public KafkaSubscriberProperties(
      PluginConfigFactory configFactory, @PluginName String pluginName) {
    super(configFactory, pluginName);

    this.pollingInterval =
        Integer.parseInt(getProperty("polling.interval.ms", DEFAULT_POLLING_INTERVAL_MS));
    this.groupId = getProperty("group.id");
    this.numberOfSubscribers =
        Integer.parseInt(getProperty("number.of.subscribers", DEFAULT_NUMBER_OF_SUBSCRIBERS));
    this.commitIntervalMs =
        Long.parseLong(
            getProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, DEFAULT_COMMIT_INTERVAL_MS));
    this.autoCommitEnabled =
        Boolean.parseBoolean(
            getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_ENABLE_AUTO_COMMIT));
  }

  @VisibleForTesting
  public KafkaSubscriberProperties(
      int pollingInterval, String groupId, int numberOfSubscribers, ClientType clientType) {
    this(
        pollingInterval,
        groupId,
        numberOfSubscribers,
        clientType,
        Long.parseLong(DEFAULT_COMMIT_INTERVAL_MS),
        true,
        null,
        null,
        null);
  }

  @VisibleForTesting
  public KafkaSubscriberProperties(
      int pollingInterval,
      String groupId,
      int numberOfSubscribers,
      ClientType clientType,
      long commitIntervalMs,
      boolean autoCommitEnabled,
      String restApiUriString,
      String restApiUsername,
      String restApiPassword) {
    super(true, clientType, restApiUriString, restApiUsername, restApiPassword);
    this.pollingInterval = pollingInterval;
    this.groupId = groupId;
    this.numberOfSubscribers = numberOfSubscribers;
    this.commitIntervalMs = commitIntervalMs;
    this.autoCommitEnabled = autoCommitEnabled;
    if (!autoCommitEnabled) {
      setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }
  }

  public Integer getPollingInterval() {
    return pollingInterval;
  }

  public String getGroupId() {
    return groupId;
  }

  public Integer getNumberOfSubscribers() {
    return numberOfSubscribers;
  }

  public long getCommitIntervalMs() {
    return commitIntervalMs;
  }

  public boolean isAutoCommitEnabled() {
    return autoCommitEnabled;
  }
}
