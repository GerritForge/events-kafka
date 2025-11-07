// Copyright (C) 2021 The Android Open Source Project
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
package com.gerritforge.gerrit.plugins.kafka;

import static com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.SEND_ASYNC_DEFAULT;
import static com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.SEND_ASYNC_FIELD;
import static com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.SEND_STREAM_EVENTS_DEFAULT;
import static com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.SEND_STREAM_EVENTS_FIELD;
import static com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.STREAM_EVENTS_TOPIC_DEFAULT;
import static com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties.STREAM_EVENTS_TOPIC_FIELD;
import static com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties.DEFAULT_NUMBER_OF_SUBSCRIBERS;
import static com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties.DEFAULT_POLLING_INTERVAL_MS;

import com.google.common.base.Strings;
import com.google.gerrit.extensions.annotations.PluginName;
import com.google.gerrit.pgm.init.api.ConsoleUI;
import com.google.gerrit.pgm.init.api.InitStep;
import com.google.gerrit.pgm.init.api.Section;
import com.google.gerrit.server.config.GerritInstanceIdProvider;
import com.google.inject.Inject;

public class InitConfig implements InitStep {
  private static final String GROUP_ID_FIELD = "groupId";
  private static final String POLLING_INTERVAL_FIELD = "pollingIntervalMs";
  private static final String NUMBER_OF_SUBSCRIBERS_FIELD = "numberOfSubscribers";

  private final Section pluginSection;
  private final String pluginName;
  private final ConsoleUI ui;
  private final GerritInstanceIdProvider gerritInstanceIdProvider;

  @Inject
  InitConfig(
      Section.Factory sections,
      @PluginName String pluginName,
      GerritInstanceIdProvider gerritInstanceIdProvider,
      ConsoleUI ui) {
    this.pluginName = pluginName;
    this.ui = ui;
    this.gerritInstanceIdProvider = gerritInstanceIdProvider;
    this.pluginSection = sections.get("plugin", pluginName);
  }

  @Override
  public void run() throws Exception {
    ui.header(String.format("%s plugin", pluginName));

    boolean sendStreamEvents = ui.yesno(SEND_STREAM_EVENTS_DEFAULT, "Should send stream events?");
    pluginSection.set(SEND_STREAM_EVENTS_FIELD, Boolean.toString(sendStreamEvents));

    if (sendStreamEvents) {
      pluginSection.string(
          "Stream events topic", STREAM_EVENTS_TOPIC_FIELD, STREAM_EVENTS_TOPIC_DEFAULT);
    }

    boolean sendAsync = ui.yesno(SEND_ASYNC_DEFAULT, "Should send messages asynchronously?");
    pluginSection.set(SEND_ASYNC_FIELD, Boolean.toString(sendAsync));

    pluginSection.string(
        "Polling interval (ms)", POLLING_INTERVAL_FIELD, DEFAULT_POLLING_INTERVAL_MS);

    pluginSection.string(
        "Number of subscribers", NUMBER_OF_SUBSCRIBERS_FIELD, DEFAULT_NUMBER_OF_SUBSCRIBERS);

    String consumerGroup =
        pluginSection.string("Consumer group", GROUP_ID_FIELD, gerritInstanceIdProvider.get());
    while (Strings.isNullOrEmpty(consumerGroup) && !ui.isBatch()) {
      ui.message("'%s' is mandatory. Please specify a value.", GROUP_ID_FIELD);
      consumerGroup =
          pluginSection.string("Consumer group", GROUP_ID_FIELD, gerritInstanceIdProvider.get());
    }

    if (Strings.isNullOrEmpty(consumerGroup) && ui.isBatch()) {
      System.err.printf(
          "FATAL [%s plugin]: Could not set '%s' in batch mode. %s will not work%n",
          pluginName, GROUP_ID_FIELD, pluginName);
    }
  }
}
