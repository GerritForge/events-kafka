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

package com.gerritforge.gerrit.plugins.kafka.subscribe;

import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

@Singleton
class KafkaConsumerFactory {
  private KafkaSubscriberProperties config;

  @Inject
  public KafkaConsumerFactory(KafkaSubscriberProperties configuration) {
    this.config = configuration;
  }

  public Consumer<byte[], byte[]> create(Deserializer<byte[]> keyDeserializer) {
    return create(config, keyDeserializer);
  }

  public Consumer<byte[], byte[]> create(
      KafkaSubscriberProperties config, Deserializer<byte[]> keyDeserializer) {
    return new KafkaConsumer<>(config, keyDeserializer, new ByteArrayDeserializer());
  }
}
