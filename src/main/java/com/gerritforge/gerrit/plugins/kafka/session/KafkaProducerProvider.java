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

package com.gerritforge.gerrit.plugins.kafka.session;

import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class KafkaProducerProvider implements Provider<Producer<String, String>> {
  private final KafkaProperties properties;

  @Inject
  public KafkaProducerProvider(KafkaProperties properties) {
    this.properties = properties;
  }

  @Override
  public Producer<String, String> get() {
    return new KafkaProducer<>(properties);
  }
}
