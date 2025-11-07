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

import java.util.Map;
import org.junit.Ignore;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

@Ignore
public class KafkaContainerProvider {
  public static int KAFKA_PORT_INTERNAL = KafkaContainer.KAFKA_PORT + 1;
  private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka";
  private static final String KAFKA_IMAGE_TAG = "5.4.3";

  public static KafkaContainer get() {
    KafkaContainer kafkaContainer =
        new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME).withTag(KAFKA_IMAGE_TAG)) {

          @Override
          public String getBootstrapServers() {
            return String.format(
                    "INTERNAL://%s:%s,", getNetworkAliases().get(0), KAFKA_PORT_INTERNAL)
                + super.getBootstrapServers();
          }
        };

    Map<String, String> kafkaEnv = kafkaContainer.getEnvMap();
    String kafkaListeners = kafkaEnv.get("KAFKA_LISTENERS");
    String kafkaProtocolMap = kafkaEnv.get("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP");

    return kafkaContainer
        .withNetwork(Network.newNetwork())
        .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", kafkaProtocolMap + ",INTERNAL:PLAINTEXT")
        .withEnv("KAFKA_LISTENERS", kafkaListeners + ",INTERNAL://0.0.0.0:" + KAFKA_PORT_INTERNAL);
  }
}
