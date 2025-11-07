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

import com.github.dockerjava.api.model.ContainerNetwork;
import com.google.common.base.Strings;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.junit.Ignore;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Ignore
public class KafkaRestContainer extends GenericContainer<KafkaRestContainer> {

  public static final String KAFKA_REST_PROXY_HOSTNAME = "restproxy";
  public static final int KAFKA_REST_PORT = 8082;
  private final String kafkaRestHostname;

  public KafkaRestContainer(KafkaContainer kafkaContainer, Boolean enableAuthentication) {
    this(kafkaContainer, null, enableAuthentication);
  }

  public KafkaRestContainer(
      KafkaContainer kafkaContainer, String kafkaRestId, Boolean enableAuthentication) {
    super(restProxyImageFor(kafkaContainer));

    kafkaRestHostname = KAFKA_REST_PROXY_HOSTNAME + Strings.nullToEmpty(kafkaRestId);

    withNetwork(kafkaContainer.getNetwork());

    withExposedPorts(KAFKA_REST_PORT);
    String bootstrapServers =
        String.format(
            "PLAINTEXT://%s:%s",
            kafkaContainer.getNetworkAliases().get(0), KafkaContainerProvider.KAFKA_PORT_INTERNAL);
    withEnv("KAFKA_REST_BOOTSTRAP_SERVERS", bootstrapServers);
    withEnv("KAFKA_REST_LISTENERS", "http://0.0.0.0:" + KAFKA_REST_PORT);
    withEnv("KAFKA_REST_CLIENT_SECURITY_PROTOCOL", "PLAINTEXT");
    withEnv("KAFKA_REST_HOST_NAME", kafkaRestHostname);
    if (kafkaRestId != null) {
      withEnv("KAFKA_REST_ID", kafkaRestId);
    }
    if (enableAuthentication) {
      withEnv("KAFKA_REST_AUTHENTICATION_METHOD", "BASIC");
      withEnv("KAFKA_REST_AUTHENTICATION_REALM", "KafkaRest");
      withEnv("KAFKA_REST_AUTHENTICATION_ROLES", "GerritRole");
      withEnv(
          "KAFKAREST_OPTS",
          "-Djava.security.auth.login.config=/etc/kafka-rest/rest-jaas.properties");
      withClasspathResourceMapping(
          "rest-jaas.properties", "/etc/kafka-rest/rest-jaas.properties", BindMode.READ_ONLY);
      withClasspathResourceMapping(
          "password.properties", "/etc/kafka-rest/password.properties", BindMode.READ_ONLY);
    }
    withCreateContainerCmdModifier(cmd -> cmd.withHostName(kafkaRestHostname));
  }

  private static DockerImageName restProxyImageFor(KafkaContainer kafkaContainer) {
    String[] kafkaImageNameParts = kafkaContainer.getDockerImageName().split(":");
    return DockerImageName.parse(kafkaImageNameParts[0] + "-rest").withTag(kafkaImageNameParts[1]);
  }

  public URI getApiURI() {
    try {
      return new URI(
          String.format("http://%s:%d", getContainerIpAddress(), getMappedPort(KAFKA_REST_PORT)));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid Kafka API URI", e);
    }
  }

  public String getKafkaRestContainerIP() {
    Map<String, ContainerNetwork> networks = getContainerInfo().getNetworkSettings().getNetworks();
    return networks.values().iterator().next().getIpAddress();
  }
}
