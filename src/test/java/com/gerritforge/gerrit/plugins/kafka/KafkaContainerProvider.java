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

package com.gerritforge.gerrit.plugins.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
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
  private static final int WAIT_FOR_PORT_BINDING_SEC = 10;
  private static final FluentLogger log = FluentLogger.forEnclosingClass();

  public static KafkaContainer get() {
    KafkaContainer kafkaContainer =
        new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME).withTag(KAFKA_IMAGE_TAG)) {

          @Override
          public String getBootstrapServers() {
            return String.format(
                    "INTERNAL://%s:%s,", getNetworkAliases().get(0), KAFKA_PORT_INTERNAL)
                + super.getBootstrapServers();
          }

          @Override
          public Integer getMappedPort(int originalPort) {
            int waitPatienceSec = WAIT_FOR_PORT_BINDING_SEC;
            Integer mappedPort = null;

            while (mappedPort == null && waitPatienceSec > 0) {
              mappedPort = getMappedPortFromContainer(originalPort);
              if (mappedPort == null) {
                try {
                  Thread.sleep(1000L);
                } catch (InterruptedException ex) {
                  throw new RuntimeException(ex);
                }
              }
              waitPatienceSec--;
            }
            if (mappedPort == null) {
              throw new IllegalArgumentException(
                  String.format(
                      "Unable to get mapped port %d from container %s after %d seconds",
                      originalPort, getContainerId(), WAIT_FOR_PORT_BINDING_SEC));
            }
            return mappedPort;
          }

          private Integer getMappedPortFromContainer(int originalPort) {
            String containerId = this.getContainerId();
            Preconditions.checkState(
                this.getContainerId() != null,
                "Mapped port can only be obtained after the container is started");

            Ports.Binding[] binding = getBindings(containerId, originalPort);
            if (binding != null && binding.length > 0 && binding[0] != null) {
              return Integer.valueOf(binding[0].getHostPortSpec());
            } else {
              log.atWarning().log(
                  "Unable to get Kafka mapped port %d from container %s",
                  originalPort, containerId);
              return null;
            }
          }

          private Ports.Binding[] getBindings(String containerId, int originalPort) {
            Map<ExposedPort, Ports.Binding[]> bindings = getBindings(getContainerInfo());
            if (bindings == null || bindings.isEmpty()) {
              bindings = getBindings(dockerClient.inspectContainerCmd(containerId).exec());
            }

            return bindings.get(new ExposedPort(originalPort));
          }

          private static Map<ExposedPort, Ports.Binding[]> getBindings(
              InspectContainerResponse containerInfo) {
            return containerInfo.getNetworkSettings().getPorts().getBindings();
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
