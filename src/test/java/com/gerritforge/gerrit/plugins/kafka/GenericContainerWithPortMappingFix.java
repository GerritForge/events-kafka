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

package com.gerritforge.gerrit.plugins.kafka;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import java.util.Map;
import java.util.concurrent.Future;
import org.junit.Ignore;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@Ignore
public class GenericContainerWithPortMappingFix<T extends GenericContainer<T>>
    extends GenericContainer<T> {
  private static final int WAIT_FOR_PORT_BINDING_SEC = 10;
  private static final FluentLogger log = FluentLogger.forEnclosingClass();

  public GenericContainerWithPortMappingFix(DockerImageName dockerImageName) {
    super(dockerImageName);
  }

  public GenericContainerWithPortMappingFix(Future<String> image) {
    super(image);
  }

  @Override
  public Integer getMappedPort(int originalPort) {
    return getMappedPort(dockerClient, this, originalPort);
  }

  public static Integer getMappedPort(
      DockerClient dockerClient, GenericContainer<?> genericContainer, int originalPort) {
    int waitPatienceSec = WAIT_FOR_PORT_BINDING_SEC;
    Integer mappedPort = null;

    while (mappedPort == null && waitPatienceSec > 0) {
      mappedPort = getMappedPortFromContainer(dockerClient, genericContainer, originalPort);
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
              originalPort, genericContainer.getContainerId(), WAIT_FOR_PORT_BINDING_SEC));
    }
    return mappedPort;
  }

  private static Integer getMappedPortFromContainer(
      DockerClient dockerClient, GenericContainer<?> genericContainer, int originalPort) {
    String containerId = genericContainer.getContainerId();
    Preconditions.checkState(
        genericContainer.getContainerId() != null,
        "Mapped port can only be obtained after the container is started");

    Ports.Binding[] binding = getBindings(dockerClient, genericContainer, originalPort);
    if (binding != null && binding.length > 0 && binding[0] != null) {
      return Integer.valueOf(binding[0].getHostPortSpec());
    } else {
      log.atWarning().log(
          "Unable to get mapped port %d from container %s", originalPort, containerId);
      return null;
    }
  }

  private static Ports.Binding[] getBindings(
      DockerClient dockerClient, GenericContainer<?> genericContainer, int originalPort) {
    String containerId = genericContainer.getContainerId();
    Map<ExposedPort, Ports.Binding[]> bindings = getBindings(genericContainer.getContainerInfo());
    if (bindings == null || bindings.isEmpty()) {
      bindings = getBindings(dockerClient.inspectContainerCmd(containerId).exec());
    }

    return bindings.get(new ExposedPort(originalPort));
  }

  private static Map<ExposedPort, Ports.Binding[]> getBindings(
      InspectContainerResponse containerInfo) {
    return containerInfo.getNetworkSettings().getPorts().getBindings();
  }
}
