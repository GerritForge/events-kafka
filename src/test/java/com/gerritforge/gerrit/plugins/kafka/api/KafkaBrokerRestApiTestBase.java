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
package com.gerritforge.gerrit.plugins.kafka.api;

import com.gerritforge.gerrit.plugins.kafka.KafkaRestContainer;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

@Ignore
public class KafkaBrokerRestApiTestBase extends KafkaBrokerApiTest {
  private static final String NGINX_IMAGE = "nginx:1.21.5";

  @SuppressWarnings("resource")
  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaBrokerApiTest.beforeClass();
    String nginxKafkaConf =
        String.format(
            "server {\\n"
                + "  listen       80  default_server;\\n"
                + "  listen  [::]:80  default_server;\\n"
                + "  location     /%s/ {\\n"
                + "    proxy_pass http://%s:%d/; \\n"
                + "	   proxy_set_header Authorization \"Basic Z2Vycml0OnNlY3JldA==\";\\n"
                + "  }\\n"
                + "  location     / {\\n"
                + "    proxy_pass http://%s:%d; \\n"
                + "	   proxy_set_header Authorization \"Basic Z2Vycml0OnNlY3JldA==\";\\n"
                + "  }\\n"
                + "}",
            KAFKA_REST_ID,
            kafkaRestWithId.getKafkaRestContainerIP(),
            KafkaRestContainer.KAFKA_REST_PORT,
            kafkaRestWithId.getKafkaRestContainerIP(),
            KafkaRestContainer.KAFKA_REST_PORT);
    nginx =
        new GenericContainer<>(
                new ImageFromDockerfile()
                    .withDockerfileFromBuilder(
                        builder ->
                            builder
                                .from(NGINX_IMAGE)
                                .run(
                                    "sh",
                                    "-c",
                                    String.format(
                                        "echo '%s' | tee /etc/nginx/conf.d/default.conf",
                                        nginxKafkaConf))
                                .build()))
            .withExposedPorts(80)
            .withNetwork(kafkaRestWithId.getNetwork())
            .waitingFor(new HttpWaitStrategy());
    nginx.start();
  }
}
