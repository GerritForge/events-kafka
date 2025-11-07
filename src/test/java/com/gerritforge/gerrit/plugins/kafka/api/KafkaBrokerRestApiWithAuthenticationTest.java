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

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaBrokerRestApiWithAuthenticationTest extends KafkaBrokerRestApiTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    restApiUsername = "gerrit";
    restApiPassword = "secret";
    KafkaBrokerRestApiTest.beforeClass();
  }
}
