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

package com.gerritforge.gerrit.plugins.kafka.rest;

import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import java.net.URISyntaxException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;

/** Looks up a remote's password in secure.config. */
public class HttpAsyncClientBuilderFactory {
  private final KafkaProperties config;

  @Inject
  public HttpAsyncClientBuilderFactory(KafkaProperties config) {
    this.config = config;
  }

  public HttpAsyncClientBuilder create() throws URISyntaxException {
    String user = config.getRestApiUsername();
    String pass = config.getRestApiPassword();
    HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom();
    if (!Strings.isNullOrEmpty(user) && !Strings.isNullOrEmpty(pass)) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(
          new AuthScope(config.getRestApiUri().getHost(), config.getRestApiUri().getPort()),
          new UsernamePasswordCredentials(user, pass));
      httpAsyncClientBuilder.setDefaultCredentialsProvider(credsProvider);
    }
    return httpAsyncClientBuilder;
  }
}
