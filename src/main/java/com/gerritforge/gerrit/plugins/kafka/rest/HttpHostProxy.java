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

package com.gerritforge.gerrit.plugins.kafka.rest;

import com.google.gerrit.common.Nullable;
import java.net.URL;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

public class HttpHostProxy {
  private final URL proxyUrl;
  private final String username;
  private final String password;

  public HttpHostProxy(URL proxyUrl, @Nullable String username, @Nullable String password) {
    this.proxyUrl = proxyUrl;
    this.username = username;
    this.password = password;
  }

  public Builder apply(Builder clientBuilder) {
    if (proxyUrl != null) {
      clientBuilder.setProxy(
          new HttpHost(proxyUrl.getHost(), proxyUrl.getPort(), proxyUrl.getProtocol()));
    }
    return clientBuilder;
  }

  public HttpAsyncClientBuilder apply(HttpAsyncClientBuilder custom) {
    if (proxyUrl != null && username != null && password != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(
          new AuthScope(proxyUrl.getHost(), proxyUrl.getPort()),
          new UsernamePasswordCredentials(username, password));
      custom.setDefaultCredentialsProvider(credsProvider);
    }
    return custom;
  }
}
