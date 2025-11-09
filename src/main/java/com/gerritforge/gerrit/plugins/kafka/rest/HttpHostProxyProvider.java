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

import com.google.common.base.Strings;
import com.google.gerrit.server.config.GerritServerConfig;
import com.google.inject.Inject;
import com.google.inject.Provider;
import java.net.MalformedURLException;
import java.net.URL;
import org.eclipse.jgit.lib.Config;

public class HttpHostProxyProvider implements Provider<HttpHostProxy> {
  private URL proxyUrl;
  private String proxyUser;
  private String proxyPassword;

  @Inject
  HttpHostProxyProvider(@GerritServerConfig Config config) throws MalformedURLException {
    String proxyUrlStr = config.getString("http", null, "proxy");
    if (!Strings.isNullOrEmpty(proxyUrlStr)) {
      proxyUrl = new URL(proxyUrlStr);
      proxyUser = config.getString("http", null, "proxyUsername");
      proxyPassword = config.getString("http", null, "proxyPassword");
      String userInfo = proxyUrl.getUserInfo();
      if (userInfo != null) {
        int c = userInfo.indexOf(':');
        if (0 < c) {
          proxyUser = userInfo.substring(0, c);
          proxyPassword = userInfo.substring(c + 1);
        } else {
          proxyUser = userInfo;
        }
      }
    }
  }

  @Override
  public HttpHostProxy get() {
    return new HttpHostProxy(proxyUrl, proxyUser, proxyPassword);
  }
}
