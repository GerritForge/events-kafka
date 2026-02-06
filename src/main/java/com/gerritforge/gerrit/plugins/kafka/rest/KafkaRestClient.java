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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.google.common.base.Function;
import com.google.common.flogger.FluentLogger;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gerrit.common.Nullable;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KafkaRestClient {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final String KAFKA_V2_JSON = "application/vnd.kafka.json.v2+json";
  private static final String KAFKA_V2 = "application/vnd.kafka.v2+json";

  private final HttpHostProxy proxy;
  private final CloseableHttpAsyncClient httpclient;
  private final ExecutorService futureExecutor;
  private final int kafkaRestApiTimeoutMsec;
  private final KafkaProperties configuration;

  private static boolean logConfigured;

  public interface Factory {
    KafkaRestClient create(KafkaProperties configuration);
  }

  @Inject
  public KafkaRestClient(
      HttpHostProxy httpHostProxy,
      @FutureExecutor ExecutorService executor,
      HttpAsyncClientBuilderFactory credentialsFactory,
      @Assisted KafkaProperties configuration)
      throws URISyntaxException {
    proxy = httpHostProxy;
    httpclient = proxy.apply(credentialsFactory.create()).build();
    httpclient.start();
    this.configuration = configuration;
    kafkaRestApiTimeoutMsec = (int) configuration.getRestApiTimeout().toMillis();
    if (configuration.isHttpWireLog()) {
      enableHttpWireLog();
    }
    this.futureExecutor = executor;
  }

  public static void enableHttpWireLog() {
    if (!logConfigured) {
      Logger httpWireLoggger = Logger.getLogger("org.apache.http.wire");
      httpWireLoggger.setLevel(Level.DEBUG);

      @SuppressWarnings("rawtypes")
      Enumeration rootLoggerAppenders = LogManager.getRootLogger().getAllAppenders();
      while (rootLoggerAppenders.hasMoreElements()) {
        Appender logAppender = (Appender) rootLoggerAppenders.nextElement();
        if (logAppender instanceof AppenderSkeleton) {
          ((AppenderSkeleton) logAppender).setThreshold(Level.DEBUG);
        }
        httpWireLoggger.addAppender(logAppender);
      }

      logConfigured = true;
    }
  }

  public ListenableFuture<HttpResponse> execute(HttpRequestBase request, int... expectedStatuses) {
    return Futures.transformAsync(
        listenableFutureOf(httpclient.execute(request, null)),
        (res) -> {
          IOException exc =
              getResponseException(
                  String.format("HTTP %s %s FAILED", request.getMethod(), request.getURI()),
                  res,
                  expectedStatuses);
          if (exc == null) {
            return Futures.immediateFuture(res);
          }
          return Futures.immediateFailedFuture(exc);
        },
        futureExecutor);
  }

  public <I, O> ListenableFuture<O> mapAsync(
      ListenableFuture<I> inputFuture, AsyncFunction<? super I, ? extends O> mapFunction) {
    return Futures.transformAsync(inputFuture, mapFunction, futureExecutor);
  }

  public <I, O> ListenableFuture<O> map(
      ListenableFuture<I> inputFuture, Function<? super I, ? extends O> mapFunction) {
    return Futures.transform(inputFuture, mapFunction, futureExecutor);
  }

  public HttpGet createGetTopic(String topic) {
    HttpGet get = new HttpGet(resolveKafkaRestApiUri("/topics/" + topic));
    get.addHeader(HttpHeaders.ACCEPT, KAFKA_V2);
    get.setConfig(createRequestConfig());
    return get;
  }

  public HttpGet createGetRecords(URI consumerUri) {
    HttpGet get = new HttpGet(consumerUri.resolve(consumerUri.getPath() + "/records"));
    get.addHeader(HttpHeaders.ACCEPT, KAFKA_V2_JSON);
    get.setConfig(createRequestConfig());
    return get;
  }

  public HttpPost createPostToConsumer(String consumerGroup) {
    return createPostToConsumer(consumerGroup, true);
  }

  public HttpPost createPostToConsumer(String consumerGroup, boolean autoCommitEnabled) {
    HttpPost post =
        new HttpPost(
            resolveKafkaRestApiUri("/consumers/" + URLEncoder.encode(consumerGroup, UTF_8)));
    post.addHeader(HttpHeaders.ACCEPT, MediaType.ANY_TYPE.toString());
    post.setConfig(createRequestConfig());
    post.setEntity(
        new StringEntity(
            String.format(
                "{\"format\": \"json\",\"auto.offset.reset\": \"earliest\","
                    + " \"auto.commit.enable\":\"%s\", \"consumer.request.timeout.ms\": \"1000\"}",
                autoCommitEnabled),
            ContentType.create(KAFKA_V2, UTF_8)));
    return post;
  }

  public HttpDelete createDeleteToConsumer(URI consumerUri) {
    HttpDelete delete = new HttpDelete(consumerUri);
    delete.addHeader(HttpHeaders.ACCEPT, "*/*");
    delete.setConfig(createRequestConfig());
    return delete;
  }

  public HttpDelete createDeleteToConsumerSubscriptions(URI consumerUri) {
    URI subscriptionUri = consumerUri.resolve("subscription");
    HttpDelete delete = new HttpDelete(subscriptionUri);
    delete.addHeader(HttpHeaders.ACCEPT, "*/*");
    delete.setConfig(createRequestConfig());
    return delete;
  }

  public HttpPost createPostToSubscribe(URI consumerUri, String topic) {
    HttpPost post = new HttpPost(consumerUri.resolve(consumerUri.getPath() + "/subscription"));
    post.addHeader(HttpHeaders.ACCEPT, "*/*");
    post.setConfig(createRequestConfig());
    post.setEntity(
        new StringEntity(
            String.format("{\"topics\":[\"%s\"]}", topic), ContentType.create(KAFKA_V2, UTF_8)));
    return post;
  }

  public HttpPost createPostToTopic(String topic, HttpEntity postBodyEntity) {
    HttpPost post =
        new HttpPost(resolveKafkaRestApiUri("/topics/" + URLEncoder.encode(topic, UTF_8)));
    post.addHeader(HttpHeaders.ACCEPT, "*/*");
    post.setConfig(createRequestConfig());
    post.setEntity(postBodyEntity);
    return post;
  }

  public HttpPost createPostSeekTopicFromBeginning(
      URI consumerUri, String topic, Set<Integer> partitions) {
    HttpPost post =
        new HttpPost(consumerUri.resolve(consumerUri.getPath() + "/positions/beginning"));
    post.addHeader(HttpHeaders.ACCEPT, "*/*");
    post.setConfig(createRequestConfig());
    post.setEntity(
        new StringEntity(
            String.format(
                "{\"partitions\":[%s]}",
                partitions.stream()
                    .map(
                        partition ->
                            String.format("{\"topic\":\"%s\",\"partition\":%d}", topic, partition))
                    .collect(Collectors.joining(","))),
            ContentType.create(KAFKA_V2, UTF_8)));
    return post;
  }

  public HttpPost createPostToCommitOffsets(
      URI consumerUri, String topic, int partition, long offset) {
    HttpPost post = new HttpPost(consumerUri.resolve(consumerUri.getPath() + "/offsets"));
    post.addHeader(HttpHeaders.ACCEPT, "*/*");
    post.setConfig(createRequestConfig());
    post.setEntity(
        new StringEntity(
            String.format(
                "{\"offsets\":[{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d}]}",
                topic, partition, offset),
            ContentType.create(KAFKA_V2, UTF_8)));
    return post;
  }

  @Nullable
  public IOException getResponseException(
      String errorMessage, HttpResponse response, int... okHttpStatuses) {
    int responseHttpStatus = response.getStatusLine().getStatusCode();
    if (okHttpStatuses.length == 0) {
      okHttpStatuses =
          new int[] {HttpStatus.SC_OK, HttpStatus.SC_CREATED, HttpStatus.SC_NO_CONTENT};
    }
    for (int httpStatus : okHttpStatuses) {
      if (responseHttpStatus == httpStatus) {
        return null;
      }
    }

    String responseBody = "";
    try {
      responseBody = getStringEntity(response);
    } catch (IOException e) {
      logger.atWarning().withCause(e).log(
          "Unable to extrace the string entity for response %d (%s)",
          response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
    }

    return new IOException(
        String.format(
            "%s\nHTTP status %d (%s)\n%s",
            errorMessage,
            response.getStatusLine().getStatusCode(),
            response.getStatusLine().getReasonPhrase(),
            responseBody));
  }

  protected String getStringEntity(HttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
        entity.writeTo(outStream);
        outStream.close();
        return outStream.toString(UTF_8);
      }
    }
    return "";
  }

  private <V> ListenableFuture<V> listenableFutureOf(Future<V> future) {
    return JdkFutureAdapters.listenInPoolThread(future, futureExecutor);
  }

  private RequestConfig createRequestConfig() {
    Builder configBuilder =
        RequestConfig.custom()
            .setConnectionRequestTimeout(kafkaRestApiTimeoutMsec)
            .setConnectTimeout(kafkaRestApiTimeoutMsec)
            .setSocketTimeout(kafkaRestApiTimeoutMsec);
    configBuilder = proxy.apply(configBuilder);
    RequestConfig config = configBuilder.build();
    return config;
  }

  public void close() throws IOException {
    httpclient.close();
  }

  public URI resolveKafkaRestApiUri(String path) {
    try {
      URI restApiUri = configuration.getRestApiUri();
      return restApiUri.resolve(path);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid Kafka REST API URI", e);
    }
  }

  public URI resolveKafkaRestApiUri(String kafkaRestId, String path) {
    URI restApiUri;
    try {
      restApiUri = configuration.getRestApiUri(kafkaRestId);
      return restApiUri.resolve(restApiUri.getPath() + path);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid Kafka REST API URI", e);
    }
  }
}
