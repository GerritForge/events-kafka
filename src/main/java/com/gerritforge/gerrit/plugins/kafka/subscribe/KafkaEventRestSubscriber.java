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
package com.gerritforge.gerrit.plugins.kafka.subscribe;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.gerritforge.gerrit.eventbroker.ContextAwareConsumer;
import com.gerritforge.gerrit.eventbroker.MessageContext;
import com.gerritforge.gerrit.plugins.kafka.broker.ConsumerExecutor;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaSubscriberProperties;
import com.gerritforge.gerrit.plugins.kafka.rest.KafkaRestClient;
import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.util.ManualRequestContext;
import com.google.gerrit.server.util.OneOffRequestContext;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaEventRestSubscriber implements KafkaEventSubscriber {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final int DELAY_RECONNECT_AFTER_FAILURE_MSEC = 1000;
  // Prefix is a length of 'rest-consumer-' string
  private static final int INSTANCE_ID_PREFIX_LEN = 14;

  /**
   * Suffix is a length of a unique identifier for example: '-9836fe85-d838-4722-97c9-4a7b-34e834d'
   */
  private static final int INSTANCE_ID_SUFFIX_LEN = 37;

  private final OneOffRequestContext oneOffCtx;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final Deserializer<Event> valueDeserializer;
  private final KafkaSubscriberProperties configuration;
  private final ExecutorService executor;
  private final KafkaEventSubscriberMetrics subscriberMetrics;
  private final Gson gson;

  private ContextAwareConsumer<Event> contextAwareMessageProcessor;
  private String topic;
  private final KafkaRestClient restClient;
  private final AtomicBoolean resetOffset;
  private final long restClientTimeoutMs;
  private volatile ReceiverJob receiver;
  private final Optional<String> externalGroupId;

  @Inject
  public KafkaEventRestSubscriber(
      KafkaSubscriberProperties configuration,
      Deserializer<Event> valueDeserializer,
      OneOffRequestContext oneOffCtx,
      @ConsumerExecutor ExecutorService executor,
      KafkaEventSubscriberMetrics subscriberMetrics,
      KafkaRestClient.Factory restClientFactory,
      @Assisted Optional<String> externalGroupId) {

    this.oneOffCtx = oneOffCtx;
    this.executor = executor;
    this.subscriberMetrics = subscriberMetrics;
    this.valueDeserializer = valueDeserializer;
    this.externalGroupId = externalGroupId;
    this.configuration = (KafkaSubscriberProperties) configuration.clone();
    externalGroupId.ifPresent(gid -> this.configuration.setProperty("group.id", gid));

    gson = new Gson();
    restClient = restClientFactory.create(configuration);
    resetOffset = new AtomicBoolean(false);
    restClientTimeoutMs = configuration.getRestApiTimeout().toMillis();
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#subscribe(ContextAwareConsumer)
   */
  @Override
  public void subscribe(String topic, ContextAwareConsumer<Event> contextAwareConsumer) {
    this.topic = topic;
    this.contextAwareMessageProcessor = contextAwareConsumer;
    logger.atInfo().log(
        "Kafka consumer subscribing to topic alias [%s] for event topic [%s] with groupId [%s]",
        topic, topic, configuration.getGroupId());
    try {
      runReceiver();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IllegalStateException(e);
    }
  }

  private void runReceiver() throws InterruptedException, ExecutionException, TimeoutException {
    receiver = new ReceiverJob(configuration.getGroupId());
    executor.execute(receiver);
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#shutdown()
   */
  @Override
  public void shutdown() {
    try {
      closed.set(true);
      receiver.close();
    } catch (InterruptedException | ExecutionException | IOException | TimeoutException e) {
      logger.atWarning().withCause(e).log("Unable to close receiver for topic=%s", topic);
    }
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#getMessageProcessor()
   */
  @Override
  public java.util.function.Consumer<Event> getMessageProcessor() {
    return event -> contextAwareMessageProcessor.accept(event, MessageContext.noop());
  }

  @Override
  public ContextAwareConsumer<Event> contextAwareMessageProcessor() {
    return contextAwareMessageProcessor;
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#getTopic()
   */
  @Override
  public String getTopic() {
    return topic;
  }

  /* (non-Javadoc)
   * @see com.gerritforge.gerrit.plugins.kafka.subscribe.KafkaEventSubscriber#resetOffset()
   */
  @Override
  public void resetOffset() {
    resetOffset.set(true);
  }

  @Override
  public Optional<String> getExternalGroupId() {
    return externalGroupId;
  }

  private class ReceiverJob implements Runnable {
    private final ListenableFuture<URI> kafkaRestConsumerUri;
    private final ListenableFuture<?> kafkaSubscriber;

    public ReceiverJob(String consumerGroup)
        throws InterruptedException, ExecutionException, TimeoutException {
      kafkaRestConsumerUri = createConsumer(consumerGroup);
      kafkaSubscriber = restClient.mapAsync(kafkaRestConsumerUri, this::subscribeToTopic);
      kafkaSubscriber.get(restClientTimeoutMs, TimeUnit.MILLISECONDS);
    }

    public void close()
        throws InterruptedException, ExecutionException, IOException, TimeoutException {
      restClient
          .mapAsync(kafkaRestConsumerUri, this::deleteConsumer)
          .get(restClientTimeoutMs, TimeUnit.MILLISECONDS);
      restClient.close();
    }

    @Override
    public void run() {
      try {
        consume();
      } catch (Exception e) {
        logger.atSevere().withCause(e).log("Consumer loop of topic %s ended", topic);
      }
    }

    private void consume() throws InterruptedException, ExecutionException, TimeoutException {
      try {
        while (!closed.get()) {
          if (resetOffset.getAndSet(false)) {
            restClient
                .mapAsync(getTopicPartitions(), this::seekToBeginning)
                .get(restClientTimeoutMs, TimeUnit.MILLISECONDS);
          }

          ConsumerRecords<byte[], byte[]> records =
              restClient
                  .mapAsync(kafkaRestConsumerUri, this::getRecords)
                  .get(restClientTimeoutMs, TimeUnit.MILLISECONDS);
          records.forEach(
              consumerRecord -> {
                try (ManualRequestContext ctx = oneOffCtx.open()) {
                  Event event =
                      valueDeserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
                  // Note: we are not calling context aware processor here because REST Client
                  // always has autocommit enabled
                  contextAwareMessageProcessor.accept(event, MessageContext.noop());
                } catch (KafkaException e) {
                  logger.atSevere().withCause(e).log(
                      "Kafka exception when consuming event '%s': [Exception: %s]",
                      new String(consumerRecord.value(), UTF_8), e.toString());
                  subscriberMetrics.incrementSubscriberFailedToConsumeMessage();
                } catch (Exception e) {
                  logger.atSevere().withCause(e).log(
                      "Malformed event '%s'", new String(consumerRecord.value(), UTF_8));
                  subscriberMetrics.incrementSubscriberFailedToConsumeMessage();
                }
              });
        }
      } catch (Exception e) {
        subscriberMetrics.incrementSubscriberFailedToPollMessages();
        logger.atSevere().withCause(e).log(
            "Existing consumer loop of topic %s because of a non-recoverable exception", topic);
        reconnectAfterFailure();
      } finally {
        restClient
            .mapAsync(kafkaRestConsumerUri, this::deleteSubscription)
            .get(restClientTimeoutMs, TimeUnit.MILLISECONDS);
      }
    }

    private ListenableFuture<HttpResponse> seekToBeginning(Set<Integer> partitions) {
      ListenableFuture<HttpPost> post =
          restClient.map(
              kafkaRestConsumerUri,
              uri -> restClient.createPostSeekTopicFromBeginning(uri, topic, partitions));
      return restClient.mapAsync(post, p -> restClient.execute(p, HttpStatus.SC_NO_CONTENT));
    }

    private ListenableFuture<Set<Integer>> getTopicPartitions() {
      HttpGet getTopic = restClient.createGetTopic(topic);
      return restClient.mapAsync(
          restClient.execute(getTopic, HttpStatus.SC_OK), this::getPartitions);
    }

    private ListenableFuture<ConsumerRecords<byte[], byte[]>> getRecords(URI consumerUri) {
      HttpGet getRecords = restClient.createGetRecords(consumerUri);
      return restClient.mapAsync(
          restClient.execute(getRecords, HttpStatus.SC_OK), this::convertRecords);
    }

    private ListenableFuture<HttpResponse> subscribeToTopic(URI consumerUri) {
      HttpPost post = restClient.createPostToSubscribe(consumerUri, topic);
      return restClient.execute(post);
    }

    private ListenableFuture<?> deleteConsumer(URI consumerUri) {
      HttpDelete delete = restClient.createDeleteToConsumer(consumerUri);
      return restClient.execute(delete);
    }

    private ListenableFuture<?> deleteSubscription(URI consumerUri) {
      HttpDelete delete = restClient.createDeleteToConsumerSubscriptions(consumerUri);
      return restClient.execute(delete);
    }

    private ListenableFuture<URI> createConsumer(String consumerGroup) {
      HttpPost post = restClient.createPostToConsumer(consumerGroup + "-" + topic);
      return restClient.mapAsync(restClient.execute(post, HttpStatus.SC_OK), this::getConsumerUri);
    }

    private ListenableFuture<Set<Integer>> getPartitions(HttpResponse response) {
      try (Reader bodyReader =
          new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)) {
        JsonObject responseJson = gson.fromJson(bodyReader, JsonObject.class);
        Set<Integer> partitions = extractPartitions(responseJson);
        return Futures.immediateFuture(partitions);
      } catch (IOException e) {
        return Futures.immediateFailedFuture(e);
      }
    }

    private ListenableFuture<URI> getConsumerUri(HttpResponse response) {
      try (Reader bodyReader =
          new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)) {
        JsonObject responseJson = gson.fromJson(bodyReader, JsonObject.class);
        URI consumerUri = new URI(responseJson.get("base_uri").getAsString());
        String instanceId = responseJson.get("instance_id").getAsString();

        String restProxyId = getRestProxyId(instanceId);
        return Futures.immediateFuture(
            restClient.resolveKafkaRestApiUri(restProxyId, consumerUri.getPath()));
      } catch (UnsupportedOperationException | IOException | URISyntaxException e) {
        return Futures.immediateFailedFuture(e);
      }
    }

    private String getRestProxyId(String instanceId) {
      int instanceIdLen = instanceId.length();
      if (instanceIdLen <= INSTANCE_ID_SUFFIX_LEN + INSTANCE_ID_PREFIX_LEN) {
        // Kafka Rest Proxy instance id is not mandatory
        return "";
      }

      return instanceId.substring(
          INSTANCE_ID_PREFIX_LEN, instanceId.length() - INSTANCE_ID_SUFFIX_LEN);
    }

    private ListenableFuture<ConsumerRecords<byte[], byte[]>> convertRecords(
        HttpResponse response) {
      try (Reader bodyReader = new InputStreamReader(response.getEntity().getContent())) {
        JsonArray jsonRecords = gson.fromJson(bodyReader, JsonArray.class);
        if (jsonRecords.size() == 0) {
          return Futures.immediateFuture(new ConsumerRecords<>(Collections.emptyMap()));
        }

        Stream<ConsumerRecord<byte[], byte[]>> jsonObjects =
            StreamSupport.stream(jsonRecords.spliterator(), false)
                .map(JsonElement::getAsJsonObject)
                .map(this::jsonToConsumerRecords);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records =
            jsonObjects.collect(Collectors.groupingBy(this::jsonRecordPartition));
        return Futures.immediateFuture(new ConsumerRecords<>(records));
      } catch (IOException e) {
        subscriberMetrics.incrementSubscriberFailedToConsumeMessage();
        return Futures.immediateFailedFuture(e);
      }
    }

    private ConsumerRecord<byte[], byte[]> jsonToConsumerRecords(JsonObject jsonRecord) {
      return new ConsumerRecord<>(
          jsonRecord.get("topic").getAsString(),
          jsonRecord.get("partition").getAsInt(),
          jsonRecord.get("offset").getAsLong(),
          jsonRecord.get("key").toString().getBytes(),
          jsonRecord.get("value").toString().getBytes());
    }

    private Set<Integer> extractPartitions(JsonObject jsonRecord) {
      return StreamSupport.stream(
              jsonRecord.get("partitions").getAsJsonArray().spliterator(), false)
          .map(jsonElem -> jsonElem.getAsJsonObject().get("partition"))
          .map(JsonElement::getAsInt)
          .collect(Collectors.toSet());
    }

    private TopicPartition jsonRecordPartition(ConsumerRecord<byte[], byte[]> consumerRecord) {
      return new TopicPartition(topic, consumerRecord.partition());
    }

    private void reconnectAfterFailure()
        throws InterruptedException, ExecutionException, TimeoutException {
      // Random delay with average of DELAY_RECONNECT_AFTER_FAILURE_MSEC
      // for avoiding hammering exactly at the same interval in case of failure
      long reconnectDelay =
          DELAY_RECONNECT_AFTER_FAILURE_MSEC / 2
              + new Random().nextInt(DELAY_RECONNECT_AFTER_FAILURE_MSEC);
      Thread.sleep(reconnectDelay);
      runReceiver();
    }
  }
}
