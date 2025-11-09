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

package com.gerritforge.gerrit.plugins.kafka.publish;

import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.gerritforge.gerrit.plugins.kafka.config.KafkaProperties;
import com.gerritforge.gerrit.plugins.kafka.rest.KafkaRestClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ProducerFencedException;

public class KafkaRestProducer implements Producer<String, String> {
  private static final RecordMetadata ZEROS_RECORD_METADATA =
      new RecordMetadata(null, 0, 0, 0, null, 0, 0);
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final String KAFKA_V2_JSON = "application/vnd.kafka.json.v2+json";
  private final KafkaRestClient restClient;

  @Inject
  public KafkaRestProducer(KafkaProperties kafkaConf, KafkaRestClient.Factory restClientFactory) {
    restClient = restClientFactory.create(kafkaConf);
  }

  @Override
  public void initTransactions() {
    unsupported();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    unsupported();
  }

  @Override
  public void sendOffsetsToTransaction(
      Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
      throws ProducerFencedException {
    unsupported();
  }

  @Override
  public void sendOffsetsToTransaction(
      Map<TopicPartition, OffsetAndMetadata> map, ConsumerGroupMetadata consumerGroupMetadata)
      throws ProducerFencedException {
    unsupported();
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    unsupported();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    unsupported();
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
    return send(record, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
    HttpPost post =
        restClient.createPostToTopic(
            record.topic(),
            new StringEntity(
                getRecordAsJson(record),
                ContentType.create(KAFKA_V2_JSON, StandardCharsets.UTF_8)));
    return restClient.mapAsync(
        restClient.execute(post, HttpStatus.SC_OK),
        (res) -> Futures.immediateFuture(ZEROS_RECORD_METADATA));
  }

  @Override
  public void flush() {
    unsupported();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return unsupported();
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return unsupported();
  }

  @Override
  public void close() {
    try {
      restClient.close();
    } catch (IOException e) {
      logger.atWarning().withCause(e).log("Unable to close httpclient");
    }
  }

  @Override
  public void close(Duration timeout) {
    close();
  }

  @Override
  public Uuid clientInstanceId(Duration duration) {
    return null;
  }

  private String getRecordAsJson(ProducerRecord<String, String> record) {
    return String.format(
        "{\"records\":[{\"key\":\"%s\",\"value\":%s}]}", record.key(), record.value());
  }

  private <T> T unsupported() {
    throw new IllegalArgumentException("Unsupported method");
  }
}
